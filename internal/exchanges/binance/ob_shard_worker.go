package binance

import (
	"bytes"
	json "github.com/goccy/go-json" // Turbo
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
)

var (
	binanceOBStreamNeedle = []byte(`"stream"`)
	binanceOBDepthNeedle  = []byte(`@depth`)
)

type incomingOBMessage struct {
	payload        []byte
	ingestUnixNano int64
	borrowed       borrowedWSMessage
}

type OrderBookShardWorker struct {
	wsURL         string
	marketType    string
	commandCh     chan OBManagerCommand
	stopCh        <-chan struct{}
	dataCh        chan<- *shared_types.OrderBookUpdate
	wg            *sync.WaitGroup
	mu            sync.Mutex
	activeStreams map[string]bool
	requestID     atomic.Uint64
}

func NewOrderBookShardWorker(wsURL, marketType string, stopCh <-chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, wg *sync.WaitGroup) *OrderBookShardWorker {
	return &OrderBookShardWorker{
		wsURL:         wsURL,
		marketType:    marketType,
		commandCh:     make(chan OBManagerCommand, 2000),
		stopCh:        stopCh,
		dataCh:        dataCh,
		wg:            wg,
		activeStreams: make(map[string]bool),
	}
}

func (sw *OrderBookShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[BINANCE-OB-SHARD] Starte Worker (%s)", sw.marketType)

	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			return
		default:
		}

		if reconnectAttempts > 0 {
			sleepDur := time.Second * time.Duration(reconnectAttempts*2)
			if sleepDur > 30*time.Second {
				sleepDur = 30 * time.Second
			}
			time.Sleep(sleepDur)
		}

		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[BINANCE-OB-SHARD] Connect Fehler: %v", err)
			reconnectAttempts++
			continue
		}

		sw.mu.Lock()
		var streamsToResub []string
		for s := range sw.activeStreams {
			streamsToResub = append(streamsToResub, s)
		}
		sw.mu.Unlock()

		if len(streamsToResub) > 0 {
			if err := sw.batchAndSend(conn, "SUBSCRIBE", streamsToResub); err != nil {
				conn.Close()
				reconnectAttempts++
				continue
			}
		}

		reconnectAttempts = 0
		if err := sw.readLoop(conn); err != nil {
			log.Printf("[BINANCE-OB-SHARD] Disconnect: %v", err)
		} else {
			conn.Close()
			return
		}
		conn.Close()
		reconnectAttempts++
	}
}

func getOBStreamName(symbol string, depth int) string {
	levels := "20"
	if depth <= 5 {
		levels = "5"
	} else if depth <= 10 {
		levels = "10"
	}
	return symbol + "@depth" + levels + "@100ms"
}

func (sw *OrderBookShardWorker) batchAndSend(conn *websocket.Conn, method string, streams []string) error {
	const batchSize = 40
	for i := 0; i < len(streams); i += batchSize {
		end := i + batchSize
		if end > len(streams) {
			end = len(streams)
		}
		batch := streams[i:end]

		req := wsRequest{
			Method: method,
			Params: batch,
			ID:     sw.requestID.Add(1),
		}

		if err := conn.WriteJSON(req); err != nil {
			return err
		}

		// Rate Limit: 350ms (Binance Limit ist 5 msgs/s)
		time.Sleep(350 * time.Millisecond)
	}
	return nil
}

func (sw *OrderBookShardWorker) readLoop(conn *websocket.Conn) error {
	msgCh := make(chan incomingOBMessage, 250)
	errCh := make(chan error, 1)

	go func() {
		defer close(msgCh)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(190 * time.Second))
			borrowed, err := readWSMessageBorrowed(conn)
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- incomingOBMessage{
				payload:        borrowed.payload,
				ingestUnixNano: time.Now().UnixNano(),
				borrowed:       borrowed,
			}
		}
	}()

	batchTicker := time.NewTicker(500 * time.Millisecond)
	defer batchTicker.Stop()

	pendingSubs := make([]string, 0, 100)
	pendingUnsubs := make([]string, 0, 100)

	flushCmds := func() error {
		if len(pendingSubs) > 0 {
			if err := sw.batchAndSend(conn, "SUBSCRIBE", pendingSubs); err != nil {
				return err
			}
			sw.mu.Lock()
			for _, s := range pendingSubs {
				sw.activeStreams[s] = true
			}
			sw.mu.Unlock()
			pendingSubs = pendingSubs[:0]
		}
		if len(pendingUnsubs) > 0 {
			if err := sw.batchAndSend(conn, "UNSUBSCRIBE", pendingUnsubs); err != nil {
				return err
			}
			sw.mu.Lock()
			for _, s := range pendingUnsubs {
				delete(sw.activeStreams, s)
			}
			sw.mu.Unlock()
			pendingUnsubs = pendingUnsubs[:0]
		}
		return nil
	}

	for {
		select {
		case incoming, ok := <-msgCh:
			if !ok {
				return <-errCh
			}
			sw.handleMessage(incoming.payload, incoming.ingestUnixNano)
			releaseWSMessage(incoming.borrowed)

		case cmd := <-sw.commandCh:
			stream := getOBStreamName(cmd.Symbol, cmd.Depth)
			if cmd.Action == "subscribe" {
				pendingSubs = append(pendingSubs, stream)
			} else {
				pendingUnsubs = append(pendingUnsubs, stream)
			}
			if len(pendingSubs) >= 40 || len(pendingUnsubs) >= 40 {
				if err := flushCmds(); err != nil {
					return err
				}
			}

		case <-batchTicker.C:
			if err := flushCmds(); err != nil {
				return err
			}

		case err := <-errCh:
			return err
		case <-sw.stopCh:
			return nil
		}
	}
}

func (sw *OrderBookShardWorker) handleMessage(msg []byte, ingestUnixNano int64) {
	// OPTIMIERUNG: Combined Unmarshal
	if bytes.Contains(msg, binanceOBStreamNeedle) {
		if !bytes.Contains(msg, binanceOBDepthNeedle) {
			return
		}

		var wrapper wsOrderBookCombined
		if err := json.Unmarshal(msg, &wrapper); err == nil && wrapper.Stream != "" {
			symbolFromStream, _, _ := strings.Cut(wrapper.Stream, "@")
			ob := wrapper.Data

			hasData := (len(ob.BidsSpot) > 0 || len(ob.AsksSpot) > 0 ||
				len(ob.BidsFut) > 0 || len(ob.AsksFut) > 0)

			if hasData {
				norm, _ := NormalizeOrderBook(ob, symbolFromStream, sw.marketType, ingestUnixNano/1e6, ingestUnixNano)
				if norm != nil {
					sw.dataCh <- norm
				}
			}
		} else if err != nil {
			metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeOBUpdate)
		}
	}
}
