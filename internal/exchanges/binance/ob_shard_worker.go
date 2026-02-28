package binance

import (
	"bytes"
	json "github.com/goccy/go-json"
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
}

type OrderBookShardWorker struct {
	wsURL          string
	marketType     string
	commandCh      chan OBManagerCommand
	stopCh         <-chan struct{}
	dataCh         chan<- *shared_types.OrderBookUpdate
	wg             *sync.WaitGroup
	mu             sync.Mutex
	desiredStreams map[string]bool
	activeStreams  map[string]bool
	requestID      atomic.Uint64
}

func NewOrderBookShardWorker(wsURL, marketType string, stopCh <-chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, wg *sync.WaitGroup) *OrderBookShardWorker {
	return &OrderBookShardWorker{
		wsURL:          wsURL,
		marketType:     marketType,
		commandCh:      make(chan OBManagerCommand, 2000),
		stopCh:         stopCh,
		dataCh:         dataCh,
		wg:             wg,
		desiredStreams: make(map[string]bool),
		activeStreams:  make(map[string]bool),
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

		streamsToResub := sw.desiredStreamsSnapshot()

		if len(streamsToResub) > 0 {
			if _, err := sw.batchAndSend(conn, "SUBSCRIBE", streamsToResub); err != nil {
				conn.Close()
				reconnectAttempts++
				continue
			}
		}

		reconnectAttempts = 0
		if err := sw.readLoop(conn); err != nil {
			log.Printf("[BINANCE-OB-SHARD] Disconnect: %v", err)
		}
		conn.Close()

		select {
		case <-sw.stopCh:
			return
		default:
		}
		if !sw.hasDesiredStreams() {
			return
		}
		reconnectAttempts++
	}
}

func (sw *OrderBookShardWorker) hasDesiredStreams() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.desiredStreams) > 0
}

func (sw *OrderBookShardWorker) desiredStreamsSnapshot() []string {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	streams := make([]string, 0, len(sw.desiredStreams))
	for stream := range sw.desiredStreams {
		streams = append(streams, stream)
	}
	return streams
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

func (sw *OrderBookShardWorker) batchAndSend(conn *websocket.Conn, method string, streams []string) ([]binanceBatchRequest, error) {
	const batchSize = 40
	reqs := make([]binanceBatchRequest, 0, (len(streams)+batchSize-1)/batchSize)
	for i := 0; i < len(streams); i += batchSize {
		end := i + batchSize
		if end > len(streams) {
			end = len(streams)
		}
		batch := streams[i:end]

		id := sw.requestID.Add(1)
		req := wsRequest{
			Method: method,
			Params: batch,
			ID:     id,
		}

		if err := conn.WriteJSON(req); err != nil {
			return nil, err
		}
		reqs = append(reqs, binanceBatchRequest{id: id, streams: append([]string(nil), batch...)})
		time.Sleep(350 * time.Millisecond)
	}
	return reqs, nil
}

func (sw *OrderBookShardWorker) readLoop(conn *websocket.Conn) error {
	msgCh := make(chan incomingOBMessage, 250)
	errCh := make(chan error, 1)
	respCh := make(chan wsCommandResponse, 128)

	go func() {
		defer close(msgCh)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(190 * time.Second))
			_, message, err := readWSMessagePooled(conn)
			if err != nil {
				errCh <- err
				return
			}
			var resp wsCommandResponse
			if !bytes.Contains(message, binanceOBStreamNeedle) && json.Unmarshal(message, &resp) == nil && resp.ID != 0 {
				respCh <- resp
				continue
			}
			msgCh <- incomingOBMessage{payload: message, ingestUnixNano: time.Now().UnixNano()}
		}
	}()

	batchTicker := time.NewTicker(500 * time.Millisecond)
	retryTicker := time.NewTicker(250 * time.Millisecond)
	defer batchTicker.Stop()
	defer retryTicker.Stop()

	pendingSubs := make([]string, 0, 100)
	pendingUnsubs := make([]string, 0, 100)
	inflight := make(map[uint64]binanceInflightCommand)

	flushCmds := func() error {
		if len(pendingSubs) > 0 {
			ids, err := sw.batchAndSend(conn, "SUBSCRIBE", pendingSubs)
			if err != nil {
				return err
			}
			now := time.Now()
			for _, req := range ids {
				inflight[req.id] = binanceInflightCommand{method: "SUBSCRIBE", streams: req.streams, attempt: 1, sentAt: now}
			}
			pendingSubs = pendingSubs[:0]
		}
		if len(pendingUnsubs) > 0 {
			ids, err := sw.batchAndSend(conn, "UNSUBSCRIBE", pendingUnsubs)
			if err != nil {
				return err
			}
			now := time.Now()
			for _, req := range ids {
				attempt := 1
				for _, stream := range req.streams {
					for _, cmd := range inflight {
						if cmd.method != "UNSUBSCRIBE" {
							continue
						}
						for _, existing := range cmd.streams {
							if existing == stream && cmd.attempt >= attempt {
								attempt = cmd.attempt + 1
							}
						}
					}
				}
				inflight[req.id] = binanceInflightCommand{method: "UNSUBSCRIBE", streams: req.streams, attempt: attempt, sentAt: now}
			}
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
		case cmd := <-sw.commandCh:
			stream := getOBStreamName(cmd.Symbol, cmd.Depth)
			if cmd.Action == "subscribe" {
				sw.mu.Lock()
				sw.desiredStreams[stream] = true
				alreadyActive := sw.activeStreams[stream]
				sw.mu.Unlock()
				if !alreadyActive {
					pendingSubs = queueUniqueStream(pendingSubs, stream)
				}
			} else {
				sw.mu.Lock()
				delete(sw.desiredStreams, stream)
				wasActive := sw.activeStreams[stream]
				sw.mu.Unlock()
				if wasActive {
					pendingUnsubs = queueUniqueStream(pendingUnsubs, stream)
				}
			}
			if len(pendingSubs) >= 40 || len(pendingUnsubs) >= 40 {
				if err := flushCmds(); err != nil {
					return err
				}
			}
		case resp := <-respCh:
			cmd, ok := inflight[resp.ID]
			if !ok {
				continue
			}
			delete(inflight, resp.ID)
			if resp.Code != 0 {
				if cmd.method == "UNSUBSCRIBE" && cmd.attempt < 4 {
					log.Printf("[BINANCE-OB-SHARD] unsubscribe nack id=%d attempt=%d code=%d msg=%q retrying", resp.ID, cmd.attempt, resp.Code, resp.Msg)
					for _, stream := range cmd.streams {
						pendingUnsubs = queueUniqueStream(pendingUnsubs, stream)
					}
					continue
				}
				return nil
			}
			sw.mu.Lock()
			switch cmd.method {
			case "SUBSCRIBE":
				for _, stream := range cmd.streams {
					sw.activeStreams[stream] = true
					if !sw.desiredStreams[stream] {
						pendingUnsubs = queueUniqueStream(pendingUnsubs, stream)
					}
				}
			case "UNSUBSCRIBE":
				for _, stream := range cmd.streams {
					delete(sw.activeStreams, stream)
				}
			}
			sw.mu.Unlock()
		case <-batchTicker.C:
			if err := flushCmds(); err != nil {
				return err
			}
		case <-retryTicker.C:
			now := time.Now()
			for id, cmd := range inflight {
				if cmd.method != "UNSUBSCRIBE" {
					continue
				}
				if now.Sub(cmd.sentAt) < nextUnsubscribeRetryDelay(cmd.attempt) {
					continue
				}
				delete(inflight, id)
				if cmd.attempt >= 4 {
					log.Printf("[BINANCE-OB-SHARD] unsubscribe ack timeout for %v after %d attempts; forcing shard recycle", cmd.streams, cmd.attempt)
					return nil
				}
				log.Printf("[BINANCE-OB-SHARD] unsubscribe ack timeout for %v attempt=%d retrying", cmd.streams, cmd.attempt)
				for _, stream := range cmd.streams {
					pendingUnsubs = queueUniqueStream(pendingUnsubs, stream)
				}
			}
		case err := <-errCh:
			return err
		case <-sw.stopCh:
			return nil
		}
	}
}

func (sw *OrderBookShardWorker) handleMessage(msg []byte, ingestUnixNano int64) {
	if bytes.Contains(msg, binanceOBStreamNeedle) {
		if !bytes.Contains(msg, binanceOBDepthNeedle) {
			return
		}

		var wrapper wsOrderBookCombined
		if err := json.Unmarshal(msg, &wrapper); err == nil && wrapper.Stream != "" {
			symbolFromStream := strings.Split(wrapper.Stream, "@")[0]
			ob := wrapper.Data

			hasData := len(ob.BidsSpot) > 0 || len(ob.AsksSpot) > 0 || len(ob.BidsFut) > 0 || len(ob.AsksFut) > 0
			if hasData {
				norm, _ := NormalizeOrderBook(ob, symbolFromStream, sw.marketType, time.Unix(0, ingestUnixNano).UnixMilli(), ingestUnixNano)
				if norm != nil {
					sw.dataCh <- norm
				}
			}
		} else if err != nil {
			metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeOBUpdate)
		}
	}
}
