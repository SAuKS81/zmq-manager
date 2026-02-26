package binance

import (
	"bytes"
	json "github.com/goccy/go-json"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
)

var binanceStreamNeedle = []byte(`"stream"`)

type ShardCommand struct {
	Action string
	Symbol string
}

type ShardWorker struct {
	wsURL         string
	marketType    string
	commandCh     chan ShardCommand
	stopCh        <-chan struct{}
	dataCh        chan<- *shared_types.TradeUpdate
	wg            *sync.WaitGroup
	mu            sync.Mutex
	activeStreams map[string]bool
	requestID     atomic.Uint64
}

func NewShardWorker(wsURL, marketType string, stopCh <-chan struct{}, dataCh chan<- *shared_types.TradeUpdate, wg *sync.WaitGroup) *ShardWorker {
	return &ShardWorker{
		wsURL:         wsURL,
		marketType:    marketType,
		commandCh:     make(chan ShardCommand, 2000),
		stopCh:        stopCh,
		dataCh:        dataCh,
		wg:            wg,
		activeStreams: make(map[string]bool),
	}
}

func (sw *ShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[BINANCE-TRADE-SHARD] Starte Worker (%s)", sw.marketType)

	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			return
		default:
		}

		if reconnectAttempts > 0 {
			time.Sleep(2 * time.Second)
		}

		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[BINANCE-TRADE-SHARD] Connect Fehler: %v", err)
			reconnectAttempts++
			continue
		}

		done := make(chan struct{})

		// 1. Resubscribe
		sw.mu.Lock()
		var streamsToResub []string
		for s := range sw.activeStreams {
			streamsToResub = append(streamsToResub, s)
		}
		sw.mu.Unlock()
		if len(streamsToResub) > 0 {
			go func() {
				sw.batchAndSend(conn, "SUBSCRIBE", streamsToResub)
			}()
		}

		// 2. Start Read Pump
		go sw.readPump(conn, done)

		// 3. Start Write Pump
		sw.writePump(conn, done)

		conn.Close()
		reconnectAttempts++
	}
}

func (sw *ShardWorker) writePump(conn *websocket.Conn, done chan struct{}) {
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
		case <-done:
			return
		case <-sw.stopCh:
			return
		case cmd := <-sw.commandCh:
			stream := cmd.Symbol + "@trade"
			if cmd.Action == "subscribe" {
				pendingSubs = append(pendingSubs, stream)
			} else {
				pendingUnsubs = append(pendingUnsubs, stream)
			}
			if len(pendingSubs) >= 40 || len(pendingUnsubs) >= 40 {
				if err := flushCmds(); err != nil {
					return
				}
			}
		case <-batchTicker.C:
			if err := flushCmds(); err != nil {
				return
			}
		}
	}
}

func (sw *ShardWorker) readPump(conn *websocket.Conn, done chan struct{}) {
	defer close(done)

	// Initial Deadline setzen
	readWait := 180 * time.Second
	conn.SetReadDeadline(time.Now().Add(readWait))

	conn.SetPongHandler(func(string) error {
		conn.SetReadDeadline(time.Now().Add(readWait))
		return nil
	})

	for {
		_, msg, err := readWSMessagePooled(conn)
		if err != nil {
			log.Printf("[BINANCE-TRADE-SHARD] Read Error: %v", err)
			return
		}

		// --- FIX: Deadline bei JEDER Nachricht verlängern ---
		// Wenn wir Daten bekommen, lebt die Verbindung noch.
		conn.SetReadDeadline(time.Now().Add(readWait))
		// ---------------------------------------------------

		ingestNow := time.Now()

		if bytes.Contains(msg, binanceStreamNeedle) {
			// ... Rest der Parsing Logik bleibt gleich ...
			var wrapper struct {
				Stream string  `json:"stream"`
				Data   wsTrade `json:"data"`
			}

			if err := json.Unmarshal(msg, &wrapper); err == nil && wrapper.Stream != "" {
				if wrapper.Data.EventTime == 0 {
					continue
				}

				norm, err := NormalizeTrade(wrapper.Data, sw.marketType, ingestNow.UnixMilli(), ingestNow.UnixNano())
				if err != nil || norm == nil {
					continue
				}
				sw.dataCh <- norm
			} else if err != nil {
				metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
			}
		}
	}
}

func (sw *ShardWorker) batchAndSend(conn *websocket.Conn, method string, streams []string) error {
	const batchSize = 40
	for i := 0; i < len(streams); i += batchSize {
		end := i + batchSize
		if end > len(streams) {
			end = len(streams)
		}

		req := wsRequest{
			Method: method,
			Params: streams[i:end],
			ID:     sw.requestID.Add(1),
		}

		if err := conn.WriteJSON(req); err != nil {
			return err
		}
		time.Sleep(350 * time.Millisecond)
	}
	return nil
}
