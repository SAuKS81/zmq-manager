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
	wsURL          string
	marketType     string
	commandCh      chan ShardCommand
	stopCh         <-chan struct{}
	dataCh         chan<- *shared_types.TradeUpdate
	wg             *sync.WaitGroup
	mu             sync.Mutex
	desiredStreams map[string]bool
	activeStreams  map[string]bool
	requestID      atomic.Uint64
}

func NewShardWorker(wsURL, marketType string, stopCh <-chan struct{}, dataCh chan<- *shared_types.TradeUpdate, wg *sync.WaitGroup) *ShardWorker {
	return &ShardWorker{
		wsURL:          wsURL,
		marketType:     marketType,
		commandCh:      make(chan ShardCommand, 2000),
		stopCh:         stopCh,
		dataCh:         dataCh,
		wg:             wg,
		desiredStreams: make(map[string]bool),
		activeStreams:  make(map[string]bool),
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
		respCh := make(chan wsCommandResponse, 128)

		streamsToResub := sw.desiredStreamsSnapshot()
		if len(streamsToResub) > 0 {
			if _, err := sw.batchAndSend(conn, "SUBSCRIBE", streamsToResub); err != nil {
				conn.Close()
				reconnectAttempts++
				continue
			}
		}

		go sw.readPump(conn, done, respCh)
		sw.writePump(conn, done, respCh)

		conn.Close()
		if !sw.hasDesiredStreams() {
			return
		}
		reconnectAttempts++
	}
}

func (sw *ShardWorker) hasDesiredStreams() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.desiredStreams) > 0
}

func (sw *ShardWorker) desiredStreamsSnapshot() []string {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	streams := make([]string, 0, len(sw.desiredStreams))
	for stream := range sw.desiredStreams {
		streams = append(streams, stream)
	}
	return streams
}

func (sw *ShardWorker) writePump(conn *websocket.Conn, done chan struct{}, respCh <-chan wsCommandResponse) {
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
		case <-done:
			return
		case <-sw.stopCh:
			return
		case cmd := <-sw.commandCh:
			stream := cmd.Symbol + "@trade"
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
					log.Printf("[BINANCE-TRADE-SHARD] flush command error: %v", err)
					return
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
					log.Printf("[BINANCE-TRADE-SHARD] unsubscribe nack id=%d attempt=%d code=%d msg=%q retrying", resp.ID, cmd.attempt, resp.Code, resp.Msg)
					for _, stream := range cmd.streams {
						pendingUnsubs = queueUniqueStream(pendingUnsubs, stream)
					}
					continue
				}
				log.Printf("[BINANCE-TRADE-SHARD] command nack id=%d method=%s code=%d msg=%q forcing shard recycle", resp.ID, cmd.method, resp.Code, resp.Msg)
				_ = conn.Close()
				return
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
				log.Printf("[BINANCE-TRADE-SHARD] periodic flush error: %v", err)
				return
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
					log.Printf("[BINANCE-TRADE-SHARD] unsubscribe ack timeout for %v after %d attempts; forcing shard recycle", cmd.streams, cmd.attempt)
					_ = conn.Close()
					return
				}
				log.Printf("[BINANCE-TRADE-SHARD] unsubscribe ack timeout for %v attempt=%d retrying", cmd.streams, cmd.attempt)
				for _, stream := range cmd.streams {
					pendingUnsubs = queueUniqueStream(pendingUnsubs, stream)
				}
			}
		}
	}
}

func (sw *ShardWorker) readPump(conn *websocket.Conn, done chan struct{}, respCh chan<- wsCommandResponse) {
	defer close(done)

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

		conn.SetReadDeadline(time.Now().Add(readWait))
		ingestNow := time.Now()

		if bytes.Contains(msg, binanceStreamNeedle) {
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
			continue
		}

		var resp wsCommandResponse
		if err := json.Unmarshal(msg, &resp); err == nil && resp.ID != 0 {
			select {
			case respCh <- resp:
			case <-sw.stopCh:
				return
			case <-done:
				return
			}
		}
	}
}

func (sw *ShardWorker) batchAndSend(conn *websocket.Conn, method string, streams []string) ([]binanceBatchRequest, error) {
	const batchSize = 40
	reqs := make([]binanceBatchRequest, 0, (len(streams)+batchSize-1)/batchSize)
	for i := 0; i < len(streams); i += batchSize {
		end := i + batchSize
		if end > len(streams) {
			end = len(streams)
		}

		id := sw.requestID.Add(1)
		req := wsRequest{
			Method: method,
			Params: streams[i:end],
			ID:     id,
		}

		if err := conn.WriteJSON(req); err != nil {
			return nil, err
		}
		reqs = append(reqs, binanceBatchRequest{id: id, streams: append([]string(nil), streams[i:end]...)})
		time.Sleep(350 * time.Millisecond)
	}
	return reqs, nil
}
