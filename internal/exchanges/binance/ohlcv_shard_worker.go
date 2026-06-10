package binance

import (
	"bytes"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	json "github.com/goccy/go-json"
	"github.com/gorilla/websocket"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
)

var (
	binanceKlineStreamNeedle = []byte(`"stream"`)
	binanceKlineNeedle       = []byte(`@kline_`)
)

type incomingKlineMessage struct {
	payload        []byte
	ingestUnixNano int64
}

type OHLCVShardWorker struct {
	wsURL          string
	marketType     string
	commandCh      chan OHLCVShardCommand
	stopCh         <-chan struct{}
	dataCh         chan<- *shared_types.OHLCVUpdate
	wg             *sync.WaitGroup
	mu             sync.Mutex
	desiredStreams map[string]bool
	activeStreams  map[string]bool
	requestID      atomic.Uint64
}

func NewOHLCVShardWorker(wsURL, marketType string, stopCh <-chan struct{}, dataCh chan<- *shared_types.OHLCVUpdate, wg *sync.WaitGroup) *OHLCVShardWorker {
	return &OHLCVShardWorker{
		wsURL:          wsURL,
		marketType:     marketType,
		commandCh:      make(chan OHLCVShardCommand, 2000),
		stopCh:         stopCh,
		dataCh:         dataCh,
		wg:             wg,
		desiredStreams: make(map[string]bool),
		activeStreams:  make(map[string]bool),
	}
}

func (sw *OHLCVShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[BINANCE-OHLCV-SHARD] Starte Worker (%s)", sw.marketType)

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
			metrics.RecordStreamReconnect("binance", sw.marketType, "ohlcv", "connect_failed")
			metrics.LogStreamLifecycle("stream_reconnecting", "binance", fmt.Sprintf("%p", sw), sw.marketType, "ohlcv", nil, reconnectAttempts+1, "connect_failed", err.Error())
			log.Printf("[BINANCE-OHLCV-SHARD] Connect Fehler: %v", err)
			reconnectAttempts++
			continue
		}
		if reconnectAttempts > 0 {
			metrics.RecordStreamRestoreSuccess("binance", sw.marketType, "ohlcv")
			metrics.LogStreamLifecycle("stream_restored", "binance", fmt.Sprintf("%p", sw), sw.marketType, "ohlcv", nil, reconnectAttempts, "", "")
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
			log.Printf("[BINANCE-OHLCV-SHARD] Disconnect: %v", err)
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
		metrics.RecordStreamReconnect("binance", sw.marketType, "ohlcv", "read_loop_exit")
		metrics.LogStreamLifecycle("stream_reconnecting", "binance", fmt.Sprintf("%p", sw), sw.marketType, "ohlcv", nil, reconnectAttempts+1, "read_loop_exit", "")
		reconnectAttempts++
	}
}

func (sw *OHLCVShardWorker) hasDesiredStreams() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.desiredStreams) > 0
}

func (sw *OHLCVShardWorker) desiredStreamsSnapshot() []string {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	streams := make([]string, 0, len(sw.desiredStreams))
	for stream := range sw.desiredStreams {
		streams = append(streams, stream)
	}
	return streams
}

func (sw *OHLCVShardWorker) batchAndSend(conn *websocket.Conn, method string, streams []string) ([]binanceBatchRequest, error) {
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
		if method == "UNSUBSCRIBE" {
			metrics.RecordUnsubscribeAttempt("binance", sw.marketType, "ohlcv", len(batch))
		}
		reqs = append(reqs, binanceBatchRequest{id: id, streams: append([]string(nil), batch...)})
		time.Sleep(350 * time.Millisecond)
	}
	return reqs, nil
}

func (sw *OHLCVShardWorker) readLoop(conn *websocket.Conn) error {
	msgCh := make(chan incomingKlineMessage, 250)
	errCh := make(chan error, 1)
	respCh := make(chan wsCommandResponse, 128)

	go func() {
		defer close(msgCh)
		for {
			_ = conn.SetReadDeadline(time.Now().Add(readIdleSec * time.Second))
			_, message, err := readWSMessagePooled(conn)
			if err != nil {
				errCh <- err
				return
			}
			var resp wsCommandResponse
			if !bytes.Contains(message, binanceKlineStreamNeedle) && json.Unmarshal(message, &resp) == nil && resp.ID != 0 {
				respCh <- resp
				continue
			}
			msgCh <- incomingKlineMessage{payload: message, ingestUnixNano: time.Now().UnixNano()}
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
			stream := cmd.Stream
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
					metrics.RecordUnsubscribeFailure("binance", sw.marketType, "ohlcv", "unsubscribe_nack")
					metrics.LogStreamLifecycle("stream_unsubscribe_failed", "binance", fmt.Sprintf("%p", sw), sw.marketType, "ohlcv", cmd.streams, cmd.attempt, "unsubscribe_nack", resp.Msg)
					log.Printf("[BINANCE-OHLCV-SHARD] unsubscribe nack id=%d attempt=%d code=%d msg=%q retrying", resp.ID, cmd.attempt, resp.Code, resp.Msg)
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
					metrics.RecordUnsubscribeFailure("binance", sw.marketType, "ohlcv", "unsubscribe_ack_timeout")
					metrics.RecordForcedShardRecycle("binance", sw.marketType, "ohlcv", "unsubscribe_ack_timeout")
					metrics.LogStreamLifecycle("stream_force_closed", "binance", fmt.Sprintf("%p", sw), sw.marketType, "ohlcv", cmd.streams, cmd.attempt, "unsubscribe_ack_timeout", "")
					log.Printf("[BINANCE-OHLCV-SHARD] unsubscribe ack timeout for %v after %d attempts; forcing shard recycle", cmd.streams, cmd.attempt)
					return nil
				}
				log.Printf("[BINANCE-OHLCV-SHARD] unsubscribe ack timeout for %v attempt=%d retrying", cmd.streams, cmd.attempt)
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

func (sw *OHLCVShardWorker) handleMessage(msg []byte, ingestUnixNano int64) {
	if !bytes.Contains(msg, binanceKlineStreamNeedle) || !bytes.Contains(msg, binanceKlineNeedle) {
		return
	}

	var wrapper wsKlineCombined
	if err := json.Unmarshal(msg, &wrapper); err != nil || wrapper.Stream == "" {
		metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeOHLCV)
		return
	}
	symbolFromStream := strings.Split(wrapper.Stream, "@")[0]
	normalized, err := NormalizeOHLCV(wrapper.Data, symbolFromStream, sw.marketType, time.Unix(0, ingestUnixNano).UnixMilli(), ingestUnixNano)
	if err != nil || normalized == nil {
		if err != nil {
			metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeOHLCV)
		}
		return
	}
	sw.dataCh <- normalized
}
