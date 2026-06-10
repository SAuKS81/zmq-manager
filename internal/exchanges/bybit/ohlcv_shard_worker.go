package bybit

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
	gjson "github.com/goccy/go-json"
	"github.com/gorilla/websocket"
)

type OHLCVShardWorker struct {
	wsURL         string
	marketType    string
	commandCh     chan ShardCommand
	stopCh        <-chan struct{}
	dataCh        chan<- *shared_types.OHLCVUpdate
	statusCh      chan<- *shared_types.StreamStatusEvent
	wg            *sync.WaitGroup
	mu            sync.Mutex
	desiredTopics map[string]bool
	activeTopics  map[string]bool
	requestID     atomic.Uint64
}

var (
	bybitKlineTopicNeedle = []byte(`"topic":"kline.`)
	bybitKlinePongNeedle  = []byte(`"op":"pong"`)
	bybitKlineReadBufPool = sync.Pool{New: func() any { buf := &bytes.Buffer{}; buf.Grow(16 * 1024); return buf }}
	bybitKlineMsgPool     = sync.Pool{New: func() any { return &wsKlineMsg{Data: make([]wsKlineData, 0, 8)} }}
)

func NewOHLCVShardWorker(wsURL, marketType string, initialTopics []string, stopCh <-chan struct{}, dataCh chan<- *shared_types.OHLCVUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *OHLCVShardWorker {
	sw := &OHLCVShardWorker{
		wsURL:         wsURL,
		marketType:    marketType,
		commandCh:     make(chan ShardCommand, 10),
		stopCh:        stopCh,
		dataCh:        dataCh,
		statusCh:      statusCh,
		wg:            wg,
		desiredTopics: make(map[string]bool),
		activeTopics:  make(map[string]bool),
	}
	for _, topic := range initialTopics {
		sw.desiredTopics[topic] = true
	}
	return sw
}

func (sw *OHLCVShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[BYBIT-OHLCV-SHARD] Starte Worker fuer %s", sw.marketType)

	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			log.Printf("[BYBIT-OHLCV-SHARD] Worker beendet.")
			return
		default:
		}

		if reconnectAttempts > 0 {
			backoff := time.Duration(math.Pow(2, float64(reconnectAttempts))) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			jitter := time.Duration(rand.Intn(1000)-500) * time.Millisecond
			time.Sleep(backoff + jitter)
		}

		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[BYBIT-OHLCV-ERROR] Connect fehlgeschlagen: %v", err)
			sw.emitStatusForTopics("stream_reconnecting", nil, "connect_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}

		if reconnectAttempts > 0 {
			sw.emitStatusForTopics("stream_restored", nil, "", reconnectAttempts, "")
		}
		if err := sw.runSession(conn); err != nil {
			sw.emitStatusForTopics("stream_reconnecting", nil, "read_loop_exit", reconnectAttempts+1, err.Error())
			log.Printf("[BYBIT-OHLCV-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredTopics() {
			log.Printf("[BYBIT-OHLCV-SHARD] Worker beendet.")
			return
		}
		reconnectAttempts++
	}
}

func (sw *OHLCVShardWorker) hasDesiredTopics() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.desiredTopics) > 0
}

func (sw *OHLCVShardWorker) desiredTopicsSnapshot() []string {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	topics := make([]string, 0, len(sw.desiredTopics))
	for topic := range sw.desiredTopics {
		topics = append(topics, topic)
	}
	return topics
}

func (sw *OHLCVShardWorker) runSession(conn *websocket.Conn) error {
	msgCh := make(chan *bytes.Buffer, 256)
	errCh := make(chan error, 1)
	pingTicker := time.NewTicker(pingEverySec * time.Second)
	maintenanceTicker := time.NewTicker(250 * time.Millisecond)
	defer pingTicker.Stop()
	defer maintenanceTicker.Stop()

	go func() {
		for {
			_ = conn.SetReadDeadline(time.Now().Add(readIdleSec * time.Second))
			buf, err := readWSKlineMessagePooled(conn)
			if err != nil {
				errCh <- err
				return
			}
			if buf == nil {
				continue
			}
			msg := buf.Bytes()
			if bytes.Contains(msg, bybitKlinePongNeedle) {
				recyclePooledBuffer(&bybitKlineReadBufPool, buf)
				continue
			}
			msgCh <- buf
		}
	}()

	pendingSubs := make([]string, 0, 128)
	pendingUnsubs := make([]string, 0, 128)
	inflight := make(map[string]bybitInflightCommand)
	for _, topic := range sw.desiredTopicsSnapshot() {
		pendingSubs = queueUniqueTopic(pendingSubs, topic)
	}

	flushCommands := func() error {
		chunkSize := bybitCommandChunkSize(sw.marketType)
		if len(pendingSubs) > 0 {
			for _, chunk := range chunkTopics(pendingSubs, chunkSize) {
				reqID, err := sw.sendSubscription(conn, "subscribe", chunk)
				if err != nil {
					return err
				}
				inflight[reqID] = bybitInflightCommand{op: "subscribe", topics: chunk, attempt: 1, sentAt: time.Now()}
			}
			pendingSubs = pendingSubs[:0]
		}
		if len(pendingUnsubs) > 0 {
			for _, chunk := range chunkTopics(pendingUnsubs, chunkSize) {
				reqID, err := sw.sendSubscription(conn, "unsubscribe", chunk)
				if err != nil {
					return err
				}
				inflight[reqID] = bybitInflightCommand{op: "unsubscribe", topics: chunk, attempt: 1, sentAt: time.Now()}
			}
			pendingUnsubs = pendingUnsubs[:0]
		}
		return nil
	}

	for {
		select {
		case buf := <-msgCh:
			msg := buf.Bytes()
			if !bytes.Contains(msg, bybitKlineTopicNeedle) {
				var resp wsCommandResponse
				if err := gjson.Unmarshal(msg, &resp); err == nil && resp.ReqID != "" {
					recyclePooledBuffer(&bybitKlineReadBufPool, buf)
					cmd, ok := inflight[resp.ReqID]
					if !ok {
						continue
					}
					delete(inflight, resp.ReqID)
					if !resp.Success {
						return fmt.Errorf("bybit ohlcv command nack op=%s req_id=%s ret_msg=%q", cmd.op, resp.ReqID, resp.RetMsg)
					}
					sw.mu.Lock()
					switch cmd.op {
					case "subscribe":
						for _, topic := range cmd.topics {
							sw.activeTopics[topic] = true
							if !sw.desiredTopics[topic] {
								pendingUnsubs = queueUniqueTopic(pendingUnsubs, topic)
							}
						}
					case "unsubscribe":
						for _, topic := range cmd.topics {
							delete(sw.activeTopics, topic)
						}
					}
					sw.mu.Unlock()
					continue
				}
			}

			if bytes.Contains(msg, bybitKlineTopicNeedle) {
				sw.handleKlineMessage(msg, time.Now())
			}
			recyclePooledBuffer(&bybitKlineReadBufPool, buf)
		case cmd := <-sw.commandCh:
			for _, topic := range cmd.Symbols {
				if cmd.Action == "subscribe" {
					sw.mu.Lock()
					sw.desiredTopics[topic] = true
					_, active := sw.activeTopics[topic]
					sw.mu.Unlock()
					if !active {
						pendingSubs = queueUniqueTopic(pendingSubs, topic)
					}
				} else {
					sw.mu.Lock()
					_, hadDesired := sw.desiredTopics[topic]
					_, active := sw.activeTopics[topic]
					delete(sw.desiredTopics, topic)
					sw.mu.Unlock()
					if hadDesired || active {
						pendingUnsubs = queueUniqueTopic(pendingUnsubs, topic)
					}
				}
			}
		case <-maintenanceTicker.C:
			if err := flushCommands(); err != nil {
				return err
			}
		case <-pingTicker.C:
			if err := conn.WriteJSON(map[string]string{"op": "ping"}); err != nil {
				return err
			}
		case err := <-errCh:
			return err
		case <-sw.stopCh:
			return nil
		}
	}
}

func (sw *OHLCVShardWorker) handleKlineMessage(msg []byte, ingestNow time.Time) {
	klineMsg := bybitKlineMsgPool.Get().(*wsKlineMsg)
	klineMsg.Topic = ""
	klineMsg.Type = ""
	klineMsg.Timestamp = 0
	klineMsg.Data = klineMsg.Data[:0]
	if err := gjson.Unmarshal(msg, klineMsg); err != nil {
		bybitKlineMsgPool.Put(klineMsg)
		metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeOHLCV)
		return
	}
	goTimestamp := ingestNow.UnixMilli()
	for _, candle := range klineMsg.Data {
		normalized, err := NormalizeOHLCV(klineMsg.Topic, candle, sw.marketType, goTimestamp, ingestNow.UnixNano())
		if err != nil {
			continue
		}
		if sw.dataCh != nil {
			sw.dataCh <- normalized
		}
	}
	klineMsg.Data = klineMsg.Data[:0]
	bybitKlineMsgPool.Put(klineMsg)
}

func (sw *OHLCVShardWorker) sendSubscription(conn *websocket.Conn, op string, topics []string) (string, error) {
	if len(topics) == 0 {
		return "", nil
	}
	log.Printf("[BYBIT-OHLCV-SEND] Sende '%s' fuer %d Topics", op, len(topics))
	if op == "unsubscribe" {
		metrics.RecordUnsubscribeAttempt("bybit", sw.marketType, "ohlcv", len(topics))
	}
	reqID := strconv.FormatUint(sw.requestID.Add(1), 10)
	msg := bybitCommandRequest{Op: op, Args: topics, ReqID: reqID}
	if err := conn.WriteJSON(msg); err != nil {
		return "", err
	}
	return reqID, nil
}

func (sw *OHLCVShardWorker) emitStatusForTopics(eventType string, topics []string, reason string, attempt int, message string) {
	if sw.statusCh == nil {
		sw.recordStatus(eventType, topics, reason, attempt, message)
		return
	}
	targets := topics
	if len(targets) == 0 {
		sw.mu.Lock()
		targets = make([]string, 0, len(sw.desiredTopics))
		for topic := range sw.desiredTopics {
			targets = append(targets, topic)
		}
		sw.mu.Unlock()
	}
	for _, topic := range targets {
		exchangeSymbol, interval, err := ParseKlineTopic(topic, sw.marketType)
		if err != nil {
			continue
		}
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "bybit",
			MarketType: sw.marketType,
			DataType:   "ohlcv",
			Interval:   interval,
			Symbol:     TranslateSymbolFromExchange(exchangeSymbol, sw.marketType),
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  time.Now().UnixMilli(),
		}
	}
	sw.recordStatus(eventType, targets, reason, attempt, message)
}

func (sw *OHLCVShardWorker) recordStatus(eventType string, topics []string, reason string, attempt int, message string) {
	switch eventType {
	case "stream_reconnecting":
		metrics.RecordStreamReconnect("bybit", sw.marketType, "ohlcv", reason)
	case "stream_restored":
		metrics.RecordStreamRestoreSuccess("bybit", sw.marketType, "ohlcv")
	case "stream_unsubscribe_failed":
		metrics.RecordUnsubscribeFailure("bybit", sw.marketType, "ohlcv", reason)
	case "stream_force_closed":
		metrics.RecordForcedShardRecycle("bybit", sw.marketType, "ohlcv", reason)
	}
	metrics.LogStreamLifecycle(eventType, "bybit", fmt.Sprintf("%p", sw), sw.marketType, "ohlcv", topics, attempt, reason, message)
}

func readWSKlineMessagePooled(conn *websocket.Conn) (*bytes.Buffer, error) {
	msgType, r, err := conn.NextReader()
	if err != nil {
		return nil, err
	}
	if msgType != websocket.TextMessage {
		return nil, nil
	}
	buf := bybitKlineReadBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	if _, err = buf.ReadFrom(r); err != nil {
		recyclePooledBuffer(&bybitKlineReadBufPool, buf)
		return nil, err
	}
	return buf, nil
}
