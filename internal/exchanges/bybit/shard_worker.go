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

type ShardCommand struct {
	Action  string
	Symbols []string
}

type ShardWorker struct {
	wsURL          string
	marketType     string
	commandCh      chan ShardCommand
	stopCh         <-chan struct{}
	dataCh         chan<- *shared_types.TradeUpdate
	statusCh       chan<- *shared_types.StreamStatusEvent
	wg             *sync.WaitGroup
	mu             sync.Mutex
	desiredSymbols map[string]bool
	activeSymbols  map[string]bool
	requestID      atomic.Uint64
}

var (
	bybitTradeTopicNeedle = []byte(`"topic":"publicTrade.`)
	bybitTradePongNeedle  = []byte(`"op":"pong"`)
	bybitTradeReadBufPool = sync.Pool{New: func() any { buf := &bytes.Buffer{}; buf.Grow(16 * 1024); return buf }}
	bybitTradeMsgPool     = sync.Pool{New: func() any { return &wsMsg{Data: make([]wsTrade, 0, 64)} }}
)

func NewShardWorker(wsURL, marketType string, initialSymbols []string, stopCh <-chan struct{}, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *ShardWorker {
	sw := &ShardWorker{
		wsURL:          wsURL,
		marketType:     marketType,
		commandCh:      make(chan ShardCommand, 10),
		stopCh:         stopCh,
		dataCh:         dataCh,
		statusCh:       statusCh,
		wg:             wg,
		desiredSymbols: make(map[string]bool),
		activeSymbols:  make(map[string]bool),
	}
	for _, s := range initialSymbols {
		sw.desiredSymbols[s] = true
	}
	return sw
}

func (sw *ShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[SHARD] Starte Worker fuer %s", sw.marketType)

	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			log.Printf("[SHARD] Worker beendet.")
			return
		default:
		}

		if reconnectAttempts > 0 {
			backoff := time.Duration(math.Pow(2, float64(reconnectAttempts))) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			jitter := time.Duration(rand.Intn(1000)-500) * time.Millisecond
			waitTime := backoff + jitter
			log.Printf("[SHARD-BACKOFF] Reconnect-Versuch #%d. Warte fuer %v...", reconnectAttempts, waitTime)
			time.Sleep(waitTime)
		}

		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			sw.emitStatusForSymbols("stream_reconnecting", nil, "connect_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}

		if reconnectAttempts > 0 {
			sw.emitStatusForSymbols("stream_restored", nil, "", reconnectAttempts, "")
		}

		if err := sw.runSession(conn); err != nil {
			sw.emitStatusForSymbols("stream_reconnecting", nil, "read_loop_exit", reconnectAttempts+1, err.Error())
			log.Printf("[SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredSymbols() {
			log.Printf("[SHARD] Worker beendet.")
			return
		}
		reconnectAttempts++
	}
}

func (sw *ShardWorker) hasDesiredSymbols() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.desiredSymbols) > 0
}

func (sw *ShardWorker) desiredSymbolsSnapshot() []string {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	symbols := make([]string, 0, len(sw.desiredSymbols))
	for symbol := range sw.desiredSymbols {
		symbols = append(symbols, symbol)
	}
	return symbols
}

func (sw *ShardWorker) runSession(conn *websocket.Conn) error {
	msgCh := make(chan *bytes.Buffer, 256)
	errCh := make(chan error, 1)
	respCh := make(chan wsCommandResponse, 128)
	pingTicker := time.NewTicker(pingEverySec * time.Second)
	batchTicker := time.NewTicker(200 * time.Millisecond)
	retryTicker := time.NewTicker(250 * time.Millisecond)
	defer pingTicker.Stop()
	defer batchTicker.Stop()
	defer retryTicker.Stop()

	go func() {
		for {
			_ = conn.SetReadDeadline(time.Now().Add(readIdleSec * time.Second))
			buf, err := readWSTradeMessagePooled(conn)
			if err != nil {
				errCh <- err
				return
			}
			if buf == nil {
				continue
			}
			msg := buf.Bytes()
			if bytes.Contains(msg, bybitTradePongNeedle) {
				recyclePooledBuffer(&bybitTradeReadBufPool, buf)
				continue
			}
			if !bytes.Contains(msg, bybitTradeTopicNeedle) {
				var resp wsCommandResponse
				if err := gjson.Unmarshal(msg, &resp); err == nil && resp.ReqID != "" {
					recyclePooledBuffer(&bybitTradeReadBufPool, buf)
					respCh <- resp
					continue
				}
			}
			msgCh <- buf
		}
	}()

	pendingSubs := make([]string, 0, 128)
	pendingUnsubs := make([]string, 0, 128)
	inflight := make(map[string]bybitInflightCommand)

	for _, symbol := range sw.desiredSymbolsSnapshot() {
		pendingSubs = queueUniqueTopic(pendingSubs, symbol)
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
				attempt := 1
				for _, topic := range chunk {
					for _, cmd := range inflight {
						if cmd.op != "unsubscribe" {
							continue
						}
						for _, existing := range cmd.topics {
							if existing == topic && cmd.attempt >= attempt {
								attempt = cmd.attempt + 1
							}
						}
					}
				}
				inflight[reqID] = bybitInflightCommand{op: "unsubscribe", topics: chunk, attempt: attempt, sentAt: time.Now()}
			}
			pendingUnsubs = pendingUnsubs[:0]
		}
		return nil
	}

	for {
		select {
		case buf := <-msgCh:
			msg := buf.Bytes()
			ingestNow := time.Now()
			goTimestamp := ingestNow.UnixMilli()
			if bytes.Contains(msg, bybitTradeTopicNeedle) {
				tradeMsg := bybitTradeMsgPool.Get().(*wsMsg)
				tradeMsg.Topic = ""
				tradeMsg.Type = ""
				tradeMsg.Data = tradeMsg.Data[:0]
				if err := gjson.Unmarshal(msg, tradeMsg); err != nil {
					bybitTradeMsgPool.Put(tradeMsg)
					recyclePooledBuffer(&bybitTradeReadBufPool, buf)
					metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
					continue
				}
				for _, trade := range tradeMsg.Data {
					normalizedTrade, err := NormalizeTrade(trade, sw.marketType, goTimestamp, ingestNow.UnixNano())
					if err != nil {
						continue
					}
					sw.dataCh <- normalizedTrade
				}
				tradeMsg.Data = tradeMsg.Data[:0]
				bybitTradeMsgPool.Put(tradeMsg)
			}
			recyclePooledBuffer(&bybitTradeReadBufPool, buf)
		case cmd := <-sw.commandCh:
			for _, symbol := range cmd.Symbols {
				if cmd.Action == "subscribe" {
					sw.mu.Lock()
					sw.desiredSymbols[symbol] = true
					_, active := sw.activeSymbols[symbol]
					sw.mu.Unlock()
					if !active {
						pendingSubs = queueUniqueTopic(pendingSubs, symbol)
					}
				} else {
					sw.mu.Lock()
					_, hadDesired := sw.desiredSymbols[symbol]
					_, active := sw.activeSymbols[symbol]
					delete(sw.desiredSymbols, symbol)
					sw.mu.Unlock()
					if hadDesired || active {
						pendingUnsubs = queueUniqueTopic(pendingUnsubs, symbol)
					}
				}
			}
		case resp := <-respCh:
			cmd, ok := inflight[resp.ReqID]
			if !ok {
				continue
			}
			delete(inflight, resp.ReqID)
			if !resp.Success {
				if cmd.op == "unsubscribe" && cmd.attempt < 4 {
					log.Printf("[SHARD-ERROR] unsubscribe nack req_id=%s attempt=%d ret_msg=%q retrying", resp.ReqID, cmd.attempt, resp.RetMsg)
					for _, topic := range cmd.topics {
						pendingUnsubs = queueUniqueTopic(pendingUnsubs, topic)
					}
					continue
				}
				sw.emitStatusForSymbols("stream_unsubscribe_failed", cmd.topics, "unsubscribe_nack", cmd.attempt, resp.RetMsg)
				sw.emitStatusForSymbols("stream_force_closed", cmd.topics, "unsubscribe_nack", cmd.attempt, resp.RetMsg)
				return fmt.Errorf("bybit command nack op=%s req_id=%s ret_msg=%q", cmd.op, resp.ReqID, resp.RetMsg)
			}
			sw.mu.Lock()
			switch cmd.op {
			case "subscribe":
				for _, symbol := range cmd.topics {
					sw.activeSymbols[symbol] = true
					if !sw.desiredSymbols[symbol] {
						pendingUnsubs = queueUniqueTopic(pendingUnsubs, symbol)
					}
				}
			case "unsubscribe":
				for _, symbol := range cmd.topics {
					delete(sw.activeSymbols, symbol)
				}
			}
			sw.mu.Unlock()
		case <-batchTicker.C:
			if err := flushCommands(); err != nil {
				return err
			}
		case <-retryTicker.C:
			now := time.Now()
			for reqID, cmd := range inflight {
				if cmd.op != "unsubscribe" {
					continue
				}
				if now.Sub(cmd.sentAt) < nextBybitRetryDelay(cmd.attempt) {
					continue
				}
				delete(inflight, reqID)
				if cmd.attempt >= 4 {
					sw.emitStatusForSymbols("stream_unsubscribe_failed", cmd.topics, "unsubscribe_ack_timeout", cmd.attempt, "")
					sw.emitStatusForSymbols("stream_force_closed", cmd.topics, "unsubscribe_ack_timeout", cmd.attempt, "")
					return fmt.Errorf("bybit unsubscribe ack timeout for %v after %d attempts", cmd.topics, cmd.attempt)
				}
				log.Printf("[SHARD-ERROR] unsubscribe ack timeout for %v attempt=%d retrying", cmd.topics, cmd.attempt)
				for _, topic := range cmd.topics {
					pendingUnsubs = queueUniqueTopic(pendingUnsubs, topic)
				}
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

func (sw *ShardWorker) emitStatusForSymbols(eventType string, symbols []string, reason string, attempt int, message string) {
	if sw.statusCh == nil {
		sw.recordStatus(eventType, symbols, reason, attempt, message)
		return
	}

	targets := symbols
	if len(targets) == 0 {
		sw.mu.Lock()
		targets = make([]string, 0, len(sw.desiredSymbols))
		for symbol := range sw.desiredSymbols {
			targets = append(targets, symbol)
		}
		sw.mu.Unlock()
	}

	for _, symbol := range targets {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "bybit",
			MarketType: sw.marketType,
			DataType:   "trades",
			Symbol:     TranslateSymbolFromExchange(symbol, sw.marketType),
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  time.Now().UnixMilli(),
		}
	}
	sw.recordStatus(eventType, targets, reason, attempt, message)
}

func (sw *ShardWorker) recordStatus(eventType string, symbols []string, reason string, attempt int, message string) {
	switch eventType {
	case "stream_reconnecting":
		metrics.RecordStreamReconnect("bybit", sw.marketType, "trades", reason)
	case "stream_restored":
		metrics.RecordStreamRestoreSuccess("bybit", sw.marketType, "trades")
	case "stream_unsubscribe_failed":
		metrics.RecordUnsubscribeFailure("bybit", sw.marketType, "trades", reason)
	case "stream_force_closed":
		metrics.RecordForcedShardRecycle("bybit", sw.marketType, "trades", reason)
	}
	metrics.LogStreamLifecycle(eventType, "bybit", fmt.Sprintf("%p", sw), sw.marketType, "trades", symbols, attempt, reason, message)
}

func (sw *ShardWorker) sendSubscription(conn *websocket.Conn, op string, symbols []string) (string, error) {
	if len(symbols) == 0 {
		return "", nil
	}
	log.Printf("[SHARD-SEND] Sende '%s' fuer %d Symbole", op, len(symbols))
	if op == "unsubscribe" {
		metrics.RecordUnsubscribeAttempt("bybit", sw.marketType, "trades", len(symbols))
	}
	args := make([]string, len(symbols))
	for i, s := range symbols {
		args[i] = fmt.Sprintf("publicTrade.%s", s)
	}
	reqID := strconv.FormatUint(sw.requestID.Add(1), 10)
	msg := bybitCommandRequest{Op: op, Args: args, ReqID: reqID}
	if err := conn.WriteJSON(msg); err != nil {
		log.Printf("[SHARD-SEND-ERROR] Fehler beim Senden von '%s': %v", op, err)
		return "", err
	}
	return reqID, nil
}

func readWSTradeMessagePooled(conn *websocket.Conn) (*bytes.Buffer, error) {
	msgType, r, err := conn.NextReader()
	if err != nil {
		return nil, err
	}
	if msgType != websocket.TextMessage {
		return nil, nil
	}

	buf := bybitTradeReadBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	_, err = buf.ReadFrom(r)
	if err != nil {
		recyclePooledBuffer(&bybitTradeReadBufPool, buf)
		return nil, err
	}
	return buf, nil
}
