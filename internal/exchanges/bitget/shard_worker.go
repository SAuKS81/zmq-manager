package bitget

import (
	"bytes"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
	json "github.com/goccy/go-json"
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
	pending        map[string]bitgetPendingCommand
}

var (
	bitgetPongNeedle         = []byte("pong")
	bitgetActionUpdateNeedle = []byte(`"action":"update"`)
	bitgetTradeNeedle        = []byte(`"channel":"trade"`)
	bitgetEventNeedle        = []byte(`"event":"`)
	bitgetReadBufPool        = sync.Pool{
		New: func() any {
			buf := &bytes.Buffer{}
			buf.Grow(16 * 1024)
			return buf
		},
	}
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
		pending:        make(map[string]bitgetPendingCommand),
	}
	for _, s := range initialSymbols {
		sw.desiredSymbols[s] = true
	}
	return sw
}

func (sw *ShardWorker) Run() {
	defer sw.wg.Done()

	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			log.Printf("[BITGET-SHARD] Worker beendet.")
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
			log.Printf("[BITGET-SHARD-BACKOFF] Reconnect-Versuch #%d. Warte fuer %v...", reconnectAttempts, waitTime)
			time.Sleep(waitTime)
		}

		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[BITGET-SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			sw.emitStatusForSymbols("stream_reconnecting", nil, "connect_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}

		if reconnectAttempts > 0 {
			sw.emitStatusForSymbols("stream_restored", nil, "", reconnectAttempts, "")
		}

		if err := sw.eventLoop(conn); err != nil {
			sw.emitStatusForSymbols("stream_reconnecting", nil, "read_loop_exit", reconnectAttempts+1, err.Error())
			log.Printf("[BITGET-SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredSymbols() {
			log.Printf("[BITGET-SHARD] Worker beendet.")
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

func (sw *ShardWorker) eventLoop(conn *websocket.Conn) error {
	msgCh := make(chan []byte, 256)
	errCh := make(chan error, 1)
	writeCh := make(chan []byte, 32)
	pingTicker := time.NewTicker(pingEverySec * time.Second)
	retryTicker := time.NewTicker(250 * time.Millisecond)
	defer pingTicker.Stop()
	defer retryTicker.Stop()

	go sw.readPump(conn, msgCh, errCh)
	go sw.writePump(conn, writeCh, errCh)

	sw.mu.Lock()
	initial := make([]string, 0, len(sw.desiredSymbols))
	for s := range sw.desiredSymbols {
		initial = append(initial, s)
	}
	sw.mu.Unlock()
	if err := sw.issueCommand(writeCh, "subscribe", initial); err != nil {
		return err
	}

	for {
		select {
		case msg := <-msgCh:
			ingestNow := time.Now()
			goTimestamp := ingestNow.UnixMilli()
			if bytes.Equal(msg, bitgetPongNeedle) {
				continue
			}

			if bytes.Contains(msg, bitgetEventNeedle) {
				if err := sw.handleEventResponse(msg, writeCh); err != nil {
					return err
				}
				continue
			}

			if bytes.Contains(msg, bitgetActionUpdateNeedle) && bytes.Contains(msg, bitgetTradeNeedle) {
				var wsMsg wsMsg
				if err := json.Unmarshal(msg, &wsMsg); err != nil {
					metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
					log.Printf("[BITGET-SHARD-WARN] Fehler beim Unmarshalling: %v", err)
					continue
				}
				for _, trade := range wsMsg.Data {
					normalizedTrade, err := NormalizeTrade(trade, wsMsg.Arg, goTimestamp, ingestNow.UnixNano())
					if err != nil {
						log.Printf("[BITGET-SHARD-WARN] Fehler beim Normalisieren: %v", err)
						continue
					}
					sw.dataCh <- normalizedTrade
				}
			}

		case cmd := <-sw.commandCh:
			log.Printf("[BITGET-SHARD] Erhalte Befehl: %s fuer %d Symbole", cmd.Action, len(cmd.Symbols))
			if cmd.Action == "subscribe" {
				sw.mu.Lock()
				toSub := make([]string, 0, len(cmd.Symbols))
				for _, symbol := range cmd.Symbols {
					sw.desiredSymbols[symbol] = true
					if sw.activeSymbols[symbol] {
						continue
					}
					if pending, ok := sw.pending[symbol]; ok && pending.op == "subscribe" {
						continue
					}
					toSub = append(toSub, symbol)
				}
				sw.mu.Unlock()
				if err := sw.issueCommand(writeCh, "subscribe", toSub); err != nil {
					return err
				}
			} else {
				sw.mu.Lock()
				toUnsub := make([]string, 0, len(cmd.Symbols))
				for _, symbol := range cmd.Symbols {
					delete(sw.desiredSymbols, symbol)
					if pending, ok := sw.pending[symbol]; ok && pending.op == "unsubscribe" {
						continue
					}
					if sw.activeSymbols[symbol] || (sw.pending[symbol].op == "subscribe") {
						toUnsub = append(toUnsub, symbol)
					}
				}
				sw.mu.Unlock()
				if err := sw.issueCommand(writeCh, "unsubscribe", toUnsub); err != nil {
					return err
				}
			}

		case <-retryTicker.C:
			if err := sw.retryPending(writeCh); err != nil {
				return err
			}

		case <-pingTicker.C:
			writeCh <- []byte("ping")

		case err := <-errCh:
			return err

		case <-sw.stopCh:
			close(writeCh)
			return nil
		}
	}
}

func (sw *ShardWorker) issueCommand(writeCh chan<- []byte, op string, symbols []string) error {
	if len(symbols) == 0 {
		return nil
	}

	instType, err := getInstType(sw.marketType)
	if err != nil {
		log.Printf("[BITGET-SHARD-ERROR] Ungueltiger Market-Type fuer Abo: %v", err)
		return err
	}

	for i := 0; i < len(symbols); i += symbolsPerBatch {
		end := i + symbolsPerBatch
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]

		log.Printf("[BITGET-SHARD-SEND] Sende '%s' fuer Batch %d/%d (%d Symbole)", op, (i/symbolsPerBatch)+1, (len(symbols)+symbolsPerBatch-1)/symbolsPerBatch, len(batch))

		args := make([]wsSubArg, len(batch))
		for j, s := range batch {
			args[j] = wsSubArg{InstType: instType, Channel: "trade", InstID: s}
		}

		req := wsRequest{Op: op, Args: args}
		payload, err := json.Marshal(req)
		if err != nil {
			log.Printf("[BITGET-SHARD-SEND-ERROR] Fehler beim Marshalling: %v", err)
			return err
		}

		sw.mu.Lock()
		now := time.Now()
		for _, symbol := range batch {
			attempt := 1
			if existing, ok := sw.pending[symbol]; ok && existing.op == op {
				attempt = existing.attempt + 1
			}
			sw.pending[symbol] = bitgetPendingCommand{op: op, attempt: attempt, sentAt: now}
		}
		sw.mu.Unlock()

		writeCh <- payload

		if end < len(symbols) {
			time.Sleep(delayPerBatchMs * time.Millisecond)
		}
	}
	return nil
}

func (sw *ShardWorker) handleEventResponse(msg []byte, writeCh chan<- []byte) error {
	var resp bitgetEventResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return nil
	}
	if resp.Arg.InstID == "" {
		return nil
	}
	symbol := resp.Arg.InstID
	switch resp.Event {
	case "subscribe":
		sw.mu.Lock()
		delete(sw.pending, symbol)
		sw.activeSymbols[symbol] = true
		stillDesired := sw.desiredSymbols[symbol]
		sw.mu.Unlock()
		if !stillDesired {
			return sw.issueCommand(writeCh, "unsubscribe", []string{symbol})
		}
	case "unsubscribe":
		sw.mu.Lock()
		delete(sw.pending, symbol)
		delete(sw.activeSymbols, symbol)
		sw.mu.Unlock()
	case "error":
		sw.mu.Lock()
		pending, ok := sw.pending[symbol]
		sw.mu.Unlock()
		if !ok {
			return nil
		}
		if pending.op == "unsubscribe" && pending.attempt >= 4 {
			sw.emitStatusForSymbols("stream_unsubscribe_failed", []string{symbol}, "unsubscribe_nack", pending.attempt, resp.Msg)
			sw.emitStatusForSymbols("stream_force_closed", []string{symbol}, "unsubscribe_nack", pending.attempt, resp.Msg)
			return &websocket.CloseError{Code: websocket.CloseInternalServerErr, Text: "bitget unsubscribe nack"}
		}
		return sw.issueCommand(writeCh, pending.op, []string{symbol})
	}
	return nil
}

func (sw *ShardWorker) retryPending(writeCh chan<- []byte) error {
	now := time.Now()
	toRetry := make([]struct {
		symbol string
		cmd    bitgetPendingCommand
	}, 0, len(sw.pending))

	sw.mu.Lock()
	for symbol, cmd := range sw.pending {
		if now.Sub(cmd.sentAt) < nextBitgetRetryDelay(cmd.attempt) {
			continue
		}
		toRetry = append(toRetry, struct {
			symbol string
			cmd    bitgetPendingCommand
		}{symbol: symbol, cmd: cmd})
	}
	sw.mu.Unlock()

	for _, item := range toRetry {
		if item.cmd.attempt >= 4 {
			if item.cmd.op == "unsubscribe" {
				sw.emitStatusForSymbols("stream_unsubscribe_failed", []string{item.symbol}, "unsubscribe_ack_timeout", item.cmd.attempt, "")
				sw.emitStatusForSymbols("stream_force_closed", []string{item.symbol}, "unsubscribe_ack_timeout", item.cmd.attempt, "")
			}
			return &websocket.CloseError{Code: websocket.CloseNormalClosure, Text: "bitget command ack timeout"}
		}
		log.Printf("[BITGET-SHARD-ERROR] %s ack timeout fuer %s attempt=%d retrying", item.cmd.op, item.symbol, item.cmd.attempt)
		if err := sw.issueCommand(writeCh, item.cmd.op, []string{item.symbol}); err != nil {
			return err
		}
	}
	return nil
}

func (sw *ShardWorker) emitStatusForSymbols(eventType string, symbols []string, reason string, attempt int, message string) {
	if sw.statusCh == nil {
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
			Exchange:   "bitget",
			MarketType: sw.marketType,
			DataType:   "trades",
			Symbol:     TranslateSymbolFromExchange(symbol, sw.marketType),
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  time.Now().UnixMilli(),
		}
	}
}

func (sw *ShardWorker) writePump(conn *websocket.Conn, writeCh <-chan []byte, errCh chan<- error) {
	defer conn.Close()
	for {
		select {
		case message, ok := <-writeCh:
			if !ok {
				conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, message); err != nil {
				errCh <- err
				return
			}
		case <-sw.stopCh:
			return
		}
	}
}

func (sw *ShardWorker) readPump(conn *websocket.Conn, msgCh chan<- []byte, errCh chan<- error) {
	defer conn.Close()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(readIdleSec * time.Second))
		_, message, err := readBitgetMessagePooled(conn)
		if err != nil {
			errCh <- err
			return
		}
		msgCh <- message
	}
}

func readBitgetMessagePooled(conn *websocket.Conn) (int, []byte, error) {
	msgType, r, err := conn.NextReader()
	if err != nil {
		return 0, nil, err
	}

	buf := bitgetReadBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	_, err = buf.ReadFrom(r)
	if err != nil {
		bitgetReadBufPool.Put(buf)
		return 0, nil, err
	}

	msg := append([]byte(nil), buf.Bytes()...)
	bitgetReadBufPool.Put(buf)
	return msgType, msg, nil
}
