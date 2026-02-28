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

type bitgetOutboundCommand struct {
	action  string
	symbols []string
}

type ShardWorker struct {
	wsURL          string
	marketType     string
	commandCh      chan ShardCommand
	stopCh         <-chan struct{}
	dataCh         chan<- *shared_types.TradeUpdate
	statusCh       chan<- *shared_types.StreamStatusEvent
	limiter        *bitgetSendLimiter
	wg             *sync.WaitGroup
	mu             sync.Mutex
	desiredSymbols map[string]bool
	activeSymbols  map[string]bool
}

var (
	bitgetPongNeedle         = []byte("pong")
	bitgetActionUpdateNeedle = []byte(`"action":"update"`)
	bitgetTradeNeedle        = []byte(`"channel":"trade"`)
	bitgetReadBufPool        = sync.Pool{
		New: func() any {
			buf := &bytes.Buffer{}
			buf.Grow(16 * 1024)
			return buf
		},
	}
)

func NewShardWorker(wsURL, marketType string, initialSymbols []string, stopCh <-chan struct{}, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent, limiter *bitgetSendLimiter, wg *sync.WaitGroup) *ShardWorker {
	sw := &ShardWorker{
		wsURL:          wsURL,
		marketType:     marketType,
		commandCh:      make(chan ShardCommand, 10),
		stopCh:         stopCh,
		dataCh:         dataCh,
		statusCh:       statusCh,
		limiter:        limiter,
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

func (sw *ShardWorker) eventLoop(conn *websocket.Conn) error {
	msgCh := make(chan []byte, 256)
	errCh := make(chan error, 1)
	writeCh := make(chan []byte, 32)
	outboundCh := make(chan bitgetOutboundCommand, 32)
	pingTicker := time.NewTicker(pingEverySec * time.Second)
	defer pingTicker.Stop()
	defer close(outboundCh)

	go sw.readPump(conn, msgCh, errCh)
	go sw.writePump(conn, writeCh, errCh)
	go sw.senderLoop(writeCh, outboundCh, errCh)

	sw.mu.Lock()
	initial := make([]string, 0, len(sw.desiredSymbols))
	for s := range sw.desiredSymbols {
		initial = append(initial, s)
	}
	sw.mu.Unlock()
	if len(initial) > 0 {
		log.Printf("[BITGET-SHARD-INFO] (Re)Connect erfolgreich. Sende initiale Abos fuer %d Symbole.", len(initial))
		outboundCh <- bitgetOutboundCommand{action: "subscribe", symbols: initial}
	}

	for {
		select {
		case msg := <-msgCh:
			ingestNow := time.Now()
			goTimestamp := ingestNow.UnixMilli()
			if bytes.Equal(msg, bitgetPongNeedle) {
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
			sw.mu.Lock()
			symbols := make([]string, 0, len(cmd.Symbols))
			for _, s := range cmd.Symbols {
				if cmd.Action == "subscribe" {
					sw.desiredSymbols[s] = true
					if sw.activeSymbols[s] {
						continue
					}
					sw.activeSymbols[s] = true
					symbols = append(symbols, s)
				} else {
					delete(sw.desiredSymbols, s)
					if !sw.activeSymbols[s] {
						continue
					}
					delete(sw.activeSymbols, s)
					symbols = append(symbols, s)
				}
			}
			sw.mu.Unlock()
			if len(symbols) > 0 {
				outboundCh <- bitgetOutboundCommand{action: cmd.Action, symbols: symbols}
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

func (sw *ShardWorker) senderLoop(writeCh chan<- []byte, outboundCh <-chan bitgetOutboundCommand, errCh chan<- error) {
	for cmd := range outboundCh {
		if err := sw.sendSubscription(writeCh, cmd.action, cmd.symbols); err != nil {
			errCh <- err
			return
		}
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

func (sw *ShardWorker) sendSubscription(writeCh chan<- []byte, op string, symbols []string) error {
	if len(symbols) == 0 {
		return nil
	}

	instType, err := getInstType(sw.marketType)
	if err != nil {
		log.Printf("[BITGET-SHARD-ERROR] Ungueltiger Market-Type fuer Abo: %v", err)
		return nil
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
			continue
		}

		if sw.limiter != nil && !sw.limiter.Wait(sw.stopCh) {
			return nil
		}

		select {
		case writeCh <- payload:
		case <-sw.stopCh:
			return nil
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
