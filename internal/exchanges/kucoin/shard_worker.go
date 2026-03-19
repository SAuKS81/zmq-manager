package kucoin

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
)

type socketMessage struct {
	messageType int
	payload     []byte
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
	requestSeq     int64
}

func NewShardWorker(wsURL, marketType string, initialSymbols []string, stopCh <-chan struct{}, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *ShardWorker {
	sw := &ShardWorker{
		wsURL:          wsURL,
		marketType:     marketType,
		commandCh:      make(chan ShardCommand, 32),
		stopCh:         stopCh,
		dataCh:         dataCh,
		statusCh:       statusCh,
		wg:             wg,
		desiredSymbols: make(map[string]bool),
		activeSymbols:  make(map[string]bool),
	}
	for _, symbol := range initialSymbols {
		sw.desiredSymbols[symbol] = true
	}
	return sw
}

func (sw *ShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[KUCOIN-SHARD] Starte Worker fuer %s", sw.marketType)

	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			log.Printf("[KUCOIN-SHARD] Worker beendet.")
			return
		default:
		}

		wsURL, pingInterval, err := negotiatePublicWS(sw.nextRequestID())
		if err != nil {
			log.Printf("[KUCOIN-SHARD-ERROR] Token/Endpoint-Abruf fehlgeschlagen: %v", err)
			sw.emitStatusForSymbols("stream_reconnecting", nil, "token_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}

		if reconnectAttempts > 0 {
			backoff := time.Duration(math.Pow(2, float64(reconnectAttempts))) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			jitter := time.Duration(rand.Intn(1000)-500) * time.Millisecond
			waitTime := backoff + jitter
			log.Printf("[KUCOIN-SHARD-BACKOFF] Reconnect-Versuch #%d. Warte fuer %v...", reconnectAttempts, waitTime)
			time.Sleep(waitTime)
		}

		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("[KUCOIN-SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			sw.emitStatusForSymbols("stream_reconnecting", nil, "connect_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}

		if reconnectAttempts > 0 {
			sw.emitStatusForSymbols("stream_restored", nil, "", reconnectAttempts, "")
		}

		if err := sw.eventLoop(conn, pingInterval); err != nil {
			sw.emitStatusForSymbols("stream_reconnecting", nil, "read_loop_exit", reconnectAttempts+1, err.Error())
			log.Printf("[KUCOIN-SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredSymbols() {
			log.Printf("[KUCOIN-SHARD] Worker beendet.")
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

func (sw *ShardWorker) nextRequestID() string {
	sw.requestSeq++
	return strconv.FormatInt(sw.requestSeq, 10)
}

func (sw *ShardWorker) eventLoop(conn *websocket.Conn, initialPingInterval time.Duration) error {
	msgCh := make(chan socketMessage, 256)
	errCh := make(chan error, 1)
	if initialPingInterval <= 0 {
		initialPingInterval = defaultPingMS * time.Millisecond
	}
	pingTicker := time.NewTicker(initialPingInterval)
	batchTicker := time.NewTicker(flushEveryMS * time.Millisecond)
	defer pingTicker.Stop()
	defer batchTicker.Stop()

	go func() {
		for {
			_ = conn.SetReadDeadline(time.Now().Add(readIdleSec * time.Second))
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- socketMessage{messageType: messageType, payload: payload}
		}
	}()

	pendingSubs := make([]string, 0, commandBatchSize)
	pendingUnsubs := make([]string, 0, commandBatchSize)
	ready := false
	for _, symbol := range sw.desiredSymbolsSnapshot() {
		pendingSubs = queueUniqueSymbol(pendingSubs, symbol)
	}

	flushCommands := func() error {
		if !ready {
			return nil
		}
		if len(pendingSubs) > 0 {
			if err := sw.sendCommands(conn, "subscribe", pendingSubs); err != nil {
				return err
			}
			sw.mu.Lock()
			for _, symbol := range pendingSubs {
				sw.activeSymbols[symbol] = true
			}
			sw.mu.Unlock()
			pendingSubs = pendingSubs[:0]
		}
		if len(pendingUnsubs) > 0 {
			if err := sw.sendCommands(conn, "unsubscribe", pendingUnsubs); err != nil {
				return err
			}
			sw.mu.Lock()
			for _, symbol := range pendingUnsubs {
				delete(sw.activeSymbols, symbol)
			}
			sw.mu.Unlock()
			pendingUnsubs = pendingUnsubs[:0]
		}
		return nil
	}

	for {
		select {
		case msg := <-msgCh:
			if msg.messageType != websocket.TextMessage {
				continue
			}
			pingInterval, becameReady, err := sw.handleTextMessage(msg.payload)
			if err != nil {
				return err
			}
			if becameReady {
				ready = true
				if err := flushCommands(); err != nil {
					return err
				}
			}
			if pingInterval > 0 {
				pingTicker.Stop()
				pingTicker = time.NewTicker(pingInterval)
			}
		case cmd := <-sw.commandCh:
			sw.mu.Lock()
			for _, symbol := range cmd.Symbols {
				if cmd.Action == "subscribe" {
					if sw.desiredSymbols[symbol] && sw.activeSymbols[symbol] {
						continue
					}
					sw.desiredSymbols[symbol] = true
					if !sw.activeSymbols[symbol] {
						pendingSubs = queueUniqueSymbol(pendingSubs, symbol)
					}
				} else {
					delete(sw.desiredSymbols, symbol)
					if sw.activeSymbols[symbol] {
						pendingUnsubs = queueUniqueSymbol(pendingUnsubs, symbol)
					}
				}
			}
			sw.mu.Unlock()
		case <-batchTicker.C:
			if err := flushCommands(); err != nil {
				return err
			}
		case <-pingTicker.C:
			if err := conn.WriteJSON(wsCommand{ID: sw.nextRequestID(), Type: "ping"}); err != nil {
				return err
			}
		case err := <-errCh:
			return err
		case <-sw.stopCh:
			return nil
		}
	}
}

func (sw *ShardWorker) handleTextMessage(payload []byte) (time.Duration, bool, error) {
	var envelope wsEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return 0, false, fmt.Errorf("decode ws payload: %w", err)
	}

	if envelope.Code.String() != "" {
		return 0, false, fmt.Errorf("kucoin ws error code=%s message=%s", envelope.Code.String(), envelope.Message)
	}
	if envelope.Type == "welcome" {
		return 0, true, nil
	}

	if envelope.Type == "ack" || envelope.Result != nil {
		return 0, false, nil
	}
	if envelope.Type == "pong" {
		return 0, false, nil
	}
	if envelope.Data == nil && envelope.Message != "" {
		return 0, false, nil
	}
	if envelope.Type != "message" || envelope.Subject != "trade.l3match" || envelope.Data == nil {
		return 0, false, nil
	}

	ingestNow := time.Now()
	goTimestamp := ingestNow.UnixMilli()
	normalizedTrade, err := NormalizeTrade(envelope.Data, goTimestamp, ingestNow.UnixNano())
	if err != nil {
		metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
		return 0, false, err
	}
	sw.dataCh <- normalizedTrade
	return 0, false, nil
}

func (sw *ShardWorker) sendCommands(conn *websocket.Conn, action string, symbols []string) error {
	for _, symbol := range symbols {
		req := wsCommand{
			ID:       sw.nextRequestID(),
			Type:     action,
			Topic:    tradeChannel() + ":" + symbol,
			Response: true,
		}
		if err := conn.WriteJSON(req); err != nil {
			return err
		}
	}
	return nil
}

func (sw *ShardWorker) emitStatusForSymbols(eventType string, symbols []string, reason string, attempt int, message string) {
	if sw.statusCh == nil {
		return
	}
	now := time.Now().UnixMilli()
	for _, symbol := range symbols {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "kucoin",
			MarketType: sw.marketType,
			DataType:   "trades",
			Symbol:     TranslateSymbolFromExchange(symbol),
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  now,
		}
	}
	if len(symbols) == 0 {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "kucoin",
			MarketType: sw.marketType,
			DataType:   "trades",
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  now,
		}
	}
}

func queueUniqueSymbol(symbols []string, symbol string) []string {
	for _, existing := range symbols {
		if existing == symbol {
			return symbols
		}
	}
	return append(symbols, symbol)
}
