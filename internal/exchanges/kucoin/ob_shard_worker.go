package kucoin

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
)

type OrderBookShardWorker struct {
	marketType     string
	commandCh      chan ShardCommand
	stopCh         <-chan struct{}
	dataCh         chan<- *shared_types.OrderBookUpdate
	statusCh       chan<- *shared_types.StreamStatusEvent
	wg             *sync.WaitGroup
	mu             sync.Mutex
	desiredSymbols map[string]int
	activeSymbols  map[string]int
	requestSeq     int64
}

func NewOrderBookShardWorker(marketType string, stopCh <-chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *OrderBookShardWorker {
	return &OrderBookShardWorker{
		marketType:     marketType,
		commandCh:      make(chan ShardCommand, 32),
		stopCh:         stopCh,
		dataCh:         dataCh,
		statusCh:       statusCh,
		wg:             wg,
		desiredSymbols: make(map[string]int),
		activeSymbols:  make(map[string]int),
	}
}

func (sw *OrderBookShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[KUCOIN-OB-SHARD] Starte Worker fuer %s", sw.marketType)

	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			log.Printf("[KUCOIN-OB-SHARD] Worker beendet.")
			return
		default:
		}

		wsURL, pingInterval, err := negotiatePublicWS(sw.nextRequestID())
		if err != nil {
			log.Printf("[KUCOIN-OB-SHARD-ERROR] Token/Endpoint-Abruf fehlgeschlagen: %v", err)
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
			time.Sleep(backoff + jitter)
		}

		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("[KUCOIN-OB-SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			sw.emitStatusForSymbols("stream_reconnecting", nil, "connect_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}

		if reconnectAttempts > 0 {
			sw.emitStatusForSymbols("stream_restored", nil, "", reconnectAttempts, "")
		}

		if err := sw.eventLoop(conn, pingInterval); err != nil {
			sw.emitStatusForSymbols("stream_reconnecting", nil, "read_loop_exit", reconnectAttempts+1, err.Error())
			log.Printf("[KUCOIN-OB-SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredSymbols() {
			log.Printf("[KUCOIN-OB-SHARD] Worker beendet.")
			return
		}
		reconnectAttempts++
	}
}

func (sw *OrderBookShardWorker) hasDesiredSymbols() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.desiredSymbols) > 0
}

func (sw *OrderBookShardWorker) desiredSymbolsSnapshot() map[string]int {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	snapshot := make(map[string]int, len(sw.desiredSymbols))
	for symbol, depth := range sw.desiredSymbols {
		snapshot[symbol] = depth
	}
	return snapshot
}

func (sw *OrderBookShardWorker) nextRequestID() string {
	sw.requestSeq++
	return fmt.Sprintf("ob-%d", sw.requestSeq)
}

func (sw *OrderBookShardWorker) eventLoop(conn *websocket.Conn, initialPingInterval time.Duration) error {
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

	pendingSubs := make([]symbolDepth, 0, commandBatchSize)
	pendingUnsubs := make([]symbolDepth, 0, commandBatchSize)
	ready := false
	for symbol, depth := range sw.desiredSymbolsSnapshot() {
		pendingSubs = queueUniqueSymbolDepth(pendingSubs, symbolDepth{Symbol: symbol, Depth: depth})
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
			for _, item := range pendingSubs {
				sw.activeSymbols[item.Symbol] = item.Depth
			}
			sw.mu.Unlock()
			pendingSubs = pendingSubs[:0]
		}
		if len(pendingUnsubs) > 0 {
			if err := sw.sendCommands(conn, "unsubscribe", pendingUnsubs); err != nil {
				return err
			}
			sw.mu.Lock()
			for _, item := range pendingUnsubs {
				delete(sw.activeSymbols, item.Symbol)
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
			pingInterval, becameReady, update, err := sw.handleTextMessage(msg.payload)
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
			if update != nil {
				sw.dataCh <- update
			}
		case cmd := <-sw.commandCh:
			sw.mu.Lock()
			for _, symbol := range cmd.Symbols {
				if cmd.Action == "subscribe" {
					depth := normalizeOrderBookDepth(cmd.Depth)
					if sw.desiredSymbols[symbol] == depth && sw.activeSymbols[symbol] == depth {
						continue
					}
					sw.desiredSymbols[symbol] = depth
					if sw.activeSymbols[symbol] != depth {
						pendingSubs = queueUniqueSymbolDepth(pendingSubs, symbolDepth{Symbol: symbol, Depth: depth})
					}
				} else {
					delete(sw.desiredSymbols, symbol)
					if depth, ok := sw.activeSymbols[symbol]; ok {
						pendingUnsubs = queueUniqueSymbolDepth(pendingUnsubs, symbolDepth{Symbol: symbol, Depth: depth})
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

type symbolDepth struct {
	Symbol string
	Depth  int
}

func queueUniqueSymbolDepth(queue []symbolDepth, item symbolDepth) []symbolDepth {
	for i, existing := range queue {
		if existing.Symbol != item.Symbol {
			continue
		}
		queue[i] = item
		return queue
	}
	return append(queue, item)
}

func (sw *OrderBookShardWorker) handleTextMessage(payload []byte) (time.Duration, bool, *shared_types.OrderBookUpdate, error) {
	var envelope wsOrderBookEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return 0, false, nil, fmt.Errorf("decode orderbook payload: %w", err)
	}

	if envelope.Code.String() != "" {
		return 0, false, nil, fmt.Errorf("kucoin orderbook ws error code=%s", envelope.Code.String())
	}
	if envelope.Type == "welcome" {
		return 0, true, nil, nil
	}
	if envelope.Type == "ack" || envelope.Result != nil || envelope.Type == "pong" {
		return 0, false, nil, nil
	}
	if envelope.Type != "message" || envelope.Data == nil {
		return 0, false, nil, nil
	}
	if envelope.Subject != "level1" && envelope.Subject != "level2" {
		return 0, false, nil, nil
	}

	symbol, depth, ok := parseOrderBookTopic(envelope.Topic)
	if !ok {
		return 0, false, nil, nil
	}

	ingestNow := time.Now()
	update, err := BuildOrderBookUpdate(symbol, depth, envelope.Data, ingestNow.UnixMilli(), ingestNow.UnixNano())
	if err != nil {
		return 0, false, nil, err
	}
	return 0, false, update, nil
}

func parseOrderBookTopic(topic string) (string, int, bool) {
	switch {
	case len(topic) > len("/spotMarket/level1:") && topic[:len("/spotMarket/level1:")] == "/spotMarket/level1:":
		return topic[len("/spotMarket/level1:"):], 1, true
	case len(topic) > len("/spotMarket/level2Depth5:") && topic[:len("/spotMarket/level2Depth5:")] == "/spotMarket/level2Depth5:":
		return topic[len("/spotMarket/level2Depth5:"):], 5, true
	case len(topic) > len("/spotMarket/level2Depth50:") && topic[:len("/spotMarket/level2Depth50:")] == "/spotMarket/level2Depth50:":
		return topic[len("/spotMarket/level2Depth50:"):], 50, true
	default:
		return "", 0, false
	}
}

func (sw *OrderBookShardWorker) sendCommands(conn *websocket.Conn, action string, items []symbolDepth) error {
	for _, item := range items {
		req := wsCommand{
			ID:       sw.nextRequestID(),
			Type:     action,
			Topic:    orderBookTopic(item.Symbol, item.Depth),
			Response: true,
		}
		if err := conn.WriteJSON(req); err != nil {
			return err
		}
	}
	return nil
}

func (sw *OrderBookShardWorker) emitStatusForSymbols(eventType string, symbols []string, reason string, attempt int, message string) {
	if sw.statusCh == nil {
		return
	}
	now := time.Now().UnixMilli()
	for _, symbol := range symbols {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "kucoin",
			MarketType: sw.marketType,
			DataType:   "orderbooks",
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
			DataType:   "orderbooks",
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  now,
		}
	}
}
