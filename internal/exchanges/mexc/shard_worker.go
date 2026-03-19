package mexc

import (
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"bybit-watcher/internal/exchanges/mexc/mexcproto"
	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
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
	desiredSymbols map[string]string
	activeSymbols  map[string]string
}

func NewShardWorker(wsURL, marketType string, initialSymbols []symbolSubscription, stopCh <-chan struct{}, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *ShardWorker {
	sw := &ShardWorker{
		wsURL:          wsURL,
		marketType:     marketType,
		commandCh:      make(chan ShardCommand, 32),
		stopCh:         stopCh,
		dataCh:         dataCh,
		statusCh:       statusCh,
		wg:             wg,
		desiredSymbols: make(map[string]string),
		activeSymbols:  make(map[string]string),
	}
	for _, symbol := range initialSymbols {
		sw.desiredSymbols[symbol.Symbol] = normalizeStreamFrequency(symbol.Freq)
	}
	return sw
}

func (sw *ShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[MEXC-SHARD] Starte Worker fuer %s", sw.marketType)

	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			log.Printf("[MEXC-SHARD] Worker beendet.")
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
			log.Printf("[MEXC-SHARD-BACKOFF] Reconnect-Versuch #%d. Warte fuer %v...", reconnectAttempts, waitTime)
			time.Sleep(waitTime)
		}

		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[MEXC-SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			sw.emitStatusForSymbols("stream_reconnecting", nil, "connect_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}

		if reconnectAttempts > 0 {
			sw.emitStatusForSymbols("stream_restored", nil, "", reconnectAttempts, "")
		}

		if err := sw.eventLoop(conn); err != nil {
			sw.emitStatusForSymbols("stream_reconnecting", nil, "read_loop_exit", reconnectAttempts+1, err.Error())
			log.Printf("[MEXC-SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredSymbols() {
			log.Printf("[MEXC-SHARD] Worker beendet.")
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

func (sw *ShardWorker) desiredSymbolsSnapshot() []symbolSubscription {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	symbols := make([]symbolSubscription, 0, len(sw.desiredSymbols))
	for symbol, freq := range sw.desiredSymbols {
		symbols = append(symbols, symbolSubscription{Symbol: symbol, Freq: freq})
	}
	return symbols
}

func (sw *ShardWorker) eventLoop(conn *websocket.Conn) error {
	msgCh := make(chan socketMessage, 256)
	errCh := make(chan error, 1)
	pingTicker := time.NewTicker(pingEverySec * time.Second)
	batchTicker := time.NewTicker(flushEvery * time.Millisecond)
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

	pendingSubs := make([]symbolSubscription, 0, commandBatchSize)
	pendingUnsubs := make([]symbolSubscription, 0, commandBatchSize)
	for _, symbol := range sw.desiredSymbolsSnapshot() {
		pendingSubs = queueUniqueSymbolSubscription(pendingSubs, symbol)
	}

	flushCommands := func() error {
		if len(pendingSubs) > 0 {
			if err := sw.sendCommand(conn, "SUBSCRIPTION", pendingSubs); err != nil {
				return err
			}
			sw.mu.Lock()
			for _, symbol := range pendingSubs {
				sw.activeSymbols[symbol.Symbol] = normalizeStreamFrequency(symbol.Freq)
			}
			sw.mu.Unlock()
			pendingSubs = pendingSubs[:0]
		}
		if len(pendingUnsubs) > 0 {
			if err := sw.sendCommand(conn, "UNSUBSCRIPTION", pendingUnsubs); err != nil {
				return err
			}
			sw.mu.Lock()
			for _, symbol := range pendingUnsubs {
				delete(sw.activeSymbols, symbol.Symbol)
			}
			sw.mu.Unlock()
			pendingUnsubs = pendingUnsubs[:0]
		}
		return nil
	}

	for {
		select {
		case msg := <-msgCh:
			switch msg.messageType {
			case websocket.BinaryMessage:
				if err := sw.handleBinaryTradeMessage(msg.payload); err != nil {
					metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
					log.Printf("[MEXC-SHARD-WARN] Proto-Decode fehlgeschlagen: %v", err)
				}
			case websocket.TextMessage:
				if sw.handleTextMessage(msg.payload) {
					continue
				}
			}
		case cmd := <-sw.commandCh:
			sw.mu.Lock()
			for _, symbol := range cmd.Symbols {
				if cmd.Action == "subscribe" {
					freq := normalizeStreamFrequency(cmd.Freq)
					if existingFreq, ok := sw.desiredSymbols[symbol]; ok && existingFreq == freq {
						continue
					}
					sw.desiredSymbols[symbol] = freq
					if activeFreq, ok := sw.activeSymbols[symbol]; !ok || activeFreq != freq {
						pendingSubs = queueUniqueSymbolSubscription(pendingSubs, symbolSubscription{Symbol: symbol, Freq: freq})
					}
				} else {
					delete(sw.desiredSymbols, symbol)
					if freq, ok := sw.activeSymbols[symbol]; ok {
						pendingUnsubs = queueUniqueSymbolSubscription(pendingUnsubs, symbolSubscription{Symbol: symbol, Freq: freq})
					}
				}
			}
			sw.mu.Unlock()
		case <-batchTicker.C:
			if err := flushCommands(); err != nil {
				return err
			}
		case <-pingTicker.C:
			if err := conn.WriteJSON(map[string]string{"method": "ping"}); err != nil {
				return err
			}
		case err := <-errCh:
			return err
		case <-sw.stopCh:
			return nil
		}
	}
}

func (sw *ShardWorker) handleBinaryTradeMessage(payload []byte) error {
	var wrapper mexcproto.PushDataV3ApiWrapper
	if err := proto.Unmarshal(payload, &wrapper); err != nil {
		return err
	}

	deals := wrapper.GetPublicAggreDeals()
	if deals == nil {
		return nil
	}

	symbol := wrapper.GetSymbol()
	if symbol == "" {
		return fmt.Errorf("missing symbol in protobuf payload")
	}

	ingestNow := time.Now()
	goTimestamp := ingestNow.UnixMilli()
	for _, trade := range deals.GetDeals() {
		normalizedTrade, err := NormalizeTrade(symbol, trade, goTimestamp, ingestNow.UnixNano())
		if err != nil {
			return err
		}
		sw.dataCh <- normalizedTrade
	}
	return nil
}

func (sw *ShardWorker) handleTextMessage(payload []byte) bool {
	if string(payload) == "PONG" {
		return true
	}

	var msg wsStatusMessage
	if err := json.Unmarshal(payload, &msg); err == nil {
		if msg.Msg == "PONG" {
			return true
		}
	}

	return false
}

func (sw *ShardWorker) sendCommand(conn *websocket.Conn, method string, symbols []symbolSubscription) error {
	channels := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		channels = append(channels, tradeChannelWithFrequency(symbol.Symbol, symbol.Freq))
	}
	for start := 0; start < len(channels); start += commandBatchSize {
		end := start + commandBatchSize
		if end > len(channels) {
			end = len(channels)
		}
		req := wsCommand{
			Method: method,
			Params: channels[start:end],
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
			Exchange:   "mexc",
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
			Exchange:   "mexc",
			MarketType: sw.marketType,
			DataType:   "trades",
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  now,
		}
	}
}
