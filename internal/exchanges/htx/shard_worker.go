package htx

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
)

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
	log.Printf("[HTX-SHARD] Starte Worker fuer %s", sw.marketType)
	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			log.Printf("[HTX-SHARD] Worker beendet.")
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
			log.Printf("[HTX-SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			sw.emitStatusForSymbols("stream_reconnecting", nil, "connect_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}
		if reconnectAttempts > 0 {
			sw.emitStatusForSymbols("stream_restored", nil, "", reconnectAttempts, "")
		}
		if err := sw.eventLoop(conn); err != nil {
			sw.emitStatusForSymbols("stream_reconnecting", nil, "read_loop_exit", reconnectAttempts+1, err.Error())
			log.Printf("[HTX-SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredSymbols() {
			log.Printf("[HTX-SHARD] Worker beendet.")
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
	return fmt.Sprintf("%d", sw.requestSeq)
}

func (sw *ShardWorker) eventLoop(conn *websocket.Conn) error {
	msgCh := make(chan socketMessage, 256)
	errCh := make(chan error, 1)
	batchTicker := time.NewTicker(flushEveryMS * time.Millisecond)
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
	for _, symbol := range sw.desiredSymbolsSnapshot() {
		pendingSubs = queueUniqueSymbol(pendingSubs, symbol)
	}

	flushCommands := func() error {
		for _, symbol := range pendingSubs {
			if err := sw.sendSubscribe(conn, symbol); err != nil {
				return err
			}
			sw.mu.Lock()
			sw.activeSymbols[symbol] = true
			sw.mu.Unlock()
		}
		pendingSubs = pendingSubs[:0]
		for _, symbol := range pendingUnsubs {
			if err := sw.sendUnsubscribe(conn, symbol); err != nil {
				return err
			}
			sw.mu.Lock()
			delete(sw.activeSymbols, symbol)
			sw.mu.Unlock()
		}
		pendingUnsubs = pendingUnsubs[:0]
		return nil
	}

	for {
		select {
		case msg := <-msgCh:
			switch msg.messageType {
			case websocket.TextMessage:
				if err := sw.handleMessage(conn, msg.payload); err != nil {
					return err
				}
			case websocket.BinaryMessage:
				decoded, err := gunzipPayload(msg.payload)
				if err != nil {
					return fmt.Errorf("gunzip ws payload: %w", err)
				}
				if err := sw.handleMessage(conn, decoded); err != nil {
					return err
				}
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
		case err := <-errCh:
			return err
		case <-sw.stopCh:
			return nil
		}
	}
}

func (sw *ShardWorker) handleMessage(conn *websocket.Conn, payload []byte) error {
	var envelope wsEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("decode ws payload: %w", err)
	}
	if envelope.Ping != 0 {
		return conn.WriteJSON(pongMessage{Pong: envelope.Ping})
	}
	if envelope.Status == "error" {
		return fmt.Errorf("htx ws error: %s", string(payload))
	}
	if envelope.Ch == "" || envelope.Tick == nil {
		return nil
	}

	ingestNow := time.Now()
	goTimestamp := ingestNow.UnixMilli()
	for _, trade := range envelope.Tick.Data {
		update, err := NormalizeTrade(envelope.Ch, trade, sw.marketType, goTimestamp, ingestNow.UnixNano())
		if err != nil {
			metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
			return err
		}
		sw.dataCh <- update
	}
	return nil
}

func (sw *ShardWorker) sendSubscribe(conn *websocket.Conn, symbol string) error {
	return conn.WriteJSON(spotCommand{Sub: tradeChannel(symbol, sw.marketType), ID: sw.nextRequestID()})
}

func (sw *ShardWorker) sendUnsubscribe(conn *websocket.Conn, symbol string) error {
	return conn.WriteJSON(spotCommand{Unsub: tradeChannel(symbol, sw.marketType), ID: sw.nextRequestID()})
}

func (sw *ShardWorker) emitStatusForSymbols(eventType string, symbols []string, reason string, attempt int, message string) {
	if sw.statusCh == nil {
		return
	}
	now := time.Now().UnixMilli()
	for _, symbol := range symbols {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "htx",
			MarketType: sw.marketType,
			DataType:   "trades",
			Symbol:     TranslateSymbolFromExchange(symbol, sw.marketType),
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  now,
		}
	}
	if len(symbols) == 0 {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "htx",
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

func gunzipPayload(payload []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
