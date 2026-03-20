package coinex

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

type OrderBookShardWorker struct {
	wsURL          string
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

func NewOrderBookShardWorker(wsURL, marketType string, stopCh <-chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *OrderBookShardWorker {
	return &OrderBookShardWorker{
		wsURL:          wsURL,
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
	log.Printf("[COINEX-OB-SHARD] Starte Worker fuer %s", sw.marketType)

	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			log.Printf("[COINEX-OB-SHARD] Worker beendet.")
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
			log.Printf("[COINEX-OB-SHARD-BACKOFF] Reconnect-Versuch #%d. Warte fuer %v...", reconnectAttempts, waitTime)
			time.Sleep(waitTime)
		}

		dialer := *websocket.DefaultDialer
		dialer.EnableCompression = true
		conn, _, err := dialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[COINEX-OB-SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			sw.emitStatusForSymbols("stream_reconnecting", nil, "connect_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}

		if reconnectAttempts > 0 {
			sw.emitStatusForSymbols("stream_restored", nil, "", reconnectAttempts, "")
		}

		if err := sw.eventLoop(conn); err != nil {
			sw.emitStatusForSymbols("stream_reconnecting", nil, "read_loop_exit", reconnectAttempts+1, err.Error())
			log.Printf("[COINEX-OB-SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredSymbols() {
			log.Printf("[COINEX-OB-SHARD] Worker beendet.")
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

func (sw *OrderBookShardWorker) nextRequestID() int64 {
	sw.requestSeq++
	return sw.requestSeq
}

func (sw *OrderBookShardWorker) eventLoop(conn *websocket.Conn) error {
	msgCh := make(chan socketMessage, 256)
	errCh := make(chan error, 1)
	pingTicker := time.NewTicker(defaultPingMS * time.Millisecond)
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
	for symbol, depth := range sw.desiredSymbolsSnapshot() {
		pendingSubs = queueUniqueSymbolDepth(pendingSubs, symbolDepth{Symbol: symbol, Depth: depth})
	}

	flushCommands := func() error {
		if len(pendingSubs) > 0 {
			if err := sw.sendCommand(conn, "depth.subscribe", pendingSubs); err != nil {
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
			if err := sw.sendUnsubscribe(conn, pendingUnsubs); err != nil {
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
			switch msg.messageType {
			case websocket.TextMessage:
				update, err := sw.handleTextMessage(msg.payload)
				if err != nil {
					return err
				}
				if update != nil {
					sw.dataCh <- update
				}
			case websocket.BinaryMessage:
				decoded, err := gunzipCoinexPayload(msg.payload)
				if err != nil {
					return fmt.Errorf("gunzip ws payload: %w", err)
				}
				update, err := sw.handleTextMessage(decoded)
				if err != nil {
					return err
				}
				if update != nil {
					sw.dataCh <- update
				}
			}
		case cmd := <-sw.commandCh:
			sw.mu.Lock()
			for _, symbol := range cmd.Symbols {
				depth := NormalizeOrderBookDepth(cmd.Depth)
				if cmd.Action == "subscribe" {
					if sw.desiredSymbols[symbol] == depth && sw.activeSymbols[symbol] == depth {
						continue
					}
					sw.desiredSymbols[symbol] = depth
					if sw.activeSymbols[symbol] != depth {
						pendingSubs = queueUniqueSymbolDepth(pendingSubs, symbolDepth{Symbol: symbol, Depth: depth})
					}
				} else {
					delete(sw.desiredSymbols, symbol)
					if activeDepth, ok := sw.activeSymbols[symbol]; ok {
						pendingUnsubs = queueUniqueSymbolDepth(pendingUnsubs, symbolDepth{Symbol: symbol, Depth: activeDepth})
					}
				}
			}
			sw.mu.Unlock()
		case <-batchTicker.C:
			if err := flushCommands(); err != nil {
				return err
			}
		case <-pingTicker.C:
			pingCmd := wsCommand{Method: "server.ping", Params: map[string]interface{}{}, ID: sw.nextRequestID()}
			if err := conn.WriteJSON(pingCmd); err != nil {
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

func (sw *OrderBookShardWorker) handleTextMessage(payload []byte) (*shared_types.OrderBookUpdate, error) {
	var envelope wsDepthEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return nil, fmt.Errorf("decode ws payload: %w", err)
	}
	if envelope.Error != nil {
		return nil, fmt.Errorf("coinex ws error code=%d message=%s", envelope.Error.Code, envelope.Error.Message)
	}
	if envelope.Method != "depth.update" || envelope.Data == nil {
		return nil, nil
	}

	ingestNow := time.Now()
	goTimestamp := ingestNow.UnixMilli()
	update, err := NormalizeOrderBook(sw.marketType, envelope.Data.Market, envelope.Data.Depth, goTimestamp, ingestNow.UnixNano())
	if err != nil {
		metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeOBUpdate)
		return nil, err
	}
	if !envelope.Data.IsFull {
		update.UpdateType = "delta"
	} else {
		update.UpdateType = "snapshot"
	}
	return update, nil
}

func (sw *OrderBookShardWorker) sendCommand(conn *websocket.Conn, method string, items []symbolDepth) error {
	marketList := make([][]interface{}, 0, len(items))
	for _, item := range items {
		marketList = append(marketList, []interface{}{item.Symbol, NormalizeOrderBookDepth(item.Depth), "0", true})
	}
	return conn.WriteJSON(wsCommand{
		Method: method,
		Params: map[string]interface{}{"market_list": marketList},
		ID:     sw.nextRequestID(),
	})
}

func (sw *OrderBookShardWorker) sendUnsubscribe(conn *websocket.Conn, items []symbolDepth) error {
	marketList := make([]string, 0, len(items))
	for _, item := range items {
		marketList = append(marketList, item.Symbol)
	}
	return conn.WriteJSON(wsCommand{
		Method: "depth.unsubscribe",
		Params: map[string]interface{}{"market_list": marketList},
		ID:     sw.nextRequestID(),
	})
}

func (sw *OrderBookShardWorker) emitStatusForSymbols(eventType string, symbols []string, reason string, attempt int, message string) {
	if sw.statusCh == nil {
		return
	}
	now := time.Now().UnixMilli()
	for _, symbol := range symbols {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "coinex",
			MarketType: sw.marketType,
			DataType:   "orderbooks",
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
			Exchange:   "coinex",
			MarketType: sw.marketType,
			DataType:   "orderbooks",
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  now,
		}
	}
}

func gunzipCoinexPayload(payload []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}
