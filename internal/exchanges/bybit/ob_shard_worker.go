package bybit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
)

// localOrderbook haelt den Zustand und die UpdateID
type localOrderbook struct {
	Bids         map[string]shared_types.OrderBookLevel
	Asks         map[string]shared_types.OrderBookLevel
	LastUpdateID int64
}

// OrderBookShardWorker verwaltet eine einzelne WebSocket-Verbindung
type OrderBookShardWorker struct {
	wsURL               string
	marketType          string
	commandCh           chan ManagerCommand
	stopCh              <-chan struct{}
	dataCh              chan<- *shared_types.OrderBookUpdate
	wg                  *sync.WaitGroup
	mu                  sync.Mutex
	conn                *websocket.Conn
	orderbooks          map[string]*localOrderbook
	activeSubscriptions map[string]int // topic -> depth
	topicSymbols        map[string]string
}

var (
	bybitOrderBookTopicNeedle = []byte(`"topic":"orderbook.`)
	bybitPongNeedle           = []byte(`"op":"pong"`)
)

func NewOrderBookShardWorker(wsURL, marketType string, stopCh <-chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, wg *sync.WaitGroup) *OrderBookShardWorker {
	return &OrderBookShardWorker{
		wsURL:               wsURL,
		marketType:          marketType,
		commandCh:           make(chan ManagerCommand, 50),
		stopCh:              stopCh,
		dataCh:              dataCh,
		wg:                  wg,
		orderbooks:          make(map[string]*localOrderbook),
		activeSubscriptions: make(map[string]int),
		topicSymbols:        make(map[string]string),
	}
}

func getBybitDepth(requestedDepth int) int {
	if requestedDepth <= 1 {
		return 1
	}
	if requestedDepth <= 50 {
		return 50
	}
	if requestedDepth <= 200 {
		return 200
	}
	return 500
}

func (sw *OrderBookShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[BYBIT-OB-SHARD] Starte OrderBook Worker fuer %s", sw.marketType)
	var err error
	for {
		sw.conn, _, err = websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[BYBIT-OB-SHARD-ERROR] Connect fehlgeschlagen: %v. Versuche in 5s erneut.", err)
			time.Sleep(5 * time.Second)
			continue
		}
		log.Printf("[BYBIT-OB-SHARD] Erfolgreich verbunden mit %s", sw.wsURL)
		sw.resubscribeAll()
		ctx, cancel := context.WithCancel(context.Background())
		go sw.readLoop(ctx)
		sw.writeLoop(ctx)
		cancel()
		sw.conn.Close()
		log.Printf("[BYBIT-OB-SHARD] Verbindung getrennt. Versuche Reconnect...")
		select {
		case <-sw.stopCh:
			log.Printf("[BYBIT-OB-SHARD] Worker wird beendet.")
			return
		default:
		}
	}
}

func (sw *OrderBookShardWorker) writeLoop(ctx context.Context) {
	pingTicker := time.NewTicker(pingEverySec * time.Second)
	defer pingTicker.Stop()
	for {
		select {
		case cmd := <-sw.commandCh:
			depth := getBybitDepth(cmd.Depth)
			topic := fmt.Sprintf("orderbook.%d.%s", depth, cmd.Symbol)
			sw.mu.Lock()
			if cmd.Action == "subscribe" {
				sw.activeSubscriptions[topic] = cmd.Depth
				sw.topicSymbols[topic] = TranslateSymbolFromExchange(cmd.Symbol, sw.marketType)
			} else {
				delete(sw.activeSubscriptions, topic)
				delete(sw.orderbooks, topic)
				delete(sw.topicSymbols, topic)
			}
			sw.mu.Unlock()
			sw.sendSubscription(cmd.Action, []string{topic})
		case <-pingTicker.C:
			if err := sw.conn.WriteJSON(map[string]string{"op": "ping"}); err != nil {
				log.Printf("[BYBIT-OB-SHARD-ERROR] Ping fehlgeschlagen: %v", err)
				return
			}
		case <-ctx.Done():
			return
		case <-sw.stopCh:
			return
		}
	}
}

func (sw *OrderBookShardWorker) readLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, msg, err := sw.conn.ReadMessage()
			if err != nil {
				log.Printf("[BYBIT-OB-SHARD-ERROR] Fehler beim Lesen: %v", err)
				return
			}
			ingestUnixNano := time.Now().UnixNano()

			if !bytes.Contains(msg, bybitOrderBookTopicNeedle) {
				continue
			}
			if bytes.Contains(msg, bybitPongNeedle) {
				continue
			}

			var obMsg wsOrderBookMsg
			if err := json.Unmarshal(msg, &obMsg); err != nil {
				metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeOBUpdate)
				continue
			}

			if obMsg.Topic == "" {
				continue
			}

			var updateToSend *shared_types.OrderBookUpdate

			sw.mu.Lock()
			if obMsg.Type == "snapshot" {
				updateToSend = sw.handleSmartSnapshot(obMsg.Topic, obMsg.Data, obMsg.Timestamp, ingestUnixNano)
			} else if obMsg.Type == "delta" {
				updateToSend = sw.handleDelta(obMsg.Topic, obMsg.Data, obMsg.Timestamp, ingestUnixNano)
			}
			sw.mu.Unlock()

			if updateToSend != nil {
				sw.dataCh <- updateToSend
			}
		}
	}
}

func (sw *OrderBookShardWorker) handleSmartSnapshot(topic string, data wsOrderBookData, ts int64, ingestUnixNano int64) *shared_types.OrderBookUpdate {
	book, ok := sw.orderbooks[topic]
	if !ok {
		book = &localOrderbook{
			Bids: make(map[string]shared_types.OrderBookLevel),
			Asks: make(map[string]shared_types.OrderBookLevel),
		}
		sw.orderbooks[topic] = book
	}
	book.LastUpdateID = data.UpdateID

	if len(data.Bids) > 0 {
		clear(book.Bids)
		for _, level := range data.Bids {
			if len(level) < 2 {
				continue
			}
			price, err := strconv.ParseFloat(level[0], 64)
			if err != nil {
				continue
			}
			amount, err := strconv.ParseFloat(level[1], 64)
			if err != nil {
				continue
			}
			book.Bids[level[0]] = shared_types.OrderBookLevel{Price: price, Amount: amount}
		}
	}
	if len(data.Asks) > 0 {
		clear(book.Asks)
		for _, level := range data.Asks {
			if len(level) < 2 {
				continue
			}
			price, err := strconv.ParseFloat(level[0], 64)
			if err != nil {
				continue
			}
			amount, err := strconv.ParseFloat(level[1], 64)
			if err != nil {
				continue
			}
			book.Asks[level[0]] = shared_types.OrderBookLevel{Price: price, Amount: amount}
		}
	}
	return sw.createUpdate(topic, ts, ingestUnixNano, metrics.TypeOBSnapshot)
}

func (sw *OrderBookShardWorker) handleDelta(topic string, data wsOrderBookData, ts int64, ingestUnixNano int64) *shared_types.OrderBookUpdate {
	book, ok := sw.orderbooks[topic]
	if !ok {
		return nil
	}
	if data.UpdateID <= book.LastUpdateID {
		metrics.RecordDropped(metrics.ReasonStaleSeq, metrics.TypeOBUpdate)
		return nil
	}

	book.LastUpdateID = data.UpdateID
	for _, level := range data.Bids {
		if len(level) < 2 {
			continue
		}
		if level[1] == "0" {
			delete(book.Bids, level[0])
		} else {
			amount, err := strconv.ParseFloat(level[1], 64)
			if err != nil {
				continue
			}
			price := 0.0
			if existing, ok := book.Bids[level[0]]; ok {
				price = existing.Price
			} else {
				price, err = strconv.ParseFloat(level[0], 64)
				if err != nil {
					continue
				}
			}
			book.Bids[level[0]] = shared_types.OrderBookLevel{Price: price, Amount: amount}
		}
	}
	for _, level := range data.Asks {
		if len(level) < 2 {
			continue
		}
		if level[1] == "0" {
			delete(book.Asks, level[0])
		} else {
			amount, err := strconv.ParseFloat(level[1], 64)
			if err != nil {
				continue
			}
			price := 0.0
			if existing, ok := book.Asks[level[0]]; ok {
				price = existing.Price
			} else {
				price, err = strconv.ParseFloat(level[0], 64)
				if err != nil {
					continue
				}
			}
			book.Asks[level[0]] = shared_types.OrderBookLevel{Price: price, Amount: amount}
		}
	}
	return sw.createUpdate(topic, ts, ingestUnixNano, metrics.TypeOBUpdate)
}

func (sw *OrderBookShardWorker) createUpdate(topic string, ts int64, ingestUnixNano int64, updateType string) *shared_types.OrderBookUpdate {
	book, ok := sw.orderbooks[topic]
	if !ok {
		return nil
	}

	update := pools.GetOrderBookUpdate()

	update.Exchange = "bybit"
	if translated, ok := sw.topicSymbols[topic]; ok {
		update.Symbol = translated
	} else {
		lastDot := strings.LastIndexByte(topic, '.')
		if lastDot < 0 || lastDot+1 >= len(topic) {
			pools.PutOrderBookUpdate(update)
			return nil
		}
		update.Symbol = TranslateSymbolFromExchange(topic[lastDot+1:], sw.marketType)
	}
	update.MarketType = sw.marketType

	update.Timestamp = ts
	update.GoTimestamp = time.Now().UnixMilli()
	update.IngestUnixNano = ingestUnixNano
	update.UpdateType = updateType

	requestedDepth := sw.activeSubscriptions[topic]
	if requestedDepth <= 0 {
		requestedDepth = 20
	}
	if requestedDepth > 20 {
		requestedDepth = 20
	}

	update.Bids = mapToTopLevels(book.Bids, update.Bids, true, requestedDepth)
	update.Asks = mapToTopLevels(book.Asks, update.Asks, false, requestedDepth)

	return update
}

func (sw *OrderBookShardWorker) sendSubscription(op string, topics []string) {
	if len(topics) == 0 || sw.conn == nil {
		return
	}
	msg := map[string]interface{}{"op": op, "args": topics}
	if err := sw.conn.WriteJSON(msg); err != nil {
		log.Printf("[BYBIT-OB-SHARD-ERROR] Subscription '%s' fehlgeschlagen: %v", op, err)
	}
}

func (sw *OrderBookShardWorker) resubscribeAll() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if len(sw.activeSubscriptions) > 0 {
		topics := make([]string, 0, len(sw.activeSubscriptions))
		for topic := range sw.activeSubscriptions {
			topics = append(topics, topic)
		}
		log.Printf("[BYBIT-OB-SHARD] Re-subscribing to %d topics upon reconnect...", len(topics))
		sw.sendSubscription("subscribe", topics)
	}
}

func mapToTopLevels(priceMap map[string]shared_types.OrderBookLevel, slice []shared_types.OrderBookLevel, sortDesc bool, limit int) []shared_types.OrderBookLevel {
	if limit <= 0 {
		limit = 20
	}
	if slice != nil {
		slice = slice[:0]
	}
	if len(priceMap) == 0 {
		return slice
	}

	for _, level := range priceMap {
		price := level.Price

		insertAt := 0
		for insertAt < len(slice) {
			if sortDesc {
				if price > slice[insertAt].Price {
					break
				}
			} else {
				if price < slice[insertAt].Price {
					break
				}
			}
			insertAt++
		}

		if len(slice) < limit {
			slice = append(slice, shared_types.OrderBookLevel{})
		} else if insertAt >= limit {
			continue
		}

		copy(slice[insertAt+1:], slice[insertAt:len(slice)-1])
		slice[insertAt] = level
		if len(slice) > limit {
			slice = slice[:limit]
		}
	}

	return slice
}
