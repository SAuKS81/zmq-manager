package bybit

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
)

// localOrderbook hält den Zustand und die UpdateID
type localOrderbook struct {
	Bids         map[string]string
	Asks         map[string]string
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
}

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
	log.Printf("[BYBIT-OB-SHARD] Starte OrderBook Worker für %s", sw.marketType)
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
			} else {
				delete(sw.activeSubscriptions, topic)
				// WICHTIG: Hier muss der Key der Topic sein, nicht das Symbol!
				// Dies war ein subtiler Bug in der vorherigen Version.
				delete(sw.orderbooks, topic)
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

			// +++ LOGGING 1 +++
			// Wir protokollieren die rohe Nachricht, aber nur, wenn es ein Orderbuch ist.
			if strings.Contains(string(msg), `"topic":"orderbook.`) {
				// log.Printf("[BYBIT-OB-SHARD-RECV] Rohe WS-Nachricht: %s", string(msg))
			}

			if strings.Contains(string(msg), `"op":"pong"`) {
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
				// +++ LOGGING 2 +++
				// Wir protokollieren das normalisierte Objekt, das wir versenden.
				// Um die Ausgabe lesbar zu halten, protokollieren wir nur die Anzahl der Bids/Asks.
				// log.Printf("[BYBIT-OB-SHARD-SEND] Sende Update für %s: Bids=%d, Asks=%d",
				// 	updateToSend.Symbol, len(updateToSend.Bids), len(updateToSend.Asks))

				sw.dataCh <- updateToSend
			}
		}
	}
}

// Ich habe die intelligente Snapshot-Funktion umbenannt, um sie klar zu kennzeichnen.
func (sw *OrderBookShardWorker) handleSmartSnapshot(topic string, data wsOrderBookData, ts int64, ingestUnixNano int64) *shared_types.OrderBookUpdate {
	book, ok := sw.orderbooks[topic]
	if !ok {
		book = &localOrderbook{
			Bids: make(map[string]string),
			Asks: make(map[string]string),
		}
		sw.orderbooks[topic] = book
	}
	book.LastUpdateID = data.UpdateID

	if len(data.Bids) > 0 {
		book.Bids = make(map[string]string)
		for _, level := range data.Bids {
			book.Bids[level[0]] = level[1]
		}
	}
	if len(data.Asks) > 0 {
		book.Asks = make(map[string]string)
		for _, level := range data.Asks {
			book.Asks[level[0]] = level[1]
		}
	}
	// ts weitergeben
	return sw.createUpdate(topic, ts, ingestUnixNano, metrics.TypeOBSnapshot)
}

// FIX: Parameter 'ts int64' hinzugefügt
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
		if level[1] == "0" {
			delete(book.Bids, level[0])
		} else {
			book.Bids[level[0]] = level[1]
		}
	}
	for _, level := range data.Asks {
		if level[1] == "0" {
			delete(book.Asks, level[0])
		} else {
			book.Asks[level[0]] = level[1]
		}
	}
	// ts weitergeben
	return sw.createUpdate(topic, ts, ingestUnixNano, metrics.TypeOBUpdate)
}

func (sw *OrderBookShardWorker) createUpdate(topic string, ts int64, ingestUnixNano int64, updateType string) *shared_types.OrderBookUpdate {
	book, ok := sw.orderbooks[topic]
	if !ok {
		return nil
	}
	parts := strings.Split(topic, ".")
	if len(parts) != 3 {
		return nil
	}
	symbol := parts[2]

	update := pools.GetOrderBookUpdate()

	update.Exchange = "bybit"
	update.Symbol = TranslateSymbolFromExchange(symbol, sw.marketType)
	update.MarketType = sw.marketType

	// KORREKTUR: Wir nutzen den echten Timestamp von Bybit, nicht die ID!
	update.Timestamp = ts
	update.GoTimestamp = time.Now().UnixMilli()
	update.IngestUnixNano = ingestUnixNano
	update.UpdateType = updateType

	update.Bids = mapToSlice(book.Bids, update.Bids, true)
	update.Asks = mapToSlice(book.Asks, update.Asks, false)

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

func mapToSlice(priceMap map[string]string, slice []shared_types.OrderBookLevel, sortDesc bool) []shared_types.OrderBookLevel {
	if slice != nil {
		slice = slice[:0]
	}
	for p, s := range priceMap {
		price, _ := strconv.ParseFloat(p, 64)
		size, _ := strconv.ParseFloat(s, 64)
		slice = append(slice, shared_types.OrderBookLevel{Price: price, Amount: size})
	}
	sort.Slice(slice, func(i, j int) bool {
		if sortDesc {
			return slice[i].Price > slice[j].Price
		}
		return slice[i].Price < slice[j].Price
	})
	return slice
}
