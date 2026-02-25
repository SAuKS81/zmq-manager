package broker

import (
	// "fmt"
	"log"
	"strings"
	"sync/atomic"
	"time"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/exchanges/binance"
	"bybit-watcher/internal/exchanges/bitget"
	"bybit-watcher/internal/exchanges/bybit"
	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
)

type SubscriptionManager struct {
	RequestCh              chan *shared_types.ClientRequest
	DistributionCh         chan<- *DistributionMessage
	TradeDataCh            chan *shared_types.TradeUpdate
	OrderBookCh            chan *shared_types.OrderBookUpdate
	tradeSubscriptions     map[string]map[string]bool
	orderBookSubscriptions map[string]map[string]bool
	exchangeRegistry       map[string]exchanges.Exchange
	wildcardSubscribers    map[string]map[string]bool
	incomingTradeCounter   atomic.Uint64
	incomingOBCounter      atomic.Uint64
	totalDataReceived      atomic.Uint64
}

func NewSubscriptionManager(distributionCh chan<- *DistributionMessage) *SubscriptionManager {
	sm := &SubscriptionManager{
		RequestCh:              make(chan *shared_types.ClientRequest, 100),
		DistributionCh:         distributionCh,
		TradeDataCh:            make(chan *shared_types.TradeUpdate, 10000),
		OrderBookCh:            make(chan *shared_types.OrderBookUpdate, 5000),
		tradeSubscriptions:     make(map[string]map[string]bool),
		orderBookSubscriptions: make(map[string]map[string]bool),
		exchangeRegistry:       make(map[string]exchanges.Exchange),
		wildcardSubscribers:    make(map[string]map[string]bool),
	}
	sm.exchangeRegistry["bybit_native"] = bybit.NewBybitExchange(sm.RequestCh, sm.TradeDataCh, sm.OrderBookCh)
	sm.exchangeRegistry["binance_native"] = binance.NewBinanceExchange(sm.RequestCh, sm.TradeDataCh, sm.OrderBookCh)
	sm.exchangeRegistry["bitget_native"] = bitget.NewBitgetExchange(sm.RequestCh, sm.TradeDataCh)
	registerCCXT(sm)
	return sm
}

func (sm *SubscriptionManager) Run() {
	log.Println("[SUB-MANAGER] Startet (Low-Latency Mode)...")
	go sm.logIncomingRate()

	// Puffer für Batches wiederverwenden
	const maxBatchSize = 250
	tradeBatch := make([]*shared_types.TradeUpdate, 0, maxBatchSize)
	obBatch := make([]*shared_types.OrderBookUpdate, 0, maxBatchSize)

	for {
		select {
		case req := <-sm.RequestCh:
			sm.handleRequest(req)

		// --- TRADES ---
		case firstTrade := <-sm.TradeDataCh:
			// 1. Ersten Trade hinzufügen
			sm.incomingTradeCounter.Add(1)
			sm.totalDataReceived.Add(1)
			firstTrade.DataType = "trades"
			metrics.RecordIngest(firstTrade.Exchange, metrics.TypeTrade)
			tradeBatch = append(tradeBatch, firstTrade)

			// 2. "Greedy" Loop: Schau, ob noch mehr im Channel ist, bis Batch voll
		TradeLoop:
			for len(tradeBatch) < maxBatchSize {
				select {
				case nextTrade := <-sm.TradeDataCh:
					sm.incomingTradeCounter.Add(1)
					sm.totalDataReceived.Add(1)
					nextTrade.DataType = "trades"
					metrics.RecordIngest(nextTrade.Exchange, metrics.TypeTrade)
					tradeBatch = append(tradeBatch, nextTrade)
				default:
					// Channel leer? Sofort senden! Nicht warten!
					break TradeLoop
				}
			}

			// 3. Verarbeiten & Senden
			sm.processTradeBatch(tradeBatch)

			// 4. Batch leeren (Kapazität behalten)
			tradeBatch = tradeBatch[:0]

		// --- ORDERBOOKS ---
		case firstOB := <-sm.OrderBookCh:
			sm.incomingOBCounter.Add(1)
			sm.totalDataReceived.Add(1)
			firstOB.DataType = "orderbooks"
			metrics.RecordIngest(firstOB.Exchange, orderBookMetricType(firstOB))
			obBatch = append(obBatch, firstOB)

		OBLoop:
			for len(obBatch) < maxBatchSize {
				select {
				case nextOB := <-sm.OrderBookCh:
					sm.incomingOBCounter.Add(1)
					sm.totalDataReceived.Add(1)
					nextOB.DataType = "orderbooks"
					metrics.RecordIngest(nextOB.Exchange, orderBookMetricType(nextOB))
					obBatch = append(obBatch, nextOB)
				default:
					break OBLoop
				}
			}

			sm.processOrderBookBatch(obBatch)
			obBatch = obBatch[:0]
		}
	}
}

func (sm *SubscriptionManager) processTradeBatch(batch []*shared_types.TradeUpdate) {
	if len(batch) == 0 {
		return
	}

	clientBatches := make(map[string][]*shared_types.TradeUpdate)

	for _, trade := range batch {
		subID := getSubscriptionID(trade.Exchange, trade.Symbol, trade.MarketType)
		targetClients := make(map[string]bool)

		if clients, ok := sm.tradeSubscriptions[subID]; ok {
			for clientIDStr := range clients {
				targetClients[clientIDStr] = true
			}
		}

		wildcardID := trade.Exchange + "-" + trade.MarketType + "-all"
		if wildClients, ok := sm.wildcardSubscribers[wildcardID]; ok {
			for clientIDStr := range wildClients {
				targetClients[clientIDStr] = true
			}
		}

		for clientIDStr := range targetClients {
			clientBatches[clientIDStr] = append(clientBatches[clientIDStr], trade)
		}
	}

	for clientIDStr, trades := range clientBatches {
		sm.DistributionCh <- &DistributionMessage{
			ClientIDs:  [][]byte{[]byte(clientIDStr)},
			RawPayload: trades,
		}
	}
}

func (sm *SubscriptionManager) processOrderBookBatch(batch []*shared_types.OrderBookUpdate) {
	if len(batch) == 0 {
		return
	}

	clientBatches := make(map[string][]*shared_types.OrderBookUpdate)

	for _, ob := range batch {
		subID := getSubscriptionID(ob.Exchange, ob.Symbol, ob.MarketType)
		if clients, ok := sm.orderBookSubscriptions[subID]; ok {
			for clientIDStr := range clients {
				clientBatches[clientIDStr] = append(clientBatches[clientIDStr], ob)
			}
		}
	}

	for clientIDStr, updates := range clientBatches {
		debugSym := ""
		if len(updates) > 0 {
			debugSym = updates[0].Symbol
		}

		sm.DistributionCh <- &DistributionMessage{
			ClientIDs:   [][]byte{[]byte(clientIDStr)},
			RawPayload:  updates,
			DebugSymbol: debugSym,
		}
	}
}

func (sm *SubscriptionManager) handleRequest(req *shared_types.ClientRequest) {
	if req.DataType == "" {
		req.DataType = "trades"
	}
	exchangeNameForSubID := strings.Split(req.Exchange, "_")[0]
	if req.Action == "subscribe_all" && req.DataType == "trades" {
		wildcardID := exchangeNameForSubID + "-" + req.MarketType + "-all"
		if _, ok := sm.wildcardSubscribers[wildcardID]; !ok {
			sm.wildcardSubscribers[wildcardID] = make(map[string]bool)
		}
		sm.wildcardSubscribers[wildcardID][string(req.ClientID)] = true
		log.Printf("[SUB-MANAGER] Client %s hat 'subscribe_all' für Trades auf %s aktiviert.", string(req.ClientID), wildcardID)
	}

	subID := getSubscriptionID(exchangeNameForSubID, req.Symbol, req.MarketType)
	clientIDStr := string(req.ClientID)

	var subMap map[string]map[string]bool
	if req.DataType == "orderbooks" {
		subMap = sm.orderBookSubscriptions
	} else {
		subMap = sm.tradeSubscriptions
	}

	switch req.Action {
	case "subscribe":
		if _, ok := subMap[subID]; !ok {
			subMap[subID] = make(map[string]bool)
		}
		subMap[subID][clientIDStr] = true
	case "unsubscribe":
		if clients, ok := subMap[subID]; ok {
			delete(clients, clientIDStr)
			if len(clients) == 0 {
				delete(subMap, subID)
			}
		}
	case "disconnect":
		sm.cleanupClientSubscriptions(clientIDStr)
		return
	}

	var handler exchanges.Exchange
	specificHandler, ok := sm.exchangeRegistry[req.Exchange]
	if ok {
		handler = specificHandler
	} else {
		handler = sm.exchangeRegistry["ccxt_generic"]
	}

	if handler != nil {
		handler.HandleRequest(req)
	} else {
		log.Printf("[SUB-MANAGER] FATAL: Kein passender Handler für die Anfrage gefunden: Exchange=%s", req.Exchange)
	}
}

func getSubscriptionID(exchange, symbol, marketType string) string {
	return exchange + "-" + marketType + "-" + symbol
}

func (sm *SubscriptionManager) logIncomingRate() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		tradesCount := sm.incomingTradeCounter.Swap(0)
		obCount := sm.incomingOBCounter.Swap(0)
		totalCount := sm.totalDataReceived.Load()

		log.Printf(
			"[STATS] 10s Rate -> Trades: %d | OrderBooks: %d || Gesamt-Events: %d",
			tradesCount, obCount, totalCount,
		)
	}
}

func (sm *SubscriptionManager) GetTotalTradesReceived() uint64 {
	return sm.totalDataReceived.Load()
}

func orderBookMetricType(ob *shared_types.OrderBookUpdate) string {
	if ob != nil && ob.UpdateType == metrics.TypeOBSnapshot {
		return metrics.TypeOBSnapshot
	}
	return metrics.TypeOBUpdate
}

func (sm *SubscriptionManager) cleanupClientSubscriptions(clientID string) {
	for subID, clients := range sm.tradeSubscriptions {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(sm.tradeSubscriptions, subID)
		}
	}

	for subID, clients := range sm.orderBookSubscriptions {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(sm.orderBookSubscriptions, subID)
		}
	}

	for wildcardID, clients := range sm.wildcardSubscribers {
		delete(clients, clientID)
		if len(clients) == 0 {
			delete(sm.wildcardSubscribers, wildcardID)
		}
	}
}
