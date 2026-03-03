package broker

import (
	// "fmt"
	"log"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/exchanges/binance"
	"bybit-watcher/internal/exchanges/bitget"
	"bybit-watcher/internal/exchanges/bybit"
	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
)

type SubscriptionManager struct {
	RequestCh                   chan *shared_types.ClientRequest
	StatusCh                    chan *shared_types.StreamStatusEvent
	DistributionCh              chan<- *DistributionMessage
	TradeDataCh                 chan *shared_types.TradeUpdate
	OrderBookCh                 chan *shared_types.OrderBookUpdate
	tradeSubscriptions          map[string]map[string]bool
	orderBookSubscriptions      map[string]map[string]bool
	tradeSubscriptionRoutes     map[string]string
	orderBookSubscriptionRoutes map[string]string
	orderBookSubscriptionDepths map[string]int
	exchangeRegistry            map[string]exchanges.Exchange
	wildcardSubscribers         map[string]map[string]bool
	wildcardRoutes              map[string]string
	incomingTradeCounter        atomic.Uint64
	incomingOBCounter           atomic.Uint64
	totalDataReceived           atomic.Uint64
	runtimeTracker              *runtimeTracker
}

func NewSubscriptionManager(distributionCh chan<- *DistributionMessage) *SubscriptionManager {
	sm := &SubscriptionManager{
		RequestCh:                   make(chan *shared_types.ClientRequest, 100),
		StatusCh:                    make(chan *shared_types.StreamStatusEvent, 1024),
		DistributionCh:              distributionCh,
		TradeDataCh:                 make(chan *shared_types.TradeUpdate, 10000),
		OrderBookCh:                 make(chan *shared_types.OrderBookUpdate, 5000),
		tradeSubscriptions:          make(map[string]map[string]bool),
		orderBookSubscriptions:      make(map[string]map[string]bool),
		tradeSubscriptionRoutes:     make(map[string]string),
		orderBookSubscriptionRoutes: make(map[string]string),
		orderBookSubscriptionDepths: make(map[string]int),
		exchangeRegistry:            make(map[string]exchanges.Exchange),
		wildcardSubscribers:         make(map[string]map[string]bool),
		wildcardRoutes:              make(map[string]string),
		runtimeTracker:              newRuntimeTracker(),
	}
	sm.exchangeRegistry["bybit_native"] = bybit.NewBybitExchange(sm.RequestCh, sm.TradeDataCh, sm.OrderBookCh, sm.StatusCh)
	sm.exchangeRegistry["binance_native"] = binance.NewBinanceExchange(sm.RequestCh, sm.TradeDataCh, sm.OrderBookCh, sm.StatusCh)
	sm.exchangeRegistry["bitget_native"] = bitget.NewBitgetExchange(sm.RequestCh, sm.TradeDataCh, sm.StatusCh)
	registerCCXT(sm)
	return sm
}

func (sm *SubscriptionManager) Run() {
	log.Println("[SUB-MANAGER] Startet (Low-Latency Mode)...")
	go sm.logIncomingRate()

	const (
		tradeBatchMaxSize = 32
		tradeBatchWindow  = 10 * time.Millisecond
		obBatchMaxSize    = 250
	)
	tradeBatch := make([]*shared_types.TradeUpdate, 0, tradeBatchMaxSize)
	obBatch := make([]*shared_types.OrderBookUpdate, 0, obBatchMaxSize)

	for {
		select {
		case req := <-sm.RequestCh:
			sm.handleRequest(req)
		case status := <-sm.StatusCh:
			sm.processStatusEvent(status)

		case firstTrade := <-sm.TradeDataCh:
			sm.incomingTradeCounter.Add(1)
			sm.totalDataReceived.Add(1)
			firstTrade.DataType = "trades"
			metrics.RecordIngest(firstTrade.Exchange, metrics.TypeTrade)
			tradeBatch = append(tradeBatch, firstTrade)

			deadline := time.NewTimer(tradeBatchWindow)
		TradeLoop:
			for len(tradeBatch) < tradeBatchMaxSize {
				select {
				case nextTrade := <-sm.TradeDataCh:
					sm.incomingTradeCounter.Add(1)
					sm.totalDataReceived.Add(1)
					nextTrade.DataType = "trades"
					metrics.RecordIngest(nextTrade.Exchange, metrics.TypeTrade)
					tradeBatch = append(tradeBatch, nextTrade)
				case <-deadline.C:
					break TradeLoop
				}
			}
			if !deadline.Stop() {
				select {
				case <-deadline.C:
				default:
				}
			}

			sm.processTradeBatch(tradeBatch)
			tradeBatch = tradeBatch[:0]

		case firstOB := <-sm.OrderBookCh:
			sm.incomingOBCounter.Add(1)
			sm.totalDataReceived.Add(1)
			firstOB.DataType = "orderbooks"
			metrics.RecordIngest(firstOB.Exchange, orderBookMetricType(firstOB))
			obBatch = append(obBatch, firstOB)

		OBLoop:
			for len(obBatch) < obBatchMaxSize {
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

func (sm *SubscriptionManager) processStatusEvent(event *shared_types.StreamStatusEvent) {
	if event == nil {
		return
	}
	for _, symbol := range runtimeSymbolAliases(event.Exchange, event.Symbol, event.MarketType) {
		sm.runtimeTracker.recordStatusForSymbol(event, symbol)
	}
	for _, symbol := range event.Symbols {
		for _, alias := range runtimeSymbolAliases(event.Exchange, symbol, event.MarketType) {
			sm.runtimeTracker.recordStatusForSymbol(event, alias)
		}
	}
	if sm.DistributionCh == nil {
		return
	}

	targetClients := make(map[string]bool)
	symbols := make([]string, 0, 1+len(event.Symbols))
	if event.Symbol != "" {
		symbols = append(symbols, event.Symbol)
	}
	symbols = append(symbols, event.Symbols...)

	for _, symbol := range symbols {
		if symbol == "" {
			continue
		}
		for _, alias := range runtimeSymbolAliases(event.Exchange, symbol, event.MarketType) {
			subID := getSubscriptionID(event.Exchange, alias, event.MarketType)
			if event.DataType == "orderbooks" {
				if clients, ok := sm.orderBookSubscriptions[subID]; ok {
					for clientID := range clients {
						targetClients[clientID] = true
					}
				}
				continue
			}
			if clients, ok := sm.tradeSubscriptions[subID]; ok {
				for clientID := range clients {
					targetClients[clientID] = true
				}
			}
			wildcardID := event.Exchange + "-" + event.MarketType + "-all"
			if wildClients, ok := sm.wildcardSubscribers[wildcardID]; ok {
				for clientID := range wildClients {
					targetClients[clientID] = true
				}
			}
		}
	}

	if len(targetClients) == 0 {
		return
	}

	clientIDs := make([][]byte, 0, len(targetClients))
	for clientID := range targetClients {
		clientIDs = append(clientIDs, []byte(clientID))
	}
	sm.DistributionCh <- &DistributionMessage{ClientIDs: clientIDs, RawPayload: event}
}

func (sm *SubscriptionManager) processTradeBatch(batch []*shared_types.TradeUpdate) {
	if len(batch) == 0 {
		return
	}

	clientBatches := make(map[string][]*shared_types.TradeUpdate)

	for _, trade := range batch {
		targetClients := make(map[string]bool)
		for _, alias := range runtimeSymbolAliases(trade.Exchange, trade.Symbol, trade.MarketType) {
			sm.runtimeTracker.recordTradeForSymbol(trade, alias)
			subID := getSubscriptionID(trade.Exchange, alias, trade.MarketType)
			if clients, ok := sm.tradeSubscriptions[subID]; ok {
				for clientIDStr := range clients {
					targetClients[clientIDStr] = true
				}
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

	toRelease := append([]*shared_types.TradeUpdate(nil), batch...)
	if len(clientBatches) == 0 {
		for _, trade := range toRelease {
			if trade != nil {
				pools.PutTradeUpdate(trade)
			}
		}
		return
	}

	var remaining atomic.Int32
	remaining.Store(int32(len(clientBatches)))
	onComplete := func() {
		if remaining.Add(-1) != 0 {
			return
		}
		for _, trade := range toRelease {
			if trade != nil {
				pools.PutTradeUpdate(trade)
			}
		}
	}

	for clientIDStr, trades := range clientBatches {
		sm.DistributionCh <- &DistributionMessage{
			ClientIDs:  [][]byte{[]byte(clientIDStr)},
			RawPayload: trades,
			OnComplete: onComplete,
		}
	}
}

func (sm *SubscriptionManager) processOrderBookBatch(batch []*shared_types.OrderBookUpdate) {
	if len(batch) == 0 {
		return
	}

	clientBatches := make(map[string][]*shared_types.OrderBookUpdate)

	for _, ob := range batch {
		for _, alias := range runtimeSymbolAliases(ob.Exchange, ob.Symbol, ob.MarketType) {
			sm.runtimeTracker.recordOrderBookForSymbol(ob, alias)
			subID := getSubscriptionID(ob.Exchange, alias, ob.MarketType)
			if clients, ok := sm.orderBookSubscriptions[subID]; ok {
				for clientIDStr := range clients {
					clientBatches[clientIDStr] = append(clientBatches[clientIDStr], ob)
				}
			}
		}
	}

	toRelease := append([]*shared_types.OrderBookUpdate(nil), batch...)
	if len(clientBatches) == 0 {
		for _, ob := range toRelease {
			if ob != nil {
				pools.PutOrderBookUpdate(ob)
			}
		}
		return
	}

	var remaining atomic.Int32
	remaining.Store(int32(len(clientBatches)))
	onComplete := func() {
		if remaining.Add(-1) != 0 {
			return
		}
		for _, ob := range toRelease {
			if ob != nil {
				pools.PutOrderBookUpdate(ob)
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
			OnComplete:  onComplete,
		}
	}
}

func (sm *SubscriptionManager) handleRequest(req *shared_types.ClientRequest) {
	if req.DataType == "" {
		req.DataType = "trades"
	}
	if sm.tradeSubscriptionRoutes == nil {
		sm.tradeSubscriptionRoutes = make(map[string]string)
	}
	if sm.orderBookSubscriptionRoutes == nil {
		sm.orderBookSubscriptionRoutes = make(map[string]string)
	}
	if sm.orderBookSubscriptionDepths == nil {
		sm.orderBookSubscriptionDepths = make(map[string]int)
	}
	if sm.wildcardRoutes == nil {
		sm.wildcardRoutes = make(map[string]string)
	}
	switch req.Action {
	case "list_subscriptions":
		sm.sendJSONSnapshot(req.ClientID, sm.buildSubscriptionsSnapshotResponse(req.Scope))
		return
	case "subscription_health_snapshot":
		sm.sendJSONSnapshot(req.ClientID, sm.buildSubscriptionHealthSnapshotResponse())
		return
	case "get_runtime_snapshot":
		sm.sendJSONSnapshot(req.ClientID, sm.buildRuntimeSnapshotResponse())
		return
	}
	exchangeNameForSubID := strings.Split(req.Exchange, "_")[0]
	if req.Action == "subscribe_all" && req.DataType == "trades" {
		wildcardID := exchangeNameForSubID + "-" + req.MarketType + "-all"
		if _, ok := sm.wildcardSubscribers[wildcardID]; !ok {
			sm.wildcardSubscribers[wildcardID] = make(map[string]bool)
		}
		sm.wildcardSubscribers[wildcardID][string(req.ClientID)] = true
		sm.wildcardRoutes[getClientRouteKey(string(req.ClientID), wildcardID)] = req.Exchange
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
		routeKey := getClientRouteKey(clientIDStr, subID)
		if req.DataType == "orderbooks" {
			sm.orderBookSubscriptionRoutes[routeKey] = req.Exchange
			if req.OrderBookDepth > 0 {
				sm.orderBookSubscriptionDepths[routeKey] = req.OrderBookDepth
			}
		} else {
			sm.tradeSubscriptionRoutes[routeKey] = req.Exchange
		}
	case "unsubscribe":
		if clients, ok := subMap[subID]; ok {
			delete(clients, clientIDStr)
			if len(clients) == 0 {
				delete(subMap, subID)
			}
		}
		routeKey := getClientRouteKey(clientIDStr, subID)
		if req.DataType == "orderbooks" {
			delete(sm.orderBookSubscriptionRoutes, routeKey)
			delete(sm.orderBookSubscriptionDepths, routeKey)
		} else {
			delete(sm.tradeSubscriptionRoutes, routeKey)
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

func runtimeSymbolAliases(exchange, symbol, marketType string) []string {
	if symbol == "" {
		return nil
	}

	aliases := []string{symbol}
	add := func(candidate string) {
		if candidate == "" {
			return
		}
		for _, existing := range aliases {
			if existing == candidate {
				return
			}
		}
		aliases = append(aliases, candidate)
	}

	switch exchange {
	case "binance":
		if strings.Contains(symbol, "/") {
			add(strings.ToUpper(binance.TranslateSymbolToExchange(symbol)))
		} else {
			add(binance.TranslateSymbolFromExchange(symbol, marketType))
		}
	case "bybit":
		if strings.Contains(symbol, "/") {
			add(strings.ToUpper(bybit.TranslateSymbolToExchange(symbol)))
		} else {
			add(bybit.TranslateSymbolFromExchange(symbol, marketType))
		}
	case "bitget":
		if strings.Contains(symbol, "/") {
			add(strings.ToUpper(bitget.TranslateSymbolToExchange(symbol)))
		} else {
			add(bitget.TranslateSymbolFromExchange(symbol, marketType))
		}
	}

	return aliases
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
	type unsubReq struct {
		exchange   string
		marketType string
		symbol     string
		dataType   string
	}
	toUnsub := make([]unsubReq, 0, 256)

	for subID, clients := range sm.tradeSubscriptions {
		delete(clients, clientID)
		routeKey := getClientRouteKey(clientID, subID)
		exactExchange := sm.tradeSubscriptionRoutes[routeKey]
		delete(sm.tradeSubscriptionRoutes, routeKey)
		if len(clients) == 0 {
			exchange, marketType, symbol, ok := parseSubscriptionID(subID)
			if ok {
				if exactExchange == "" {
					exactExchange = exchange
				}
				toUnsub = append(toUnsub, unsubReq{
					exchange:   exactExchange,
					marketType: marketType,
					symbol:     symbol,
					dataType:   "trades",
				})
			}
			delete(sm.tradeSubscriptions, subID)
		}
	}

	for subID, clients := range sm.orderBookSubscriptions {
		delete(clients, clientID)
		routeKey := getClientRouteKey(clientID, subID)
		exactExchange := sm.orderBookSubscriptionRoutes[routeKey]
		delete(sm.orderBookSubscriptionRoutes, routeKey)
		delete(sm.orderBookSubscriptionDepths, routeKey)
		if len(clients) == 0 {
			exchange, marketType, symbol, ok := parseSubscriptionID(subID)
			if ok {
				if exactExchange == "" {
					exactExchange = exchange
				}
				toUnsub = append(toUnsub, unsubReq{
					exchange:   exactExchange,
					marketType: marketType,
					symbol:     symbol,
					dataType:   "orderbooks",
				})
			}
			delete(sm.orderBookSubscriptions, subID)
		}
	}

	for wildcardID, clients := range sm.wildcardSubscribers {
		delete(clients, clientID)
		delete(sm.wildcardRoutes, getClientRouteKey(clientID, wildcardID))
		if len(clients) == 0 {
			delete(sm.wildcardSubscribers, wildcardID)
		}
	}

	for _, req := range toUnsub {
		if req.symbol == "" {
			continue
		}
		handler, ok := sm.exchangeRegistry[req.exchange]
		if !ok {
			handler = sm.exchangeRegistry["ccxt_generic"]
		}
		if handler == nil {
			continue
		}
		handler.HandleRequest(&shared_types.ClientRequest{
			Action:     "unsubscribe",
			Exchange:   req.exchange,
			Symbol:     req.symbol,
			MarketType: req.marketType,
			DataType:   req.dataType,
		})
	}
}

func parseSubscriptionID(subID string) (exchange, marketType, symbol string, ok bool) {
	parts := strings.SplitN(subID, "-", 3)
	if len(parts) != 3 {
		return "", "", "", false
	}
	return parts[0], parts[1], parts[2], true
}

func getClientRouteKey(clientID, subID string) string {
	return clientID + "|" + subID
}

func (sm *SubscriptionManager) buildSubscriptionsSnapshotResponse(scope string) *shared_types.SubscriptionsSnapshotResponse {
	if scope == "" {
		scope = "global"
	}
	return &shared_types.SubscriptionsSnapshotResponse{
		Type:  "subscriptions_snapshot",
		Scope: scope,
		TS:    time.Now().UnixMilli(),
		Items: sm.buildRuntimeSubscriptionItems(),
	}
}

func (sm *SubscriptionManager) buildSubscriptionHealthSnapshotResponse() *shared_types.SubscriptionHealthSnapshotResponse {
	items := sm.buildRuntimeSubscriptionItems()
	health, _ := sm.runtimeTracker.snapshotHealth(items, time.Now())
	return &shared_types.SubscriptionHealthSnapshotResponse{
		Type:  "subscription_health_snapshot",
		TS:    time.Now().UnixMilli(),
		Items: health,
	}
}

func (sm *SubscriptionManager) buildRuntimeSnapshotResponse() *shared_types.RuntimeSnapshotResponse {
	items := sm.buildRuntimeSubscriptionItems()
	health, totals := sm.runtimeTracker.snapshotHealth(items, time.Now())
	return &shared_types.RuntimeSnapshotResponse{
		Type:          "runtime_snapshot",
		TS:            time.Now().UnixMilli(),
		Subscriptions: items,
		Health:        health,
		Totals:        totals,
	}
}

func (sm *SubscriptionManager) buildRuntimeSubscriptionItems() []shared_types.RuntimeSubscriptionItem {
	items := make([]shared_types.RuntimeSubscriptionItem, 0, len(sm.tradeSubscriptions)+len(sm.orderBookSubscriptions))
	items = append(items, sm.aggregateRuntimeSubscriptions(sm.tradeSubscriptions, sm.tradeSubscriptionRoutes, nil, "trades")...)
	items = append(items, sm.aggregateRuntimeSubscriptions(sm.orderBookSubscriptions, sm.orderBookSubscriptionRoutes, sm.orderBookSubscriptionDepths, "orderbooks")...)

	sort.Slice(items, func(i, j int) bool {
		if items[i].Exchange != items[j].Exchange {
			return items[i].Exchange < items[j].Exchange
		}
		if items[i].MarketType != items[j].MarketType {
			return items[i].MarketType < items[j].MarketType
		}
		if items[i].Symbol != items[j].Symbol {
			return items[i].Symbol < items[j].Symbol
		}
		if items[i].DataType != items[j].DataType {
			return items[i].DataType < items[j].DataType
		}
		return items[i].Depth < items[j].Depth
	})
	return items
}

func (sm *SubscriptionManager) aggregateRuntimeSubscriptions(
	subscriptions map[string]map[string]bool,
	routeMap map[string]string,
	depthMap map[string]int,
	defaultDataType string,
) []shared_types.RuntimeSubscriptionItem {
	type aggregate struct {
		exchange   string
		marketType string
		symbol     string
		dataType   string
		adapter    string
		depth      int
		clients    map[string]bool
	}

	grouped := make(map[string]*aggregate)
	for subID, clients := range subscriptions {
		baseExchange, marketType, symbol, ok := parseSubscriptionID(subID)
		if !ok {
			continue
		}
		for clientID := range clients {
			routeKey := getClientRouteKey(clientID, subID)
			exactExchange := routeMap[routeKey]
			if exactExchange == "" {
				exactExchange = baseExchange
			}

			groupKey := strings.Join([]string{exactExchange, marketType, symbol, defaultDataType}, "|")
			entry, exists := grouped[groupKey]
			if !exists {
				entry = &aggregate{
					exchange:   exactExchange,
					marketType: marketType,
					symbol:     symbol,
					dataType:   defaultDataType,
					adapter:    adapterFromExchangeRoute(exactExchange),
					clients:    make(map[string]bool),
				}
				grouped[groupKey] = entry
			}
			entry.clients[clientID] = true
			if depthMap != nil {
				if depth := depthMap[routeKey]; depth > entry.depth {
					entry.depth = depth
				}
			}
		}
	}

	items := make([]shared_types.RuntimeSubscriptionItem, 0, len(grouped))
	for _, entry := range grouped {
		clientCount := len(entry.clients)
		items = append(items, shared_types.RuntimeSubscriptionItem{
			Exchange:   entry.exchange,
			MarketType: entry.marketType,
			Symbol:     entry.symbol,
			DataType:   entry.dataType,
			Adapter:    entry.adapter,
			Depth:      entry.depth,
			Running: sm.runtimeTracker.isRunning(runtimeKey{
				Exchange:   entry.exchange,
				MarketType: entry.marketType,
				Symbol:     entry.symbol,
				DataType:   entry.dataType,
			}),
			Owners:  clientCount,
			Clients: clientCount,
		})
	}
	return items
}

func (sm *SubscriptionManager) sendJSONSnapshot(clientID []byte, payload any) {
	if sm.DistributionCh == nil || len(clientID) == 0 || payload == nil {
		return
	}
	sm.DistributionCh <- &DistributionMessage{
		ClientIDs:  [][]byte{append([]byte(nil), clientID...)},
		RawPayload: payload,
	}
}

func adapterFromExchangeRoute(exchange string) string {
	if strings.HasSuffix(exchange, "_native") {
		return "native"
	}
	return "ccxt"
}
