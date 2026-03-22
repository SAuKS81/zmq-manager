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
	"bybit-watcher/internal/exchanges/bitmart"
	"bybit-watcher/internal/exchanges/bybit"
	"bybit-watcher/internal/exchanges/coinex"
	"bybit-watcher/internal/exchanges/htx"
	"bybit-watcher/internal/exchanges/kucoin"
	"bybit-watcher/internal/exchanges/mexc"
	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
)

type SubscriptionManager struct {
	RequestCh                      chan *shared_types.ClientRequest
	StatusCh                       chan *shared_types.StreamStatusEvent
	DistributionCh                 chan<- *DistributionMessage
	TradeDataCh                    chan *shared_types.TradeUpdate
	OrderBookCh                    chan *shared_types.OrderBookUpdate
	tradeSubscriptions             map[string]map[string]bool
	orderBookSubscriptions         map[string]map[string]bool
	tradeSubscriptionRoutes        map[string]string
	tradeSubscriptionCacheN        map[string]int
	orderBookSubscriptionRoutes    map[string]string
	orderBookSubscriptionDepths    map[string]int
	orderBookSubscriptionModes     map[string]string
	tradeSubscriptionEncodings     map[string]string
	orderBookSubscriptionEncodings map[string]string
	stickyTradeSubscriptions       map[string]bool
	stickyOrderBookSubscriptions   map[string]bool
	exchangeRegistry               map[string]exchanges.Exchange
	wildcardSubscribers            map[string]map[string]bool
	wildcardRoutes                 map[string]string
	incomingTradeCounter           atomic.Uint64
	incomingOBCounter              atomic.Uint64
	totalDataReceived              atomic.Uint64
	runtimeTracker                 *runtimeTracker
	pendingActivations             map[runtimeKey]pendingActivation
	requestContexts                map[runtimeKey]requestOperationContext
	deployBatches                  map[string]*deployBatchState
}

func NewSubscriptionManager(distributionCh chan<- *DistributionMessage) *SubscriptionManager {
	sm := &SubscriptionManager{
		RequestCh:                      make(chan *shared_types.ClientRequest, 100),
		StatusCh:                       make(chan *shared_types.StreamStatusEvent, 1024),
		DistributionCh:                 distributionCh,
		TradeDataCh:                    make(chan *shared_types.TradeUpdate, 10000),
		OrderBookCh:                    make(chan *shared_types.OrderBookUpdate, 5000),
		tradeSubscriptions:             make(map[string]map[string]bool),
		orderBookSubscriptions:         make(map[string]map[string]bool),
		tradeSubscriptionRoutes:        make(map[string]string),
		tradeSubscriptionCacheN:        make(map[string]int),
		orderBookSubscriptionRoutes:    make(map[string]string),
		orderBookSubscriptionDepths:    make(map[string]int),
		orderBookSubscriptionModes:     make(map[string]string),
		tradeSubscriptionEncodings:     make(map[string]string),
		orderBookSubscriptionEncodings: make(map[string]string),
		stickyTradeSubscriptions:       make(map[string]bool),
		stickyOrderBookSubscriptions:   make(map[string]bool),
		exchangeRegistry:               make(map[string]exchanges.Exchange),
		wildcardSubscribers:            make(map[string]map[string]bool),
		wildcardRoutes:                 make(map[string]string),
		runtimeTracker:                 newRuntimeTracker(),
		pendingActivations:             make(map[runtimeKey]pendingActivation),
		requestContexts:                make(map[runtimeKey]requestOperationContext),
		deployBatches:                  make(map[string]*deployBatchState),
	}
	sm.exchangeRegistry["bybit_native"] = bybit.NewBybitExchange(sm.RequestCh, sm.TradeDataCh, sm.OrderBookCh, sm.StatusCh)
	sm.exchangeRegistry["binance_native"] = binance.NewBinanceExchange(sm.RequestCh, sm.TradeDataCh, sm.OrderBookCh, sm.StatusCh)
	sm.exchangeRegistry["bitget_native"] = bitget.NewBitgetExchange(sm.RequestCh, sm.TradeDataCh, sm.StatusCh)
	sm.exchangeRegistry["bitmart_native"] = bitmart.NewBitmartExchange(sm.RequestCh, sm.TradeDataCh, sm.OrderBookCh, sm.StatusCh)
	sm.exchangeRegistry["mexc_native"] = mexc.NewMexcExchange(sm.RequestCh, sm.TradeDataCh, sm.OrderBookCh, sm.StatusCh)
	sm.exchangeRegistry["kucoin_native"] = kucoin.NewKucoinExchange(sm.RequestCh, sm.TradeDataCh, sm.OrderBookCh, sm.StatusCh)
	sm.exchangeRegistry["coinex_native"] = coinex.NewCoinexExchange(sm.RequestCh, sm.TradeDataCh, sm.OrderBookCh, sm.StatusCh)
	sm.exchangeRegistry["htx_native"] = htx.NewHtxExchange(sm.RequestCh, sm.TradeDataCh, sm.OrderBookCh, sm.StatusCh)
	sm.exchangeRegistry["huobi_native"] = sm.exchangeRegistry["htx_native"]
	registerCCXT(sm)
	return sm
}

func (sm *SubscriptionManager) Run() {
	log.Println("[SUB-MANAGER] Startet (Low-Latency Mode)...")
	go sm.logIncomingRate()
	runtimeTick := time.NewTicker(runtimeTotalsTickInterval)
	defer runtimeTick.Stop()

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
		case <-runtimeTick.C:
			sm.broadcastRuntimeTotalsTick()

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
	sm.enrichStatusEvent(event)
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
					routeKey := getClientRouteKey(clientIDStr, subID)
					exactExchange := sm.tradeSubscriptionRoutes[routeKey]
					if exactExchange == "" {
						exactExchange = trade.Exchange
					}
					sm.emitPendingActivation(runtimeKey{
						Exchange:   exactExchange,
						MarketType: trade.MarketType,
						Symbol:     alias,
						DataType:   "trades",
					})
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
					routeKey := getClientRouteKey(clientIDStr, subID)
					exactExchange := sm.orderBookSubscriptionRoutes[routeKey]
					if exactExchange == "" {
						exactExchange = ob.Exchange
					}
					sm.emitPendingActivation(runtimeKey{
						Exchange:   exactExchange,
						MarketType: ob.MarketType,
						Symbol:     alias,
						DataType:   "orderbooks",
					})
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
	if sm.runtimeTracker == nil {
		sm.runtimeTracker = newRuntimeTracker()
	}
	if sm.tradeSubscriptionRoutes == nil {
		sm.tradeSubscriptionRoutes = make(map[string]string)
	}
	if sm.tradeSubscriptionCacheN == nil {
		sm.tradeSubscriptionCacheN = make(map[string]int)
	}
	if sm.orderBookSubscriptionRoutes == nil {
		sm.orderBookSubscriptionRoutes = make(map[string]string)
	}
	if sm.tradeSubscriptionEncodings == nil {
		sm.tradeSubscriptionEncodings = make(map[string]string)
	}
	if sm.orderBookSubscriptionEncodings == nil {
		sm.orderBookSubscriptionEncodings = make(map[string]string)
	}
	if sm.orderBookSubscriptionDepths == nil {
		sm.orderBookSubscriptionDepths = make(map[string]int)
	}
	if sm.orderBookSubscriptionModes == nil {
		sm.orderBookSubscriptionModes = make(map[string]string)
	}
	if sm.stickyTradeSubscriptions == nil {
		sm.stickyTradeSubscriptions = make(map[string]bool)
	}
	if sm.stickyOrderBookSubscriptions == nil {
		sm.stickyOrderBookSubscriptions = make(map[string]bool)
	}
	if sm.wildcardRoutes == nil {
		sm.wildcardRoutes = make(map[string]string)
	}
	switch req.Action {
	case "list_subscriptions":
		sm.sendJSONSnapshot(req.ClientID, sm.buildSubscriptionsSnapshotResponse(req.Scope, req.RequestID))
		return
	case "subscription_health_snapshot":
		sm.sendJSONSnapshot(req.ClientID, sm.buildSubscriptionHealthSnapshotResponse(req.RequestID))
		return
	case "get_runtime_snapshot":
		sm.sendJSONSnapshot(req.ClientID, sm.buildRuntimeSnapshotResponse(req.RequestID))
		return
	case "get_capabilities":
		sm.sendJSONSnapshot(req.ClientID, buildCapabilitiesSnapshotResponse(req.RequestID))
		return
	case "deploy_batch_register":
		sm.registerDeployBatch(req)
		return
	}
	exchangeNameForSubID := canonicalSubscriptionExchange(req.Exchange)
	req.Symbol = canonicalSubscriptionSymbol(exchangeNameForSubID, req.Symbol, req.MarketType)
	if req.Action == "subscribe_all" && req.DataType == "trades" {
		wildcardID := exchangeNameForSubID + "-" + req.MarketType + "-all"
		if _, ok := sm.wildcardSubscribers[wildcardID]; !ok {
			sm.wildcardSubscribers[wildcardID] = make(map[string]bool)
		}
		sm.wildcardSubscribers[wildcardID][string(req.ClientID)] = true
		sm.wildcardRoutes[getClientRouteKey(string(req.ClientID), wildcardID)] = req.Exchange
	}

	subID := getSubscriptionID(exchangeNameForSubID, req.Symbol, req.MarketType)
	clientIDStr := string(req.ClientID)

	var subMap map[string]map[string]bool
	var encMap map[string]string
	if req.DataType == "orderbooks" {
		subMap = sm.orderBookSubscriptions
		encMap = sm.orderBookSubscriptionEncodings
	} else {
		subMap = sm.tradeSubscriptions
		encMap = sm.tradeSubscriptionEncodings
	}

	eventType := "stream_subscribe_acked"
	failedEventType := "stream_subscribe_failed"
	wasActive := false
	routeKey := getClientRouteKey(clientIDStr, subID)
	opKey := runtimeKey{
		Exchange:   req.Exchange,
		MarketType: req.MarketType,
		Symbol:     req.Symbol,
		DataType:   req.DataType,
	}
	if validationReason := sm.validateRequestSpec(req); validationReason != "" {
		if req.Action == "subscribe" && req.DataType == "orderbooks" {
			failedEventType = "stream_update_failed"
		}
		sm.sendStatusToClient(req.ClientID, &shared_types.StreamStatusEvent{
			Type:       failedEventType,
			Exchange:   req.Exchange,
			MarketType: req.MarketType,
			Symbol:     req.Symbol,
			DataType:   req.DataType,
			Adapter:    adapterFromExchangeRoute(req.Exchange),
			RequestID:  req.RequestID,
			Status:     "failed",
			Reason:     validationReason,
			Timestamp:  time.Now().UnixMilli(),
		})
		sm.recordDeployBatchResult(req.RequestID, true)
		return
	}
	if (req.Action == "subscribe" || req.Action == "unsubscribe") && req.Symbol != "" && !isUnifiedSymbol(req.Symbol, req.MarketType) {
		if req.Action == "subscribe" && req.DataType == "orderbooks" {
			failedEventType = "stream_update_failed"
		}
		sm.sendStatusToClient(req.ClientID, &shared_types.StreamStatusEvent{
			Type:       failedEventType,
			Exchange:   req.Exchange,
			MarketType: req.MarketType,
			Symbol:     req.Symbol,
			DataType:   req.DataType,
			Adapter:    adapterFromExchangeRoute(req.Exchange),
			RequestID:  req.RequestID,
			Status:     "failed",
			Reason:     "invalid_symbol_format",
			Message:    unifiedSymbolFormatHint(req.MarketType),
			Timestamp:  time.Now().UnixMilli(),
		})
		sm.recordDeployBatchResult(req.RequestID, true)
		return
	}

	prevOwnerCount := 0
	prevEffectiveDepth := 0
	prevEffectiveMode := ""
	prevEffectiveCacheN := 0
	existingClientSubscribed := false
	if clients, ok := subMap[subID]; ok {
		existingClientSubscribed = clients[clientIDStr]
	}
	existingEncoding := encMap[routeKey]
	existingSticky := false
	existingExactExchange := ""
	existingDepth := 0
	existingMode := ""
	existingCacheN := 0
	if req.DataType == "orderbooks" {
		prevOwnerCount = sm.orderBookOwnerCount(subID, req.Exchange)
		prevEffectiveDepth = sm.effectiveOrderBookDepth(subID, req.Exchange)
		prevEffectiveMode = sm.effectiveOrderBookMode(subID, req.Exchange)
		existingSticky = sm.stickyOrderBookSubscriptions[routeKey]
		existingExactExchange = sm.orderBookSubscriptionRoutes[routeKey]
		existingDepth = sm.orderBookSubscriptionDepths[routeKey]
		existingMode = sm.orderBookSubscriptionModes[routeKey]
	} else {
		prevOwnerCount = sm.tradeOwnerCount(subID, req.Exchange)
		prevEffectiveCacheN = sm.effectiveTradeCacheN(subID, req.Exchange)
		existingSticky = sm.stickyTradeSubscriptions[routeKey]
		existingExactExchange = sm.tradeSubscriptionRoutes[routeKey]
		existingCacheN = sm.tradeSubscriptionCacheN[routeKey]
	}

	if req.Action == "subscribe" && existingClientSubscribed {
		sameRoute := existingExactExchange == "" || existingExactExchange == req.Exchange
		sameEncoding := existingEncoding == req.Encoding
		sameSticky := existingSticky == req.Sticky
		sameParams := false
		if req.DataType == "orderbooks" {
			sameParams = existingDepth == req.OrderBookDepth && existingMode == req.OrderBookMode
		} else {
			sameParams = existingCacheN == req.CacheN
		}
		if sameRoute && sameEncoding && sameSticky && sameParams {
			sm.recordDeployBatchResult(req.RequestID, false)
			return
		}
	}

	var forwardReq *shared_types.ClientRequest

	switch req.Action {
	case "subscribe":
		if req.DataType == "orderbooks" {
			prevMode := sm.orderBookSubscriptionModes[routeKey]
			depthChanged := false
			if prevDepth, ok := sm.orderBookSubscriptionDepths[routeKey]; ok && prevDepth > 0 && req.OrderBookDepth > 0 && prevDepth != req.OrderBookDepth {
				depthChanged = true
			}
			if depthChanged || (prevMode != "" && prevMode != req.OrderBookMode) {
				eventType = "stream_update_acked"
			}
		}
		if _, ok := subMap[subID]; !ok {
			subMap[subID] = make(map[string]bool)
		}
		subMap[subID][clientIDStr] = true
		if req.Encoding != "" {
			encMap[routeKey] = req.Encoding
		}
		if req.DataType == "orderbooks" {
			if req.Sticky {
				sm.stickyOrderBookSubscriptions[routeKey] = true
			} else {
				delete(sm.stickyOrderBookSubscriptions, routeKey)
			}
		} else {
			if req.Sticky {
				sm.stickyTradeSubscriptions[routeKey] = true
			} else {
				delete(sm.stickyTradeSubscriptions, routeKey)
			}
		}
		if req.DataType == "orderbooks" {
			sm.orderBookSubscriptionRoutes[routeKey] = req.Exchange
			if req.OrderBookDepth > 0 {
				sm.orderBookSubscriptionDepths[routeKey] = req.OrderBookDepth
			}
			if req.OrderBookMode != "" {
				sm.orderBookSubscriptionModes[routeKey] = req.OrderBookMode
			} else {
				delete(sm.orderBookSubscriptionModes, routeKey)
			}
		} else {
			sm.tradeSubscriptionRoutes[routeKey] = req.Exchange
			if req.CacheN > 0 {
				sm.tradeSubscriptionCacheN[routeKey] = req.CacheN
			} else {
				delete(sm.tradeSubscriptionCacheN, routeKey)
			}
		}
		wasActive = sm.runtimeTracker.isActive(opKey)
		if req.DataType == "orderbooks" {
			newEffectiveDepth := sm.effectiveOrderBookDepth(subID, req.Exchange)
			newEffectiveMode := sm.effectiveOrderBookMode(subID, req.Exchange)
			if prevOwnerCount == 0 || newEffectiveDepth != prevEffectiveDepth || newEffectiveMode != prevEffectiveMode {
				forwardReq = cloneClientRequest(req)
				forwardReq.OrderBookDepth = newEffectiveDepth
				forwardReq.OrderBookMode = newEffectiveMode
			}
		} else {
			newEffectiveCacheN := sm.effectiveTradeCacheN(subID, req.Exchange)
			if prevOwnerCount == 0 || newEffectiveCacheN != prevEffectiveCacheN {
				forwardReq = cloneClientRequest(req)
				forwardReq.CacheN = newEffectiveCacheN
			}
		}
		if forwardReq != nil {
			sm.noteRequestContext(opKey, forwardReq, "subscribe")
		}
		sm.sendStatusToClient(req.ClientID, &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   req.Exchange,
			MarketType: req.MarketType,
			Symbol:     req.Symbol,
			DataType:   req.DataType,
			Adapter:    adapterFromExchangeRoute(req.Exchange),
			RequestID:  req.RequestID,
			Status:     "acked",
			Timestamp:  time.Now().UnixMilli(),
		})
		if wasActive {
			sm.sendStatusToClient(req.ClientID, &shared_types.StreamStatusEvent{
				Type:       "stream_subscribe_active",
				Exchange:   req.Exchange,
				MarketType: req.MarketType,
				Symbol:     req.Symbol,
				DataType:   req.DataType,
				Adapter:    adapterFromExchangeRoute(req.Exchange),
				RequestID:  req.RequestID,
				Status:     runtimeStatusRunning,
				Timestamp:  time.Now().UnixMilli(),
			})
		} else {
			sm.queuePendingActivation(opKey, req, "stream_subscribe_active")
		}
	case "unsubscribe":
		prevExactExchange := req.Exchange
		if req.DataType == "orderbooks" {
			if exact := sm.orderBookSubscriptionRoutes[routeKey]; exact != "" {
				prevExactExchange = exact
			}
		} else {
			if exact := sm.tradeSubscriptionRoutes[routeKey]; exact != "" {
				prevExactExchange = exact
			}
		}
		sm.sendStatusToClient(req.ClientID, &shared_types.StreamStatusEvent{
			Type:       "stream_stop_requested",
			Exchange:   req.Exchange,
			MarketType: req.MarketType,
			Symbol:     req.Symbol,
			DataType:   req.DataType,
			Adapter:    adapterFromExchangeRoute(req.Exchange),
			RequestID:  req.RequestID,
			Status:     runtimeStatusStopped,
			Timestamp:  time.Now().UnixMilli(),
		})
		if clients, ok := subMap[subID]; ok {
			delete(clients, clientIDStr)
			if len(clients) == 0 {
				delete(subMap, subID)
			}
		}
		if req.DataType == "orderbooks" {
			delete(sm.orderBookSubscriptionRoutes, routeKey)
			delete(sm.orderBookSubscriptionDepths, routeKey)
			delete(sm.orderBookSubscriptionModes, routeKey)
			delete(sm.stickyOrderBookSubscriptions, routeKey)
		} else {
			delete(sm.tradeSubscriptionRoutes, routeKey)
			delete(sm.tradeSubscriptionCacheN, routeKey)
			delete(sm.stickyTradeSubscriptions, routeKey)
		}
		delete(encMap, routeKey)
		currentOwnerCount := 0
		if req.DataType == "orderbooks" {
			currentOwnerCount = sm.orderBookOwnerCount(subID, prevExactExchange)
		} else {
			currentOwnerCount = sm.tradeOwnerCount(subID, prevExactExchange)
		}
		if currentOwnerCount == 0 {
			forwardReq = cloneClientRequest(req)
			forwardReq.Exchange = prevExactExchange
		} else if req.DataType == "orderbooks" {
			newEffectiveDepth := sm.effectiveOrderBookDepth(subID, prevExactExchange)
			newEffectiveMode := sm.effectiveOrderBookMode(subID, prevExactExchange)
			if newEffectiveDepth != prevEffectiveDepth || newEffectiveMode != prevEffectiveMode {
				forwardReq = cloneClientRequest(req)
				forwardReq.Action = "subscribe"
				forwardReq.Exchange = prevExactExchange
				forwardReq.OrderBookDepth = newEffectiveDepth
				forwardReq.OrderBookMode = newEffectiveMode
			}
		} else {
			newEffectiveCacheN := sm.effectiveTradeCacheN(subID, prevExactExchange)
			if newEffectiveCacheN != prevEffectiveCacheN {
				forwardReq = cloneClientRequest(req)
				forwardReq.Action = "subscribe"
				forwardReq.Exchange = prevExactExchange
				forwardReq.CacheN = newEffectiveCacheN
			}
		}
		sm.sendStatusToClient(req.ClientID, &shared_types.StreamStatusEvent{
			Type:       "stream_unsubscribe_acked",
			Exchange:   req.Exchange,
			MarketType: req.MarketType,
			Symbol:     req.Symbol,
			DataType:   req.DataType,
			Adapter:    adapterFromExchangeRoute(req.Exchange),
			RequestID:  req.RequestID,
			Status:     "acked",
			Timestamp:  time.Now().UnixMilli(),
		})
	case "disconnect":
		sm.cleanupClientSubscriptions(clientIDStr)
		return
	}

	if forwardReq == nil {
		if req.Action == "subscribe" || req.Action == "unsubscribe" {
			sm.recordDeployBatchResult(req.RequestID, false)
		}
		return
	}

	var handler exchanges.Exchange
	specificHandler, ok := sm.exchangeRegistry[forwardReq.Exchange]
	if ok {
		handler = specificHandler
	} else {
		if sm.shouldLogCCXTFallback(forwardReq) {
			log.Printf(
				"[SUB-MANAGER] Kein exakter Handler fuer exchange=%s market_type=%s data_type=%s, fallback=ccxt_generic",
				forwardReq.Exchange,
				forwardReq.MarketType,
				forwardReq.DataType,
			)
		}
		handler = sm.exchangeRegistry["ccxt_generic"]
	}

	if handler != nil {
		handler.HandleRequest(forwardReq)
		if req.Action == "subscribe" || req.Action == "unsubscribe" {
			sm.recordDeployBatchResult(req.RequestID, false)
		}
	} else {
		log.Printf("[SUB-MANAGER] ERROR: Kein passender Handler fuer die Anfrage gefunden: exchange=%s market_type=%s data_type=%s", forwardReq.Exchange, forwardReq.MarketType, forwardReq.DataType)
		sm.sendStatusToClient(req.ClientID, &shared_types.StreamStatusEvent{
			Type:       failedEventType,
			Exchange:   req.Exchange,
			MarketType: req.MarketType,
			Symbol:     req.Symbol,
			DataType:   req.DataType,
			Adapter:    adapterFromExchangeRoute(req.Exchange),
			RequestID:  req.RequestID,
			Status:     runtimeStatusFailed,
			Reason:     "no_handler",
			Timestamp:  time.Now().UnixMilli(),
		})
		sm.recordDeployBatchResult(req.RequestID, true)
	}
}

func (sm *SubscriptionManager) shouldLogCCXTFallback(req *shared_types.ClientRequest) bool {
	if req == nil || req.RequestID == "" || sm.deployBatches == nil {
		return true
	}
	state := sm.deployBatches[req.RequestID]
	if state == nil {
		return true
	}
	if state.FallbackLogWritten {
		return false
	}
	state.FallbackLogWritten = true
	return true
}

func getSubscriptionID(exchange, symbol, marketType string) string {
	return exchange + "-" + marketType + "-" + symbol
}

func canonicalSubscriptionExchange(exchange string) string {
	base := strings.Split(strings.ToLower(strings.TrimSpace(exchange)), "_")[0]
	return canonicalCapabilityExchange(base)
}

func canonicalSubscriptionSymbol(exchange, symbol, marketType string) string {
	exchange = canonicalSubscriptionExchange(exchange)
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return ""
	}

	switch exchange {
	case "binance":
		if strings.Contains(symbol, "/") {
			return binance.TranslateSymbolFromExchange(strings.ToUpper(binance.TranslateSymbolToExchange(symbol)), marketType)
		}
		return binance.TranslateSymbolFromExchange(symbol, marketType)
	case "bybit":
		if strings.Contains(symbol, "/") {
			return bybit.TranslateSymbolFromExchange(strings.ToUpper(bybit.TranslateSymbolToExchange(symbol)), marketType)
		}
		return bybit.TranslateSymbolFromExchange(symbol, marketType)
	case "bitget":
		if strings.Contains(symbol, "/") {
			return bitget.TranslateSymbolFromExchange(strings.ToUpper(bitget.TranslateSymbolToExchange(symbol)), marketType)
		}
		return bitget.TranslateSymbolFromExchange(symbol, marketType)
	case "mexc":
		if strings.Contains(symbol, "/") {
			return mexc.TranslateSymbolFromExchange(strings.ToUpper(mexc.TranslateSymbolToExchange(symbol)))
		}
		return mexc.TranslateSymbolFromExchange(symbol)
	case "kucoin":
		if strings.Contains(symbol, "/") {
			return kucoin.TranslateSymbolFromExchange(strings.ToUpper(kucoin.TranslateSymbolToExchange(symbol)))
		}
		return kucoin.TranslateSymbolFromExchange(symbol)
	case "htx":
		if strings.Contains(symbol, "/") {
			return htx.TranslateSymbolFromExchange(htx.TranslateSymbolToExchange(symbol, marketType), marketType)
		}
		return htx.TranslateSymbolFromExchange(symbol, marketType)
	default:
		return symbol
	}
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
	case "mexc":
		if strings.Contains(symbol, "/") {
			add(strings.ToUpper(mexc.TranslateSymbolToExchange(symbol)))
		} else {
			add(mexc.TranslateSymbolFromExchange(symbol))
		}
	case "kucoin":
		if strings.Contains(symbol, "/") {
			add(strings.ToUpper(kucoin.TranslateSymbolToExchange(symbol)))
		} else {
			add(kucoin.TranslateSymbolFromExchange(symbol))
		}
	case "htx":
		if strings.Contains(symbol, "/") {
			add(htx.TranslateSymbolToExchange(symbol, marketType))
		} else {
			add(htx.TranslateSymbolFromExchange(symbol, marketType))
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
		routeKey := getClientRouteKey(clientID, subID)
		if sm.stickyTradeSubscriptions[routeKey] {
			continue
		}
		delete(clients, clientID)
		exactExchange := sm.tradeSubscriptionRoutes[routeKey]
		delete(sm.tradeSubscriptionRoutes, routeKey)
		delete(sm.tradeSubscriptionCacheN, routeKey)
		delete(sm.tradeSubscriptionEncodings, routeKey)
		delete(sm.stickyTradeSubscriptions, routeKey)
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
		routeKey := getClientRouteKey(clientID, subID)
		if sm.stickyOrderBookSubscriptions[routeKey] {
			continue
		}
		delete(clients, clientID)
		exactExchange := sm.orderBookSubscriptionRoutes[routeKey]
		delete(sm.orderBookSubscriptionRoutes, routeKey)
		delete(sm.orderBookSubscriptionDepths, routeKey)
		delete(sm.orderBookSubscriptionModes, routeKey)
		delete(sm.orderBookSubscriptionEncodings, routeKey)
		delete(sm.stickyOrderBookSubscriptions, routeKey)
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

func cloneClientRequest(req *shared_types.ClientRequest) *shared_types.ClientRequest {
	if req == nil {
		return nil
	}
	cloned := *req
	if req.ClientID != nil {
		cloned.ClientID = append([]byte(nil), req.ClientID...)
	}
	return &cloned
}

func effectiveRouteForClient(subID, routeKey string, routeMap map[string]string) string {
	if exactExchange := routeMap[routeKey]; exactExchange != "" {
		return exactExchange
	}
	baseExchange, _, _, ok := parseSubscriptionID(subID)
	if !ok {
		return ""
	}
	return baseExchange
}

func (sm *SubscriptionManager) tradeOwnerCount(subID, exactExchange string) int {
	clients := sm.tradeSubscriptions[subID]
	count := 0
	for clientID := range clients {
		routeKey := getClientRouteKey(clientID, subID)
		if effectiveRouteForClient(subID, routeKey, sm.tradeSubscriptionRoutes) == exactExchange {
			count++
		}
	}
	return count
}

func (sm *SubscriptionManager) orderBookOwnerCount(subID, exactExchange string) int {
	clients := sm.orderBookSubscriptions[subID]
	count := 0
	for clientID := range clients {
		routeKey := getClientRouteKey(clientID, subID)
		if effectiveRouteForClient(subID, routeKey, sm.orderBookSubscriptionRoutes) == exactExchange {
			count++
		}
	}
	return count
}

func (sm *SubscriptionManager) effectiveTradeCacheN(subID, exactExchange string) int {
	clients := sm.tradeSubscriptions[subID]
	maxCacheN := 0
	for clientID := range clients {
		routeKey := getClientRouteKey(clientID, subID)
		if effectiveRouteForClient(subID, routeKey, sm.tradeSubscriptionRoutes) != exactExchange {
			continue
		}
		if cacheN := sm.tradeSubscriptionCacheN[routeKey]; cacheN > maxCacheN {
			maxCacheN = cacheN
		}
	}
	return maxCacheN
}

func (sm *SubscriptionManager) effectiveOrderBookDepth(subID, exactExchange string) int {
	clients := sm.orderBookSubscriptions[subID]
	maxDepth := 0
	for clientID := range clients {
		routeKey := getClientRouteKey(clientID, subID)
		if effectiveRouteForClient(subID, routeKey, sm.orderBookSubscriptionRoutes) != exactExchange {
			continue
		}
		if depth := sm.orderBookSubscriptionDepths[routeKey]; depth > maxDepth {
			maxDepth = depth
		}
	}
	return maxDepth
}

func (sm *SubscriptionManager) effectiveOrderBookMode(subID, exactExchange string) string {
	clients := sm.orderBookSubscriptions[subID]
	mode := ""
	for clientID := range clients {
		routeKey := getClientRouteKey(clientID, subID)
		if effectiveRouteForClient(subID, routeKey, sm.orderBookSubscriptionRoutes) != exactExchange {
			continue
		}
		currentMode := bitmart.NormalizeOrderBookMode(sm.orderBookSubscriptionModes[routeKey])
		if currentMode == "all" {
			return "all"
		}
		if currentMode != "" {
			mode = currentMode
		}
	}
	if mode == "" {
		return "level100"
	}
	return mode
}

func (sm *SubscriptionManager) buildSubscriptionsSnapshotResponse(scope, requestID string) *shared_types.SubscriptionsSnapshotResponse {
	if scope == "" {
		scope = "global"
	}
	return &shared_types.SubscriptionsSnapshotResponse{
		Type:      "subscriptions_snapshot",
		Scope:     scope,
		RequestID: requestID,
		TS:        time.Now().UnixMilli(),
		Items:     sm.buildRuntimeSubscriptionItems(),
	}
}

func (sm *SubscriptionManager) buildSubscriptionHealthSnapshotResponse(requestID string) *shared_types.SubscriptionHealthSnapshotResponse {
	if sm.runtimeTracker == nil {
		sm.runtimeTracker = newRuntimeTracker()
	}
	items := sm.buildRuntimeSubscriptionItems()
	health, _ := sm.runtimeTracker.snapshotHealth(items, time.Now())
	return &shared_types.SubscriptionHealthSnapshotResponse{
		Type:      "subscription_health_snapshot",
		RequestID: requestID,
		TS:        time.Now().UnixMilli(),
		Items:     health,
	}
}

func (sm *SubscriptionManager) buildRuntimeSnapshotResponse(requestID string) *shared_types.RuntimeSnapshotResponse {
	if sm.runtimeTracker == nil {
		sm.runtimeTracker = newRuntimeTracker()
	}
	items := sm.buildRuntimeSubscriptionItems()
	health, totals := sm.runtimeTracker.snapshotHealth(items, time.Now())
	return &shared_types.RuntimeSnapshotResponse{
		Type:          "runtime_snapshot",
		RequestID:     requestID,
		TS:            time.Now().UnixMilli(),
		Subscriptions: items,
		Health:        health,
		Totals:        totals,
	}
}

func (sm *SubscriptionManager) buildRuntimeSubscriptionItems() []shared_types.RuntimeSubscriptionItem {
	items := make([]shared_types.RuntimeSubscriptionItem, 0, len(sm.tradeSubscriptions)+len(sm.orderBookSubscriptions))
	items = append(items, sm.aggregateRuntimeSubscriptions(sm.tradeSubscriptions, sm.tradeSubscriptionRoutes, sm.tradeSubscriptionEncodings, sm.tradeSubscriptionCacheN, nil, "trades")...)
	items = append(items, sm.aggregateRuntimeSubscriptions(sm.orderBookSubscriptions, sm.orderBookSubscriptionRoutes, sm.orderBookSubscriptionEncodings, nil, sm.orderBookSubscriptionDepths, "orderbooks")...)

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
	encodingMap map[string]string,
	cacheMap map[string]int,
	depthMap map[string]int,
	defaultDataType string,
) []shared_types.RuntimeSubscriptionItem {
	type aggregate struct {
		exchange   string
		marketType string
		symbol     string
		dataType   string
		adapter    string
		cacheN     int
		depth      int
		sticky     bool
		clients    map[string]bool
		encodings  map[string]bool
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
					encodings:  make(map[string]bool),
				}
				grouped[groupKey] = entry
			}
			entry.clients[clientID] = true
			if encodingMap != nil {
				if encoding := encodingMap[routeKey]; encoding != "" {
					entry.encodings[encoding] = true
				}
			}
			if defaultDataType == "trades" {
				entry.sticky = entry.sticky || sm.stickyTradeSubscriptions[routeKey]
			} else if defaultDataType == "orderbooks" {
				entry.sticky = entry.sticky || sm.stickyOrderBookSubscriptions[routeKey]
			}
			if cacheMap != nil {
				if cacheN := cacheMap[routeKey]; cacheN > entry.cacheN {
					entry.cacheN = cacheN
				}
			}
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
			Encoding:   aggregateEncoding(entry.encodings),
			CacheN:     entry.cacheN,
			Depth:      entry.depth,
			Sticky:     entry.sticky,
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

func aggregateEncoding(encodings map[string]bool) string {
	if len(encodings) == 0 {
		return ""
	}
	if len(encodings) == 1 {
		for encoding := range encodings {
			return encoding
		}
	}
	return "mixed"
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

func (sm *SubscriptionManager) broadcastRuntimeTotalsTick() {
	if sm.DistributionCh == nil {
		return
	}
	items := sm.buildRuntimeSubscriptionItems()
	_, totals := sm.runtimeTracker.snapshotHealth(items, time.Now())
	sm.DistributionCh <- &DistributionMessage{
		Broadcast: true,
		RawPayload: &shared_types.RuntimeTotalsTickEvent{
			Type:                "runtime_totals_tick",
			TS:                  time.Now().UnixMilli(),
			ActiveSubscriptions: totals.ActiveSubscriptions,
			MessagesPerSec:      totals.MessagesPerSec,
			Reconnects24H:       totals.Reconnects24H,
		},
	}
}

func (sm *SubscriptionManager) validateRequestSpec(req *shared_types.ClientRequest) string {
	if req == nil || req.Action != "subscribe" || req.DataType != "orderbooks" {
		return ""
	}
	if canonicalSubscriptionExchange(req.Exchange) == "bitmart" {
		if !bitmart.IsValidOrderBookMode(req.OrderBookMode) {
			return "unsupported orderbook_mode"
		}
		req.OrderBookMode = bitmart.NormalizeOrderBookMode(req.OrderBookMode)
		req.OrderBookDepth = bitmart.NormalizeOrderBookDepth(req.OrderBookDepth)
		return ""
	}
	capability, ok := capabilityForExchange(req.Exchange)
	if !ok && !strings.HasSuffix(req.Exchange, "_native") {
		capability, ok = capabilityForExchange("ccxt_default")
	}
	if !ok {
		return ""
	}
	if len(capability.OrderBookDepths) == 0 {
		return "orderbooks not supported"
	}
	if req.OrderBookDepth <= 0 {
		return ""
	}
	for _, depth := range capability.OrderBookDepths {
		if req.OrderBookDepth <= depth {
			req.OrderBookDepth = depth
			return ""
		}
	}
	return "unsupported depth"
}

func (sm *SubscriptionManager) enrichStatusEvent(event *shared_types.StreamStatusEvent) {
	if event == nil {
		return
	}
	if event.Adapter == "" {
		event.Adapter = adapterFromExchangeRoute(event.Exchange)
	}
	if event.Status == "" {
		switch event.Type {
		case "stream_reconnecting":
			event.Status = runtimeStatusReconnecting
		case "stream_restored", "stream_subscribe_active":
			event.Status = runtimeStatusRunning
		case "stream_force_closed":
			event.Status = runtimeStatusStopped
		case "stream_unsubscribe_failed", "stream_subscribe_failed", "stream_update_failed":
			event.Status = runtimeStatusFailed
		}
	}
	if event.RequestID != "" {
		return
	}

	seen := make(map[string]bool)
	candidateSymbols := make([]string, 0, 1+len(event.Symbols))
	if event.Symbol != "" {
		candidateSymbols = append(candidateSymbols, event.Symbol)
	}
	candidateSymbols = append(candidateSymbols, event.Symbols...)

	var matchedRequestID string
	for _, symbol := range candidateSymbols {
		for _, alias := range runtimeSymbolAliases(event.Exchange, symbol, event.MarketType) {
			key := runtimeKey{
				Exchange:   event.Exchange,
				MarketType: event.MarketType,
				Symbol:     alias,
				DataType:   event.DataType,
			}
			seenKey := key.Exchange + "|" + key.MarketType + "|" + key.Symbol + "|" + key.DataType
			if seen[seenKey] {
				continue
			}
			seen[seenKey] = true
			ctx := sm.peekRequestContext(key)
			if ctx.RequestID == "" {
				continue
			}
			if matchedRequestID == "" {
				matchedRequestID = ctx.RequestID
				event.RequestID = ctx.RequestID
			} else if matchedRequestID != ctx.RequestID {
				return
			}
		}
	}
}

func adapterFromExchangeRoute(exchange string) string {
	if strings.HasSuffix(exchange, "_native") {
		return "native"
	}
	return "ccxt"
}
