package broker

import (
	"testing"
	"time"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

type testExchange struct{}

func (t *testExchange) HandleRequest(req *shared_types.ClientRequest) {}
func (t *testExchange) Stop()                                         {}

var _ exchanges.Exchange = (*testExchange)(nil)

type recordingExchange struct {
	reqs []*shared_types.ClientRequest
}

func (r *recordingExchange) HandleRequest(req *shared_types.ClientRequest) {
	r.reqs = append(r.reqs, req)
}
func (r *recordingExchange) Stop() {}

func TestDisconnectCleansAllSubscriptionMaps(t *testing.T) {
	clientID := []byte("client-a")
	otherID := "client-b"

	sm := &SubscriptionManager{
		tradeSubscriptions: map[string]map[string]bool{
			"binance-spot-BTC/USDT": {otherID: true},
		},
		orderBookSubscriptions: map[string]map[string]bool{
			"binance-spot-ETH/USDT": {otherID: true},
		},
		wildcardSubscribers: map[string]map[string]bool{
			"binance-spot-all": {otherID: true},
		},
		exchangeRegistry: map[string]exchanges.Exchange{
			"binance_native": &testExchange{},
			"ccxt_generic":   &testExchange{},
		},
	}

	sm.handleRequest(&shared_types.ClientRequest{
		ClientID:   clientID,
		Action:     "subscribe",
		Exchange:   "binance_native",
		Symbol:     "BTC/USDT",
		MarketType: "spot",
		DataType:   "trades",
	})

	sm.handleRequest(&shared_types.ClientRequest{
		ClientID:   clientID,
		Action:     "subscribe",
		Exchange:   "binance_native",
		Symbol:     "ETH/USDT",
		MarketType: "spot",
		DataType:   "orderbooks",
	})

	sm.handleRequest(&shared_types.ClientRequest{
		ClientID:   clientID,
		Action:     "subscribe_all",
		Exchange:   "binance_native",
		MarketType: "spot",
		DataType:   "trades",
	})

	sm.handleRequest(&shared_types.ClientRequest{
		ClientID: clientID,
		Action:   "disconnect",
	})

	if sm.tradeSubscriptions["binance-spot-BTC/USDT"][string(clientID)] {
		t.Fatalf("client still present in tradeSubscriptions")
	}
	if sm.orderBookSubscriptions["binance-spot-ETH/USDT"][string(clientID)] {
		t.Fatalf("client still present in orderBookSubscriptions")
	}
	if sm.wildcardSubscribers["binance-spot-all"][string(clientID)] {
		t.Fatalf("client still present in wildcardSubscribers")
	}

	if !sm.tradeSubscriptions["binance-spot-BTC/USDT"][otherID] {
		t.Fatalf("other client should remain in tradeSubscriptions")
	}
	if !sm.orderBookSubscriptions["binance-spot-ETH/USDT"][otherID] {
		t.Fatalf("other client should remain in orderBookSubscriptions")
	}
	if !sm.wildcardSubscribers["binance-spot-all"][otherID] {
		t.Fatalf("other client should remain in wildcardSubscribers")
	}
}

func TestDisconnectCleanupFlappingLeavesNoGhostSubscribers(t *testing.T) {
	sm := &SubscriptionManager{
		tradeSubscriptions:     make(map[string]map[string]bool),
		orderBookSubscriptions: make(map[string]map[string]bool),
		wildcardSubscribers:    make(map[string]map[string]bool),
		exchangeRegistry: map[string]exchanges.Exchange{
			"binance_native": &testExchange{},
			"ccxt_generic":   &testExchange{},
		},
	}

	for i := 0; i < 100; i++ {
		clientID := []byte("client-flap")

		sm.handleRequest(&shared_types.ClientRequest{
			ClientID:   clientID,
			Action:     "subscribe",
			Exchange:   "binance_native",
			Symbol:     "BTC/USDT",
			MarketType: "spot",
			DataType:   "trades",
		})
		sm.handleRequest(&shared_types.ClientRequest{
			ClientID:   clientID,
			Action:     "subscribe",
			Exchange:   "binance_native",
			Symbol:     "ETH/USDT",
			MarketType: "spot",
			DataType:   "orderbooks",
		})
		sm.handleRequest(&shared_types.ClientRequest{
			ClientID:   clientID,
			Action:     "subscribe_all",
			Exchange:   "binance_native",
			MarketType: "spot",
			DataType:   "trades",
		})
		sm.handleRequest(&shared_types.ClientRequest{
			ClientID: clientID,
			Action:   "disconnect",
		})
	}

	for subID, clients := range sm.tradeSubscriptions {
		if clients["client-flap"] {
			t.Fatalf("client-flap leaked in tradeSubscriptions[%s]", subID)
		}
	}
	for subID, clients := range sm.orderBookSubscriptions {
		if clients["client-flap"] {
			t.Fatalf("client-flap leaked in orderBookSubscriptions[%s]", subID)
		}
	}
	for wildcardID, clients := range sm.wildcardSubscribers {
		if clients["client-flap"] {
			t.Fatalf("client-flap leaked in wildcardSubscribers[%s]", wildcardID)
		}
	}
}

func TestDisconnectSendsUnsubscribeForLastSubscriber(t *testing.T) {
	clientID := []byte("client-a")
	rec := &recordingExchange{}

	sm := &SubscriptionManager{
		tradeSubscriptions: map[string]map[string]bool{
			"binance-spot-BTC/USDT": {string(clientID): true},
		},
		orderBookSubscriptions: map[string]map[string]bool{
			"binance-spot-ETH/USDT": {string(clientID): true},
		},
		wildcardSubscribers: make(map[string]map[string]bool),
		exchangeRegistry: map[string]exchanges.Exchange{
			"binance_native": &testExchange{},
			"ccxt_generic":   rec,
		},
	}

	sm.handleRequest(&shared_types.ClientRequest{
		ClientID: clientID,
		Action:   "disconnect",
	})

	if len(rec.reqs) != 2 {
		t.Fatalf("expected 2 unsubscribe requests, got %d", len(rec.reqs))
	}
	seen := map[string]bool{}
	for _, req := range rec.reqs {
		if req.Action != "unsubscribe" || req.Exchange != "binance" || req.MarketType != "spot" {
			t.Fatalf("unexpected unsubscribe request: %+v", req)
		}
		seen[req.DataType+":"+req.Symbol] = true
	}
	if !seen["trades:BTC/USDT"] {
		t.Fatalf("missing trades unsubscribe")
	}
	if !seen["orderbooks:ETH/USDT"] {
		t.Fatalf("missing orderbooks unsubscribe")
	}
}

func TestDisconnectKeepsExactCCXTRoute(t *testing.T) {
	clientID := []byte("client-a")
	rec := &recordingExchange{}

	sm := &SubscriptionManager{
		tradeSubscriptions: map[string]map[string]bool{
			"binance-spot-BTC/USDT": {string(clientID): true},
		},
		orderBookSubscriptions: map[string]map[string]bool{
			"binance-spot-ETH/USDT": {string(clientID): true},
		},
		tradeSubscriptionRoutes: map[string]string{
			getClientRouteKey(string(clientID), "binance-spot-BTC/USDT"): "binance",
		},
		orderBookSubscriptionRoutes: map[string]string{
			getClientRouteKey(string(clientID), "binance-spot-ETH/USDT"): "binance",
		},
		wildcardSubscribers: make(map[string]map[string]bool),
		exchangeRegistry: map[string]exchanges.Exchange{
			"binance_native": &testExchange{},
			"ccxt_generic":   rec,
		},
	}

	sm.handleRequest(&shared_types.ClientRequest{
		ClientID: clientID,
		Action:   "disconnect",
	})

	if len(rec.reqs) != 2 {
		t.Fatalf("expected 2 ccxt unsubscribe requests, got %d", len(rec.reqs))
	}
	for _, req := range rec.reqs {
		if req.Exchange != "binance" {
			t.Fatalf("expected exact ccxt exchange route, got %+v", req)
		}
		if req.Action != "unsubscribe" {
			t.Fatalf("expected unsubscribe action, got %+v", req)
		}
	}
}

func TestBuildSubscriptionsSnapshotGroupsByExactRoute(t *testing.T) {
	clientNative := "client-native"
	clientCCXT := "client-ccxt"
	subID := "binance-spot-BTC/USDT"

	sm := &SubscriptionManager{
		tradeSubscriptions: map[string]map[string]bool{
			subID: {clientNative: true, clientCCXT: true},
		},
		tradeSubscriptionRoutes: map[string]string{
			getClientRouteKey(clientNative, subID): "binance_native",
			getClientRouteKey(clientCCXT, subID):   "binance",
		},
		orderBookSubscriptions:      make(map[string]map[string]bool),
		orderBookSubscriptionRoutes: make(map[string]string),
		orderBookSubscriptionDepths: make(map[string]int),
		runtimeTracker:              newRuntimeTracker(),
	}

	resp := sm.buildSubscriptionsSnapshotResponse("global", "")
	if len(resp.Items) != 2 {
		t.Fatalf("expected 2 runtime subscription items, got %d", len(resp.Items))
	}

	seen := map[string]shared_types.RuntimeSubscriptionItem{}
	for _, item := range resp.Items {
		seen[item.Exchange] = item
	}

	if seen["binance"].Adapter != "ccxt" {
		t.Fatalf("expected binance adapter ccxt, got %+v", seen["binance"])
	}
	if seen["binance_native"].Adapter != "native" {
		t.Fatalf("expected binance_native adapter native, got %+v", seen["binance_native"])
	}
	if seen["binance"].Clients != 1 || seen["binance_native"].Clients != 1 {
		t.Fatalf("expected client counts preserved, got %+v", seen)
	}
}

func TestBuildRuntimeSnapshotIncludesHealth(t *testing.T) {
	sm := &SubscriptionManager{
		tradeSubscriptions: map[string]map[string]bool{
			"binance-spot-BTC/USDT": {"client-a": true},
		},
		tradeSubscriptionRoutes:     map[string]string{getClientRouteKey("client-a", "binance-spot-BTC/USDT"): "binance"},
		orderBookSubscriptions:      make(map[string]map[string]bool),
		orderBookSubscriptionRoutes: make(map[string]string),
		orderBookSubscriptionDepths: make(map[string]int),
		runtimeTracker:              newRuntimeTracker(),
	}

	sm.runtimeTracker.recordTrade(&shared_types.TradeUpdate{
		Exchange:       "binance",
		Symbol:         "BTC/USDT",
		MarketType:     "spot",
		Timestamp:      time.Now().Add(-15 * time.Millisecond).UnixMilli(),
		GoTimestamp:    time.Now().UnixMilli(),
		IngestUnixNano: time.Now().Add(-5 * time.Millisecond).UnixNano(),
	})
	sm.runtimeTracker.recordStatus(&shared_types.StreamStatusEvent{
		Type:       "stream_reconnecting",
		Exchange:   "binance",
		MarketType: "spot",
		Symbol:     "BTC/USDT",
		DataType:   "trades",
		Reason:     "test",
	})
	sm.runtimeTracker.recordStatus(&shared_types.StreamStatusEvent{
		Type:       "stream_restored",
		Exchange:   "binance",
		MarketType: "spot",
		Symbol:     "BTC/USDT",
		DataType:   "trades",
	})

	resp := sm.buildRuntimeSnapshotResponse("")
	if len(resp.Subscriptions) != 1 {
		t.Fatalf("expected 1 subscription item, got %d", len(resp.Subscriptions))
	}
	if len(resp.Health) != 1 {
		t.Fatalf("expected 1 health item, got %d", len(resp.Health))
	}
	if resp.Health[0].Status != "running" {
		t.Fatalf("expected running status, got %+v", resp.Health[0])
	}
	if resp.Totals.ActiveSubscriptions != 1 {
		t.Fatalf("expected active_subscriptions=1, got %+v", resp.Totals)
	}
	if resp.Totals.Reconnects24H != 1 {
		t.Fatalf("expected reconnects_24h=1, got %+v", resp.Totals)
	}
	if resp.Health[0].LatencyMS <= 0 {
		t.Fatalf("expected exchange latency > 0, got %+v", resp.Health[0])
	}
	if resp.Health[0].BrokerLatencyMS <= 0 {
		t.Fatalf("expected broker latency > 0, got %+v", resp.Health[0])
	}
}

func TestBuildRuntimeSnapshotIncludesHealthForNativeRoute(t *testing.T) {
	sm := &SubscriptionManager{
		tradeSubscriptions: map[string]map[string]bool{
			"binance-spot-BTCUSDT": {"client-a": true},
		},
		tradeSubscriptionRoutes:     map[string]string{getClientRouteKey("client-a", "binance-spot-BTCUSDT"): "binance_native"},
		orderBookSubscriptions:      make(map[string]map[string]bool),
		orderBookSubscriptionRoutes: make(map[string]string),
		orderBookSubscriptionDepths: make(map[string]int),
		runtimeTracker:              newRuntimeTracker(),
	}

	sm.runtimeTracker.recordTrade(&shared_types.TradeUpdate{
		Exchange:       "binance",
		Symbol:         "BTCUSDT",
		MarketType:     "spot",
		Timestamp:      time.Now().Add(-15 * time.Millisecond).UnixMilli(),
		GoTimestamp:    time.Now().UnixMilli(),
		IngestUnixNano: time.Now().Add(-5 * time.Millisecond).UnixNano(),
	})

	resp := sm.buildRuntimeSnapshotResponse("")
	if len(resp.Subscriptions) != 1 {
		t.Fatalf("expected 1 subscription item, got %d", len(resp.Subscriptions))
	}
	if len(resp.Health) != 1 {
		t.Fatalf("expected 1 health item, got %d", len(resp.Health))
	}
	if resp.Health[0].Exchange != "binance_native" {
		t.Fatalf("expected native route in health snapshot, got %+v", resp.Health[0])
	}
	if resp.Health[0].MessagesPerSec <= 0 {
		t.Fatalf("expected messages_per_sec > 0, got %+v", resp.Health[0])
	}
	if resp.Health[0].Status != "running" {
		t.Fatalf("expected running status, got %+v", resp.Health[0])
	}
	if resp.Health[0].LatencyMS <= 0 {
		t.Fatalf("expected exchange latency > 0, got %+v", resp.Health[0])
	}
	if resp.Health[0].BrokerLatencyMS <= 0 {
		t.Fatalf("expected broker latency > 0, got %+v", resp.Health[0])
	}
}

func TestBuildCapabilitiesSnapshotResponseIncludesRequestID(t *testing.T) {
	resp := buildCapabilitiesSnapshotResponse("req-1")
	if resp.RequestID != "req-1" {
		t.Fatalf("expected request_id to round-trip, got %+v", resp)
	}
	if len(resp.Items) == 0 {
		t.Fatalf("expected capabilities items")
	}
}

func TestRecordDeployBatchResultEmitsSummary(t *testing.T) {
	distCh := make(chan *DistributionMessage, 1)
	sm := &SubscriptionManager{
		DistributionCh: distCh,
		deployBatches:  make(map[string]*deployBatchState),
	}

	sm.registerDeployBatch(&shared_types.ClientRequest{
		ClientID:  []byte("client-a"),
		RequestID: "deploy-123",
		BatchSent: 2,
	})
	sm.recordDeployBatchResult("deploy-123", false)
	select {
	case <-distCh:
		t.Fatalf("did not expect summary before all results are recorded")
	default:
	}

	sm.recordDeployBatchResult("deploy-123", true)
	msg := <-distCh
	summary, ok := msg.RawPayload.(*shared_types.DeployBatchSummaryEvent)
	if !ok {
		t.Fatalf("expected deploy batch summary payload, got %T", msg.RawPayload)
	}
	if summary.RequestID != "deploy-123" || summary.Sent != 2 || summary.Acked != 1 || summary.Failed != 1 {
		t.Fatalf("unexpected summary payload: %+v", summary)
	}
}

func TestHandleRequestNormalizesBybitOrderBookDepth(t *testing.T) {
	rec := &recordingExchange{}
	sm := &SubscriptionManager{
		tradeSubscriptions:             make(map[string]map[string]bool),
		orderBookSubscriptions:         make(map[string]map[string]bool),
		orderBookSubscriptionRoutes:    make(map[string]string),
		orderBookSubscriptionDepths:    make(map[string]int),
		orderBookSubscriptionEncodings: make(map[string]string),
		exchangeRegistry: map[string]exchanges.Exchange{
			"bybit_native": rec,
		},
		runtimeTracker: newRuntimeTracker(),
	}

	req := &shared_types.ClientRequest{
		ClientID:       []byte("client-a"),
		Action:         "subscribe",
		Exchange:       "bybit_native",
		Symbol:         "BTCUSDT",
		MarketType:     "spot",
		DataType:       "orderbooks",
		OrderBookDepth: 5,
	}
	sm.handleRequest(req)

	if len(rec.reqs) != 1 {
		t.Fatalf("expected one forwarded request, got %d", len(rec.reqs))
	}
	if rec.reqs[0].OrderBookDepth != 50 {
		t.Fatalf("expected bybit depth to normalize to 50, got %+v", rec.reqs[0])
	}
	routeKey := getClientRouteKey("client-a", "bybit-spot-BTCUSDT")
	if got := sm.orderBookSubscriptionDepths[routeKey]; got != 50 {
		t.Fatalf("expected runtime depth 50, got %d", got)
	}
}

func TestHandleRequestNormalizesBinanceOrderBookDepth(t *testing.T) {
	rec := &recordingExchange{}
	sm := &SubscriptionManager{
		tradeSubscriptions:             make(map[string]map[string]bool),
		orderBookSubscriptions:         make(map[string]map[string]bool),
		orderBookSubscriptionRoutes:    make(map[string]string),
		orderBookSubscriptionDepths:    make(map[string]int),
		orderBookSubscriptionEncodings: make(map[string]string),
		exchangeRegistry: map[string]exchanges.Exchange{
			"binance_native": rec,
		},
		runtimeTracker: newRuntimeTracker(),
	}

	req := &shared_types.ClientRequest{
		ClientID:       []byte("client-a"),
		Action:         "subscribe",
		Exchange:       "binance_native",
		Symbol:         "BTCUSDT",
		MarketType:     "spot",
		DataType:       "orderbooks",
		OrderBookDepth: 7,
	}
	sm.handleRequest(req)

	if len(rec.reqs) != 1 {
		t.Fatalf("expected one forwarded request, got %d", len(rec.reqs))
	}
	if rec.reqs[0].OrderBookDepth != 10 {
		t.Fatalf("expected binance depth to normalize to 10, got %+v", rec.reqs[0])
	}
}

func TestHandleRequestRejectsUnsupportedOrderBookDepth(t *testing.T) {
	rec := &recordingExchange{}
	sm := &SubscriptionManager{
		tradeSubscriptions:             make(map[string]map[string]bool),
		orderBookSubscriptions:         make(map[string]map[string]bool),
		orderBookSubscriptionRoutes:    make(map[string]string),
		orderBookSubscriptionDepths:    make(map[string]int),
		orderBookSubscriptionEncodings: make(map[string]string),
		exchangeRegistry: map[string]exchanges.Exchange{
			"binance_native": rec,
		},
		runtimeTracker: newRuntimeTracker(),
	}

	sm.handleRequest(&shared_types.ClientRequest{
		ClientID:       []byte("client-a"),
		Action:         "subscribe",
		Exchange:       "binance_native",
		Symbol:         "BTCUSDT",
		MarketType:     "spot",
		DataType:       "orderbooks",
		OrderBookDepth: 21,
	})

	if len(rec.reqs) != 0 {
		t.Fatalf("expected unsupported depth to be rejected before exchange handler, got %+v", rec.reqs)
	}
}
