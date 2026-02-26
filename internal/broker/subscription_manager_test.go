package broker

import (
	"testing"

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
			"binance_native": rec,
			"ccxt_generic":   &testExchange{},
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
		if req.Action != "unsubscribe" || req.Exchange != "binance_native" || req.MarketType != "spot" {
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
