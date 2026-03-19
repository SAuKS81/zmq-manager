package mexc

import (
	"testing"

	"bybit-watcher/internal/exchanges/mexc/protoc"
	"bybit-watcher/internal/shared_types"
)

func TestNormalizeOrderBookDepth(t *testing.T) {
	tests := []struct {
		in   int
		want int
	}{
		{0, 5},
		{1, 5},
		{5, 5},
		{6, 10},
		{10, 10},
		{11, 20},
		{20, 20},
		{50, 20},
	}

	for _, tt := range tests {
		if got := normalizeOrderBookDepth(tt.in); got != tt.want {
			t.Fatalf("normalizeOrderBookDepth(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}

func TestFindCacheStartIndex(t *testing.T) {
	pending := []depthDelta{
		{FromVersion: 101},
		{FromVersion: 120},
		{FromVersion: 140},
	}
	if got := findCacheStartIndex(99, pending); got != -1 {
		t.Fatalf("expected -1 for stale snapshot, got %d", got)
	}
	if got := findCacheStartIndex(100, pending); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
	if got := findCacheStartIndex(121, pending); got != 2 {
		t.Fatalf("expected 2, got %d", got)
	}
}

func TestApplyDeltaToState(t *testing.T) {
	state := newLocalOrderBookState()
	state.Bids["100.0"] = orderBookLevel(100.0, 1.0)
	state.Asks["101.0"] = orderBookLevel(101.0, 2.0)
	state.Nonce = 100
	state.LastToVersion = 100

	err := applyDeltaToState(state, depthDelta{
		Bids: []*protoc.PublicAggreDepthV3ApiItem{
			{Price: "100.0", Quantity: "3.0"},
			{Price: "99.5", Quantity: "1.5"},
		},
		Asks: []*protoc.PublicAggreDepthV3ApiItem{
			{Price: "101.0", Quantity: "0"},
		},
		FromVersion: 101,
		ToVersion:   105,
	})
	if err != nil {
		t.Fatalf("applyDeltaToState() error = %v", err)
	}
	if got := state.Bids["100.0"].Amount; got != 3.0 {
		t.Fatalf("expected updated bid amount 3.0, got %v", got)
	}
	if _, ok := state.Asks["101.0"]; ok {
		t.Fatalf("expected ask level to be deleted")
	}
	if state.LastToVersion != 105 {
		t.Fatalf("expected LastToVersion 105, got %d", state.LastToVersion)
	}
}

func TestBuildOrderBookUpdateTrimsDepth(t *testing.T) {
	state := newLocalOrderBookState()
	state.Bids["100.0"] = orderBookLevel(100.0, 1)
	state.Bids["99.0"] = orderBookLevel(99.0, 1)
	state.Bids["98.0"] = orderBookLevel(98.0, 1)
	state.Asks["101.0"] = orderBookLevel(101.0, 1)
	state.Asks["102.0"] = orderBookLevel(102.0, 1)
	state.Asks["103.0"] = orderBookLevel(103.0, 1)

	update := buildOrderBookUpdate("BTCUSDT", "spot", 2, state, 1000, 1001, 1002)
	if len(update.Bids) != 2 || len(update.Asks) != 2 {
		t.Fatalf("unexpected trimmed depth bids=%d asks=%d", len(update.Bids), len(update.Asks))
	}
	if update.Bids[0].Price != 100.0 || update.Bids[1].Price != 99.0 {
		t.Fatalf("unexpected bid ordering: %+v", update.Bids)
	}
	if update.Asks[0].Price != 101.0 || update.Asks[1].Price != 102.0 {
		t.Fatalf("unexpected ask ordering: %+v", update.Asks)
	}
}

func orderBookLevel(price float64, amount float64) shared_types.OrderBookLevel {
	return shared_types.OrderBookLevel{Price: price, Amount: amount}
}
