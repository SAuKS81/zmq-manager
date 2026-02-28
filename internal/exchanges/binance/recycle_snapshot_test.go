package binance

import "testing"

func containsExactly(got []string, want ...string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(got))
	for _, item := range got {
		seen[item] = true
	}
	for _, item := range want {
		if !seen[item] {
			return false
		}
	}
	return true
}

func TestTradeDesiredStreamsSnapshotIgnoresStaleActiveStreams(t *testing.T) {
	sw := NewShardWorker("ws://example", "spot", nil, nil, nil)
	sw.desiredStreams["btcusdt@trade"] = true
	sw.activeStreams["btcusdt@trade"] = true
	sw.activeStreams["ethusdt@trade"] = true

	got := sw.desiredStreamsSnapshot()
	if !containsExactly(got, "btcusdt@trade") {
		t.Fatalf("expected only desired streams, got %v", got)
	}
}

func TestOrderBookDesiredStreamsSnapshotIgnoresStaleActiveStreams(t *testing.T) {
	sw := NewOrderBookShardWorker("ws://example", "spot", nil, nil, nil)
	sw.desiredStreams["btcusdt@depth5@100ms"] = true
	sw.activeStreams["btcusdt@depth5@100ms"] = true
	sw.activeStreams["ethusdt@depth5@100ms"] = true

	got := sw.desiredStreamsSnapshot()
	if !containsExactly(got, "btcusdt@depth5@100ms") {
		t.Fatalf("expected only desired orderbook streams, got %v", got)
	}
}
