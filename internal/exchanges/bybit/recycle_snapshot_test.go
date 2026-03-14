package bybit

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

func TestTradeDesiredSymbolsSnapshotIgnoresStaleActiveSymbols(t *testing.T) {
	sw := NewShardWorker("ws://example", "spot", nil, nil, nil, nil, nil)
	sw.desiredSymbols["BTCUSDT"] = true
	sw.activeSymbols["BTCUSDT"] = true
	sw.activeSymbols["ETHUSDT"] = true

	got := sw.desiredSymbolsSnapshot()
	if !containsExactly(got, "BTCUSDT") {
		t.Fatalf("expected only desired symbols, got %v", got)
	}
}

func TestOrderBookDesiredTopicsSnapshotIgnoresStaleActiveTopics(t *testing.T) {
	sw := NewOrderBookShardWorker("ws://example", "spot", nil, nil, nil, nil)
	sw.desiredSubscriptions["orderbook.50.BTCUSDT"] = 50
	sw.activeSubscriptions["orderbook.50.BTCUSDT"] = 50
	sw.activeSubscriptions["orderbook.50.ETHUSDT"] = 50

	got := sw.desiredTopicsSnapshot()
	if !containsExactly(got, "orderbook.50.BTCUSDT") {
		t.Fatalf("expected only desired topics, got %v", got)
	}
}

func TestOrderBookManagerRetiresEmptyShard(t *testing.T) {
	cm := NewOrderBookConnectionManager("ws://example", "spot", nil, nil)
	stopCh := make(chan struct{})
	shard := NewOrderBookShardWorker("ws://example", "spot", stopCh, nil, nil, nil)

	cm.shards = append(cm.shards, shard)
	cm.symbolToShard["BTCUSDT"] = shard
	cm.symbolDepth["BTCUSDT"] = 1
	cm.shardLoad[shard] = 1
	cm.shardStops[shard] = stopCh

	cm.removeSubscription("BTCUSDT", 1)

	if len(cm.shards) != 0 {
		t.Fatalf("expected shard to be retired, got %d shards", len(cm.shards))
	}
	if _, ok := cm.symbolToShard["BTCUSDT"]; ok {
		t.Fatalf("expected symbolToShard entry to be removed")
	}
	if _, ok := cm.symbolDepth["BTCUSDT"]; ok {
		t.Fatalf("expected symbolDepth entry to be removed")
	}
	if _, ok := cm.shardLoad[shard]; ok {
		t.Fatalf("expected shardLoad entry to be removed")
	}
	if _, ok := cm.shardStops[shard]; ok {
		t.Fatalf("expected shard stop channel to be removed")
	}
}
