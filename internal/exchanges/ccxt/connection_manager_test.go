//go:build ccxt
// +build ccxt

package ccxt

import (
	"sync"
	"testing"
	"time"

	"bybit-watcher/internal/shared_types"
)

type fakeShardWorker struct {
	commandCh chan ShardCommand
}

func (f *fakeShardWorker) Run() {}

func (f *fakeShardWorker) getCommandChannel() chan<- ShardCommand {
	return f.commandCh
}

func TestUnsubscribeTradeSymbolsLocked(t *testing.T) {
	cm := NewConnectionManager(
		"binance",
		"spot",
		ExchangeConfig{Enabled: true},
		make(chan *shared_types.TradeUpdate, 1),
		make(chan *shared_types.OrderBookUpdate, 1),
		make(chan *shared_types.StreamStatusEvent, 1),
	)

	shard := &fakeShardWorker{commandCh: make(chan ShardCommand, 1)}
	cm.tradeShards = []IShardWorker{shard}
	cm.symbolToTradeShard["BTC/USDT"] = shard
	cm.tradeShardLoad[shard] = 1

	cm.unsubscribeTradeSymbolsLocked(map[string]int{"BTC/USDT": 0})

	select {
	case cmd := <-shard.commandCh:
		if cmd.Action != "unsubscribe" {
			t.Fatalf("unexpected action: %s", cmd.Action)
		}
		if _, ok := cmd.Symbols["BTC/USDT"]; !ok {
			t.Fatalf("expected BTC/USDT in unsubscribe payload")
		}
	default:
		t.Fatalf("expected unsubscribe command")
	}

	if _, ok := cm.symbolToTradeShard["BTC/USDT"]; ok {
		t.Fatalf("symbolToTradeShard entry was not removed")
	}
	if got := cm.tradeShardLoad[shard]; got != 0 {
		t.Fatalf("expected shard load 0, got %d", got)
	}
}

func TestUnsubscribeTradeSymbolsLockedRetiresEmptyShard(t *testing.T) {
	cm := NewConnectionManager(
		"binance",
		"spot",
		ExchangeConfig{Enabled: true},
		make(chan *shared_types.TradeUpdate, 1),
		make(chan *shared_types.OrderBookUpdate, 1),
		make(chan *shared_types.StreamStatusEvent, 1),
	)

	shard := &fakeShardWorker{commandCh: make(chan ShardCommand, 1)}
	stopCh := make(chan struct{})
	cm.tradeShards = []IShardWorker{shard}
	cm.tradeShardStops[shard] = stopCh
	cm.symbolToTradeShard["BTC/USDT"] = shard
	cm.tradeShardLoad[shard] = 1

	cm.unsubscribeTradeSymbolsLocked(map[string]int{"BTC/USDT": 0})

	select {
	case <-stopCh:
	default:
		t.Fatalf("expected empty trade shard to be retired")
	}
	if len(cm.tradeShards) != 0 {
		t.Fatalf("expected retired trade shard to be removed, got %d shards", len(cm.tradeShards))
	}
}

func TestUnsubscribeOrderBookSymbolsLocked(t *testing.T) {
	cm := NewConnectionManager(
		"binance",
		"spot",
		ExchangeConfig{Enabled: true},
		make(chan *shared_types.TradeUpdate, 1),
		make(chan *shared_types.OrderBookUpdate, 1),
		make(chan *shared_types.StreamStatusEvent, 1),
	)

	shard := &fakeShardWorker{commandCh: make(chan ShardCommand, 1)}
	cm.obShards = []IShardWorker{shard}
	cm.symbolToOBShard["BTC/USDT"] = shard
	cm.obShardLoad[shard] = 1

	cm.unsubscribeOrderBookSymbolsLocked(map[string]int{"BTC/USDT": 5})

	select {
	case cmd := <-shard.commandCh:
		if cmd.Action != "unsubscribe" {
			t.Fatalf("unexpected action: %s", cmd.Action)
		}
		if _, ok := cmd.Symbols["BTC/USDT"]; !ok {
			t.Fatalf("expected BTC/USDT in unsubscribe payload")
		}
	default:
		t.Fatalf("expected unsubscribe command")
	}

	if _, ok := cm.symbolToOBShard["BTC/USDT"]; ok {
		t.Fatalf("symbolToOBShard entry was not removed")
	}
	if got := cm.obShardLoad[shard]; got != 0 {
		t.Fatalf("expected shard load 0, got %d", got)
	}
}

func TestUnsubscribeOrderBookSymbolsLockedRetiresEmptyShard(t *testing.T) {
	cm := NewConnectionManager(
		"binance",
		"spot",
		ExchangeConfig{Enabled: true},
		make(chan *shared_types.TradeUpdate, 1),
		make(chan *shared_types.OrderBookUpdate, 1),
		make(chan *shared_types.StreamStatusEvent, 1),
	)

	shard := &fakeShardWorker{commandCh: make(chan ShardCommand, 1)}
	stopCh := make(chan struct{})
	cm.obShards = []IShardWorker{shard}
	cm.obShardStops[shard] = stopCh
	cm.symbolToOBShard["BTC/USDT"] = shard
	cm.obShardLoad[shard] = 1

	cm.unsubscribeOrderBookSymbolsLocked(map[string]int{"BTC/USDT": 5})

	select {
	case <-stopCh:
	default:
		t.Fatalf("expected empty orderbook shard to be retired")
	}
	if len(cm.obShards) != 0 {
		t.Fatalf("expected retired orderbook shard to be removed, got %d shards", len(cm.obShards))
	}
}

func TestConnectionManagerStartStopNoDeadlock(t *testing.T) {
	cfg := ExchangeConfig{
		Enabled:         true,
		UseForSymbols:   false,
		BatchSize:       1,
		SymbolsPerShard: 1,
		SubscribePause:  0,
		NewShardPause:   0,
	}

	for i := 0; i < 20; i++ {
		cm := NewConnectionManager(
			"binance",
			"spot",
			cfg,
			make(chan *shared_types.TradeUpdate, 1),
			make(chan *shared_types.OrderBookUpdate, 1),
			make(chan *shared_types.StreamStatusEvent, 1),
		)

		doneRun := make(chan struct{})
		go func() {
			cm.Run()
			close(doneRun)
		}()

		time.Sleep(10 * time.Millisecond)

		var wg sync.WaitGroup
		wg.Add(1)
		go cm.Stop(&wg)

		waitOrTimeout(t, &wg, 2*time.Second, "Stop waitgroup timeout on iteration")
		waitChanOrTimeout(t, doneRun, 2*time.Second, "Run loop did not exit on iteration")
	}
}

func TestTradeShardCapacityUsesBatchSizeWhenOnlyOneBatchPerShardIsAllowed(t *testing.T) {
	cm := NewConnectionManager(
		"kucoin",
		"spot",
		ExchangeConfig{
			Enabled:               true,
			UseForSymbols:         true,
			BatchSize:             100,
			SymbolsPerShard:       200,
			OneTradeBatchPerShard: true,
		},
		make(chan *shared_types.TradeUpdate, 1),
		make(chan *shared_types.OrderBookUpdate, 1),
		make(chan *shared_types.StreamStatusEvent, 1),
	)

	if got := cm.tradeShardCapacity(); got != 100 {
		t.Fatalf("expected trade shard capacity 100, got %d", got)
	}
}

func TestProcessTradeCommandsDoesNotResubscribeWhenCacheNIsUnchanged(t *testing.T) {
	cm := NewConnectionManager(
		"binance",
		"spot",
		ExchangeConfig{Enabled: true},
		make(chan *shared_types.TradeUpdate, 1),
		make(chan *shared_types.OrderBookUpdate, 1),
		make(chan *shared_types.StreamStatusEvent, 1),
	)

	shard := &fakeShardWorker{commandCh: make(chan ShardCommand, 2)}
	cm.tradeShards = []IShardWorker{shard}
	cm.symbolToTradeShard["BTC/USDT"] = shard
	cm.symbolToTradeCache["BTC/USDT"] = 1
	cm.tradeShardLoad[shard] = 1

	cm.processTradeCommands([]ManagerCommand{{Action: "add", Symbol: "BTC/USDT", DataType: "trades", CacheN: 1}})

	select {
	case cmd := <-shard.commandCh:
		t.Fatalf("expected no reconfigure command, got %+v", cmd)
	default:
	}
}

func TestProcessTradeCommandsReconfiguresWhenCacheNChanges(t *testing.T) {
	cm := NewConnectionManager(
		"binance",
		"spot",
		ExchangeConfig{Enabled: true},
		make(chan *shared_types.TradeUpdate, 1),
		make(chan *shared_types.OrderBookUpdate, 1),
		make(chan *shared_types.StreamStatusEvent, 1),
	)

	shard := &fakeShardWorker{commandCh: make(chan ShardCommand, 4)}
	cm.tradeShards = []IShardWorker{shard}
	cm.symbolToTradeShard["BTC/USDT"] = shard
	cm.symbolToTradeCache["BTC/USDT"] = 1
	cm.tradeShardLoad[shard] = 1

	cm.processTradeCommands([]ManagerCommand{{Action: "add", Symbol: "BTC/USDT", DataType: "trades", CacheN: 5}})

	first := <-shard.commandCh
	second := <-shard.commandCh
	if first.Action != "unsubscribe" {
		t.Fatalf("expected first command unsubscribe, got %+v", first)
	}
	if second.Action != "subscribe" {
		t.Fatalf("expected second command subscribe, got %+v", second)
	}
	if second.Symbols["BTC/USDT"] != 5 {
		t.Fatalf("expected subscribe command to carry cache_n=5, got %+v", second)
	}
	if got := cm.symbolToTradeCache["BTC/USDT"]; got != 5 {
		t.Fatalf("expected manager cache_n=5, got %d", got)
	}
}

func TestProcessSingleOrderBookCommandsReconfiguresWhenDepthChanges(t *testing.T) {
	cm := NewConnectionManager(
		"binance",
		"spot",
		ExchangeConfig{Enabled: true},
		make(chan *shared_types.TradeUpdate, 1),
		make(chan *shared_types.OrderBookUpdate, 1),
		make(chan *shared_types.StreamStatusEvent, 1),
	)

	shard := &fakeShardWorker{commandCh: make(chan ShardCommand, 4)}
	cm.obShards = []IShardWorker{shard}
	cm.symbolToOBShard["BTC/USDT"] = shard
	cm.symbolToOBDepth["BTC/USDT"] = 5
	cm.obShardLoad[shard] = 1

	cm.processSingleOrderBookCommands([]ManagerCommand{{Action: "add", Symbol: "BTC/USDT", DataType: "orderbooks", Depth: 20}})

	first := <-shard.commandCh
	second := <-shard.commandCh
	if first.Action != "unsubscribe" {
		t.Fatalf("expected first command unsubscribe, got %+v", first)
	}
	if second.Action != "subscribe" {
		t.Fatalf("expected second command subscribe, got %+v", second)
	}
	if second.Symbols["BTC/USDT"] != 20 {
		t.Fatalf("expected subscribe command to carry depth=20, got %+v", second)
	}
	if got := cm.symbolToOBDepth["BTC/USDT"]; got != 20 {
		t.Fatalf("expected manager depth=20, got %d", got)
	}
}

func waitOrTimeout(t *testing.T, wg *sync.WaitGroup, timeout time.Duration, msg string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatalf("%s", msg)
	}
}

func waitChanOrTimeout(t *testing.T, ch <-chan struct{}, timeout time.Duration, msg string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(timeout):
		t.Fatalf("%s", msg)
	}
}
