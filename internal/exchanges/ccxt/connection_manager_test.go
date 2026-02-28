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

func TestUnsubscribeOrderBookSymbolsLocked(t *testing.T) {
	cm := NewConnectionManager(
		"binance",
		"spot",
		ExchangeConfig{Enabled: true},
		make(chan *shared_types.TradeUpdate, 1),
		make(chan *shared_types.OrderBookUpdate, 1),
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
