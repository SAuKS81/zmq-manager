//go:build ccxt
// +build ccxt

package ccxt

import (
	"sync"
	"testing"
	"time"

	"bybit-watcher/internal/shared_types"
)

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

