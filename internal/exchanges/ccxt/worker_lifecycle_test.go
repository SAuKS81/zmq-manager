//go:build ccxt
// +build ccxt

package ccxt

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"bybit-watcher/internal/shared_types"
)

func TestWaitForActiveWorkersReturnsTrueWhenCounterDrains(t *testing.T) {
	var counter atomic.Int64
	counter.Store(1)

	go func() {
		time.Sleep(25 * time.Millisecond)
		counter.Store(0)
	}()

	if !waitForActiveWorkers(&counter, 250*time.Millisecond) {
		t.Fatal("expected waitForActiveWorkers to observe drained workers")
	}
}

func TestBatchShardSendTradeUpdateHonorsContextCancel(t *testing.T) {
	dataCh := make(chan *shared_types.TradeUpdate, 1)
	dataCh <- &shared_types.TradeUpdate{Symbol: "BTC/USDT"}

	sw := &BatchShardWorker{
		dataCh: dataCh,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if sw.sendTradeUpdate(ctx, &shared_types.TradeUpdate{Symbol: "ETH/USDT"}) {
		t.Fatal("expected canceled context to abort blocked trade send")
	}
}

func TestBatchOrderBookSendHonorsContextCancel(t *testing.T) {
	dataCh := make(chan *shared_types.OrderBookUpdate, 1)
	dataCh <- &shared_types.OrderBookUpdate{Symbol: "BTC/USDT"}

	sw := &BatchOrderBookShardWorker{
		dataCh: dataCh,
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if sw.sendOrderBookUpdate(ctx, &shared_types.OrderBookUpdate{Symbol: "ETH/USDT"}) {
		t.Fatal("expected canceled context to abort blocked orderbook send")
	}
}
