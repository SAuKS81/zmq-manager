//go:build ccxt
// +build ccxt

package ccxt

import (
	"fmt"
	"runtime/debug"

	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
)

// NormalizeOrderBook converts CCXT orderbook payload to shared OrderBookUpdate.
func NormalizeOrderBook(ob ccxtpro.OrderBook, exchangeName, marketType string, goTimestamp int64, ingestUnixNano int64) (_ *shared_types.OrderBookUpdate, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("orderbook normalize panic: %v\n%s", r, string(debug.Stack()))
		}
	}()

	if ob.Symbol == nil || ob.Timestamp == nil {
		return nil, fmt.Errorf("critical orderbook fields are nil")
	}

	normalized := pools.GetOrderBookUpdate()

	bids := normalized.Bids[:0]
	for _, level := range ob.Bids {
		if len(level) < 2 {
			continue
		}
		bids = append(bids, shared_types.OrderBookLevel{Price: level[0], Amount: level[1]})
	}

	asks := normalized.Asks[:0]
	for _, level := range ob.Asks {
		if len(level) < 2 {
			continue
		}
		asks = append(asks, shared_types.OrderBookLevel{Price: level[0], Amount: level[1]})
	}

	normalized.Exchange = exchangeName
	normalized.Symbol = *ob.Symbol
	normalized.MarketType = marketType
	normalized.Timestamp = *ob.Timestamp
	normalized.GoTimestamp = goTimestamp
	normalized.IngestUnixNano = ingestUnixNano
	normalized.UpdateType = "ob_update"
	normalized.Bids = bids
	normalized.Asks = asks

	return normalized, nil
}
