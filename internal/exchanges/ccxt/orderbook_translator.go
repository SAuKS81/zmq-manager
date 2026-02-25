//go:build ccxt
// +build ccxt

package ccxt

import (
	"bybit-watcher/internal/pools" // NEUER IMPORT
	"bybit-watcher/internal/shared_types"
	"fmt"
	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
)

func NormalizeOrderBook(ob ccxtpro.OrderBook, exchangeName, marketType string, goTimestamp int64, ingestUnixNano int64) (*shared_types.OrderBookUpdate, error) {
	if ob.Symbol == nil || ob.Timestamp == nil {
		return nil, fmt.Errorf("kritisches orderbuch-feld ist nil")
	}

	// Hole ein recyceltes Objekt aus dem Pool
	normalized := pools.GetOrderBookUpdate()

	// Wiederverwende die bestehenden Slices, wenn möglich, um Allokationen zu vermeiden
	bids := normalized.Bids[:0]
	for _, level := range ob.Bids {
		bids = append(bids, shared_types.OrderBookLevel{Price: level[0], Amount: level[1]})
	}

	asks := normalized.Asks[:0]
	for _, level := range ob.Asks {
		asks = append(asks, shared_types.OrderBookLevel{Price: level[0], Amount: level[1]})
	}

	// Fülle das recycelte Objekt mit den neuen Daten
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

