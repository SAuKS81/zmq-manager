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

// NormalizeTrade converts a CCXT trade into shared TradeUpdate using pooled objects.
func NormalizeTrade(trade ccxtpro.Trade, exchangeName, marketType string, goTimestamp int64, ingestUnixNano int64) (_ *shared_types.TradeUpdate, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("trade normalize panic: %v\n%s", r, string(debug.Stack()))
		}
	}()

	if trade.Symbol == nil || trade.Price == nil || trade.Side == nil || trade.Timestamp == nil {
		return nil, fmt.Errorf("critical trade fields are nil")
	}

	symbol := *trade.Symbol
	price := *trade.Price
	side := *trade.Side
	timestamp := *trade.Timestamp

	var amount float64
	if trade.Amount != nil {
		amount = *trade.Amount
	} else if amountVal, ok := trade.Info["amount"].(float64); ok {
		amount = amountVal
	} else if sizeVal, ok := trade.Info["size"].(float64); ok {
		amount = sizeVal
	}
	if amount == 0 {
		return nil, nil
	}

	var tradeID string
	if trade.Id != nil {
		tradeID = *trade.Id
	} else if idVal, ok := trade.Info["id"].(string); ok && idVal != "" {
		tradeID = idVal
	} else {
		tradeID = fmt.Sprintf("%d-%.8f", timestamp, price)
	}

	normalized := pools.GetTradeUpdate()
	normalized.Exchange = exchangeName
	normalized.Symbol = symbol
	normalized.MarketType = marketType
	normalized.Timestamp = timestamp
	normalized.GoTimestamp = goTimestamp
	normalized.IngestUnixNano = ingestUnixNano
	normalized.Price = price
	normalized.Amount = amount
	normalized.Side = side
	normalized.TradeID = tradeID

	return normalized, nil
}
