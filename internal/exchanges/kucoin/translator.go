package kucoin

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
)

func TranslateSymbolToExchange(ccxtSymbol string) string {
	s := strings.Split(ccxtSymbol, ":")[0]
	return strings.ReplaceAll(s, "/", "-")
}

func TranslateSymbolFromExchange(exchangeSymbol string) string {
	parts := strings.Split(exchangeSymbol, "-")
	if len(parts) >= 2 {
		return parts[0] + "/" + parts[1]
	}
	if strings.HasSuffix(exchangeSymbol, "USDT") {
		base := strings.TrimSuffix(exchangeSymbol, "USDT")
		if base != "" {
			return base + "/USDT"
		}
	}
	return exchangeSymbol
}

func NormalizeTrade(trade *wsClassicTrade, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	if trade == nil {
		return nil, fmt.Errorf("trade is nil")
	}

	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil {
		return nil, fmt.Errorf("parse price: %w", err)
	}
	amount, err := strconv.ParseFloat(trade.Amount, 64)
	if err != nil {
		return nil, fmt.Errorf("parse amount: %w", err)
	}

	return &shared_types.TradeUpdate{
		Exchange:       "kucoin",
		Symbol:         TranslateSymbolFromExchange(trade.Symbol),
		MarketType:     "spot",
		Timestamp:      parseTradeTimestampMS(trade.Time),
		GoTimestamp:    goTimestamp,
		IngestUnixNano: ingestUnixNano,
		Price:          price,
		Amount:         amount,
		Side:           strings.ToLower(trade.Side),
		TradeID:        trade.TradeID,
	}, nil
}

func parseTradeTimestampMS(raw string) int64 {
	if raw == "" {
		return 0
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0
	}
	switch {
	case value >= 1_000_000_000_000_000:
		return value / int64(time.Millisecond)
	case value >= 1_000_000_000_000:
		return value
	}
	return 0
}

func tradeChannel() string {
	return "/market/match"
}

func normalizeOrderBookDepth(depth int) int {
	for _, supported := range validOrderBookDepths {
		if depth <= supported {
			return supported
		}
	}
	return validOrderBookDepths[len(validOrderBookDepths)-1]
}

func orderBookTopic(symbol string, depth int) string {
	switch normalizeOrderBookDepth(depth) {
	case 1:
		return "/spotMarket/level1:" + symbol
	case 5:
		return "/spotMarket/level2Depth5:" + symbol
	default:
		return "/spotMarket/level2Depth50:" + symbol
	}
}

func BuildOrderBookUpdate(symbol string, depth int, payload *wsOrderBookPayload, goTimestamp int64, ingestUnixNano int64) (*shared_types.OrderBookUpdate, error) {
	if payload == nil {
		return nil, fmt.Errorf("orderbook payload is nil")
	}

	bids, err := parseOrderBookSide(payload.Bids)
	if err != nil {
		return nil, fmt.Errorf("parse bids: %w", err)
	}
	asks, err := parseOrderBookSide(payload.Asks)
	if err != nil {
		return nil, fmt.Errorf("parse asks: %w", err)
	}

	update := pools.GetOrderBookUpdate()
	update.Exchange = "kucoin"
	update.Symbol = TranslateSymbolFromExchange(symbol)
	update.MarketType = "spot"
	update.Timestamp = payload.Timestamp
	update.GoTimestamp = goTimestamp
	update.IngestUnixNano = ingestUnixNano
	update.UpdateType = metrics.TypeOBSnapshot
	update.DataType = "orderbooks"
	update.Bids = append(update.Bids[:0], bids...)
	update.Asks = append(update.Asks[:0], asks...)
	return update, nil
}

func parseOrderBookSide(raw []byte) ([]shared_types.OrderBookLevel, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return nil, nil
	}

	var oneDim []string
	if err := json.Unmarshal(raw, &oneDim); err == nil && len(oneDim) >= 2 {
		level, err := parseOrderBookLevel(oneDim[0], oneDim[1])
		if err != nil {
			return nil, err
		}
		return []shared_types.OrderBookLevel{level}, nil
	}

	var twoDim [][]string
	if err := json.Unmarshal(raw, &twoDim); err != nil {
		return nil, err
	}
	levels := make([]shared_types.OrderBookLevel, 0, len(twoDim))
	for _, row := range twoDim {
		if len(row) < 2 {
			continue
		}
		level, err := parseOrderBookLevel(row[0], row[1])
		if err != nil {
			return nil, err
		}
		levels = append(levels, level)
	}
	return levels, nil
}

func parseOrderBookLevel(priceRaw, amountRaw string) (shared_types.OrderBookLevel, error) {
	price, err := strconv.ParseFloat(priceRaw, 64)
	if err != nil {
		return shared_types.OrderBookLevel{}, err
	}
	amount, err := strconv.ParseFloat(amountRaw, 64)
	if err != nil {
		return shared_types.OrderBookLevel{}, err
	}
	return shared_types.OrderBookLevel{Price: price, Amount: amount}, nil
}
