package coinex

import (
	"fmt"
	"strconv"
	"strings"

	"bybit-watcher/internal/shared_types"
)

func TranslateSymbolToExchange(ccxtSymbol string) string {
	s := strings.Split(ccxtSymbol, ":")[0]
	return strings.ReplaceAll(s, "/", "")
}

func TranslateSymbolFromExchange(exchangeSymbol, marketType string) string {
	var base, quote string
	switch {
	case strings.HasSuffix(exchangeSymbol, "USDT"):
		base = strings.TrimSuffix(exchangeSymbol, "USDT")
		quote = "USDT"
	case strings.HasSuffix(exchangeSymbol, "USDC"):
		base = strings.TrimSuffix(exchangeSymbol, "USDC")
		quote = "USDC"
	default:
		return exchangeSymbol
	}

	unified := base + "/" + quote
	if marketType == "swap" {
		return unified + ":" + quote
	}
	return unified
}

func NormalizeTrade(marketType string, market string, trade wsDeal, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil {
		return nil, fmt.Errorf("parse price: %w", err)
	}
	amount, err := strconv.ParseFloat(trade.Amount, 64)
	if err != nil {
		return nil, fmt.Errorf("parse amount: %w", err)
	}

	return &shared_types.TradeUpdate{
		Exchange:       "coinex",
		Symbol:         TranslateSymbolFromExchange(market, marketType),
		MarketType:     marketType,
		Timestamp:      trade.CreatedAt,
		GoTimestamp:    goTimestamp,
		IngestUnixNano: ingestUnixNano,
		Price:          price,
		Amount:         amount,
		Side:           strings.ToLower(trade.Side),
		TradeID:        strconv.FormatInt(trade.DealID, 10),
	}, nil
}

func NormalizeOrderBookDepth(depth int) int {
	switch depth {
	case 5, 10, 20, 50:
		return depth
	default:
		return 50
	}
}

func NormalizeOrderBook(marketType string, market string, depth wsDepthBooks, goTimestamp int64, ingestUnixNano int64) (*shared_types.OrderBookUpdate, error) {
	bids, err := normalizeLevels(depth.Bids)
	if err != nil {
		return nil, fmt.Errorf("normalize bids: %w", err)
	}
	asks, err := normalizeLevels(depth.Asks)
	if err != nil {
		return nil, fmt.Errorf("normalize asks: %w", err)
	}

	return &shared_types.OrderBookUpdate{
		Exchange:       "coinex",
		Symbol:         TranslateSymbolFromExchange(market, marketType),
		MarketType:     marketType,
		Timestamp:      depth.UpdatedAt,
		GoTimestamp:    goTimestamp,
		IngestUnixNano: ingestUnixNano,
		UpdateType:     "snapshot",
		Bids:           bids,
		Asks:           asks,
		DataType:       "orderbooks",
	}, nil
}

func normalizeLevels(raw [][]string) ([]shared_types.OrderBookLevel, error) {
	levels := make([]shared_types.OrderBookLevel, 0, len(raw))
	for _, level := range raw {
		if len(level) < 2 {
			continue
		}
		price, err := strconv.ParseFloat(level[0], 64)
		if err != nil {
			return nil, err
		}
		amount, err := strconv.ParseFloat(level[1], 64)
		if err != nil {
			return nil, err
		}
		levels = append(levels, shared_types.OrderBookLevel{
			Price:  price,
			Amount: amount,
		})
	}
	return levels, nil
}
