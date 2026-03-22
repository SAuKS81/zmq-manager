package htx

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
)

func TranslateSymbolToExchange(ccxtSymbol, marketType string) string {
	switch marketType {
	case "swap":
		baseAndQuote := strings.Split(strings.TrimSpace(ccxtSymbol), ":")[0]
		parts := strings.Split(baseAndQuote, "/")
		if len(parts) == 2 {
			return strings.ToUpper(parts[0] + "-" + parts[1])
		}
		return strings.ToUpper(strings.ReplaceAll(baseAndQuote, "/", "-"))
	default:
		baseAndQuote := strings.Split(strings.TrimSpace(ccxtSymbol), ":")[0]
		return strings.ToLower(strings.ReplaceAll(baseAndQuote, "/", ""))
	}
}

func TranslateSymbolFromExchange(exchangeSymbol, marketType string) string {
	switch marketType {
	case "swap":
		parts := strings.Split(strings.ToUpper(exchangeSymbol), "-")
		if len(parts) >= 2 {
			return parts[0] + "/" + parts[1] + ":" + parts[1]
		}
		return exchangeSymbol
	default:
		upper := strings.ToUpper(exchangeSymbol)
		switch {
		case strings.HasSuffix(upper, "USDT"):
			return strings.TrimSuffix(upper, "USDT") + "/USDT"
		case strings.HasSuffix(upper, "USDC"):
			return strings.TrimSuffix(upper, "USDC") + "/USDC"
		case strings.HasSuffix(upper, "BTC"):
			return strings.TrimSuffix(upper, "BTC") + "/BTC"
		default:
			return exchangeSymbol
		}
	}
}

func tradeChannel(symbol, marketType string) string {
	return "market." + TranslateSymbolToExchange(symbol, marketType) + ".trade.detail"
}

func normalizeOrderBookDepth(depth int) int {
	for _, supported := range []int{5, 20, 150, 400} {
		if depth <= supported {
			return supported
		}
	}
	return 400
}

func orderBookChannel(symbol string, depth int, marketType string) string {
	depth = normalizeOrderBookDepth(depth)
	exchangeSymbol := TranslateSymbolToExchange(symbol, marketType)
	if marketType == "spot" {
		return "market." + exchangeSymbol + ".mbp." + strconv.Itoa(depth)
	}
	return "market." + exchangeSymbol + ".depth.size_" + strconv.Itoa(depth) + ".high_freq"
}

func NormalizeTrade(channel string, trade wsTrade, marketType string, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	market, err := symbolFromChannel(channel)
	if err != nil {
		return nil, err
	}
	price, err := parseJSONNumberFloat(trade.Price)
	if err != nil {
		return nil, fmt.Errorf("parse price: %w", err)
	}
	amount, err := parseJSONNumberFloat(trade.Amount)
	if err != nil {
		return nil, fmt.Errorf("parse amount: %w", err)
	}
	return &shared_types.TradeUpdate{
		Exchange:       "htx",
		Symbol:         TranslateSymbolFromExchange(market, marketType),
		MarketType:     marketType,
		Timestamp:      trade.Ts,
		GoTimestamp:    goTimestamp,
		IngestUnixNano: ingestUnixNano,
		Price:          price,
		Amount:         amount,
		Side:           strings.ToLower(trade.Direction),
		TradeID:        trade.TradeID.String(),
		DataType:       "trades",
	}, nil
}

func symbolFromChannel(channel string) (string, error) {
	parts := strings.Split(channel, ".")
	if len(parts) < 4 {
		return "", fmt.Errorf("invalid channel: %s", channel)
	}
	return parts[1], nil
}

func parseJSONNumberFloat(n json.Number) (float64, error) {
	return strconv.ParseFloat(n.String(), 64)
}

func normalizeLevels(levels [][]json.Number) ([]shared_types.OrderBookLevel, error) {
	out := make([]shared_types.OrderBookLevel, 0, len(levels))
	for _, level := range levels {
		if len(level) < 2 {
			continue
		}
		price, err := parseJSONNumberFloat(level[0])
		if err != nil {
			return nil, err
		}
		amount, err := parseJSONNumberFloat(level[1])
		if err != nil {
			return nil, err
		}
		out = append(out, shared_types.OrderBookLevel{Price: price, Amount: amount})
	}
	return out, nil
}

func buildOrderBookSnapshot(exchangeSymbol, marketType string, tick *wsTick, goTimestamp int64, ingestUnixNano int64) (*shared_types.OrderBookUpdate, error) {
	if tick == nil {
		return nil, fmt.Errorf("tick is nil")
	}
	bids, err := normalizeLevels(tick.Bids)
	if err != nil {
		return nil, fmt.Errorf("normalize bids: %w", err)
	}
	asks, err := normalizeLevels(tick.Asks)
	if err != nil {
		return nil, fmt.Errorf("normalize asks: %w", err)
	}
	return &shared_types.OrderBookUpdate{
		Exchange:       "htx",
		Symbol:         TranslateSymbolFromExchange(exchangeSymbol, marketType),
		MarketType:     marketType,
		Timestamp:      tick.Ts,
		GoTimestamp:    goTimestamp,
		IngestUnixNano: ingestUnixNano,
		UpdateType:     metrics.TypeOBSnapshot,
		Bids:           bids,
		Asks:           asks,
		DataType:       "orderbooks",
	}, nil
}
