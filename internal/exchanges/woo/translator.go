package woo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
)

func TranslateSymbolToExchange(symbol, marketType string) string {
	s := strings.ToUpper(strings.TrimSpace(symbol))
	if s == "" {
		return ""
	}
	if strings.HasPrefix(s, "SPOT_") || strings.HasPrefix(s, "PERP_") {
		return s
	}
	s = strings.Split(s, ":")[0]
	parts := strings.Split(s, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return ""
	}
	prefix := "SPOT"
	if marketType == "swap" {
		prefix = "PERP"
	}
	return prefix + "_" + parts[0] + "_" + parts[1]
}

func TranslateSymbolFromExchange(exchangeSymbol, marketType string) string {
	s := strings.ToUpper(strings.TrimSpace(exchangeSymbol))
	s = strings.TrimSuffix(s, "@TRADE")
	s = strings.TrimSuffix(s, "@TRADES")
	parts := strings.Split(s, "_")
	if len(parts) < 3 {
		return s
	}
	base, quote := parts[1], parts[2]
	if marketType == "swap" || parts[0] == "PERP" {
		return base + "/" + quote + ":" + quote
	}
	return base + "/" + quote
}

func tradeTopic(exchangeSymbol string) string {
	return exchangeSymbol + "@trade"
}

func NormalizeTrade(input trade, marketType string, fallbackTS int64, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	price, err := parseFloat(input.Price)
	if err != nil {
		return nil, fmt.Errorf("invalid price: %w", err)
	}
	amount, err := parseFloat(input.Size)
	if err != nil {
		return nil, fmt.Errorf("invalid size: %w", err)
	}
	timestamp := parseTimestamp(input.Timestamp)
	if timestamp == 0 {
		timestamp = parseTimestamp(input.Timestamp2)
	}
	if timestamp == 0 {
		timestamp = fallbackTS
	}
	update := pools.GetTradeUpdate()
	update.Exchange = "woo"
	update.Symbol = TranslateSymbolFromExchange(input.Symbol, marketType)
	update.MarketType = marketType
	update.Timestamp = timestamp
	update.GoTimestamp = goTimestamp
	update.IngestUnixNano = ingestUnixNano
	update.Price = price
	update.Amount = amount
	update.Side = strings.ToLower(input.Side)
	update.TradeID = tradeID(input)
	update.DataType = "trades"
	return update, nil
}

func decodeTrade(raw json.RawMessage) (trade, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var item trade
	if err := dec.Decode(&item); err != nil {
		return trade{}, err
	}
	if item.Symbol == "" {
		return trade{}, fmt.Errorf("missing symbol")
	}
	return item, nil
}

func decodeTradeList(raw json.RawMessage) ([]trade, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var items []trade
	if err := dec.Decode(&items); err != nil {
		return nil, err
	}
	return items, nil
}

func parseFloat(value interface{}) (float64, error) {
	switch v := value.(type) {
	case json.Number:
		return v.Float64()
	case float64:
		return v, nil
	case float32:
		return float64(v), nil
	case int:
		return float64(v), nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(strings.TrimSpace(v), 64)
	default:
		return 0, fmt.Errorf("unsupported numeric type %T", value)
	}
}

func parseTimestamp(value interface{}) int64 {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return normalizeTimestamp(i)
		}
		if f, err := v.Float64(); err == nil {
			return normalizeTimestamp(int64(f))
		}
	case float64:
		return normalizeTimestamp(int64(v))
	case int64:
		return normalizeTimestamp(v)
	case int:
		return normalizeTimestamp(int64(v))
	case string:
		if i, err := strconv.ParseInt(strings.TrimSpace(v), 10, 64); err == nil {
			return normalizeTimestamp(i)
		}
		if f, err := strconv.ParseFloat(strings.TrimSpace(v), 64); err == nil {
			return normalizeTimestamp(int64(f))
		}
	}
	return 0
}

func normalizeTimestamp(ts int64) int64 {
	if ts > 0 && ts < 1_000_000_000_000 {
		return ts * 1000
	}
	return ts
}

func tradeID(input trade) string {
	for _, value := range []interface{}{input.TradeID, input.TradeID2} {
		switch v := value.(type) {
		case json.Number:
			return v.String()
		case string:
			return strings.TrimSpace(v)
		case float64:
			return strconv.FormatInt(int64(v), 10)
		case int64:
			return strconv.FormatInt(v, 10)
		case int:
			return strconv.Itoa(v)
		}
	}
	return ""
}
