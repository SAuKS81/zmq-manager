package gateio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
)

func TranslateSymbolToExchange(ccxtSymbol string) string {
	s := strings.Split(strings.TrimSpace(ccxtSymbol), ":")[0]
	return strings.ToUpper(strings.ReplaceAll(s, "/", "_"))
}

func TranslateSymbolFromExchange(exchangeSymbol, marketType string) string {
	parts := strings.Split(strings.ToUpper(strings.TrimSpace(exchangeSymbol)), "_")
	if len(parts) < 2 {
		return strings.ToUpper(strings.TrimSpace(exchangeSymbol))
	}
	base, quote := parts[0], parts[1]
	if marketType == "swap" {
		return base + "/" + quote + ":" + quote
	}
	return base + "/" + quote
}

func NormalizeSpotTrade(trade spotTrade, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil {
		return nil, err
	}
	amount, err := strconv.ParseFloat(trade.Amount, 64)
	if err != nil {
		return nil, err
	}
	timestamp := parseGateTimeMS(trade.CreateTimeMS, trade.CreateTime)
	update := pools.GetTradeUpdate()
	update.Exchange = "gate"
	update.Symbol = TranslateSymbolFromExchange(trade.CurrencyPair, "spot")
	update.MarketType = "spot"
	update.Timestamp = timestamp
	update.GoTimestamp = goTimestamp
	update.IngestUnixNano = ingestUnixNano
	update.Price = price
	update.Amount = amount
	update.Side = strings.ToLower(trade.Side)
	update.TradeID = trade.ID.String()
	update.DataType = "trades"
	return update, nil
}

func NormalizeFuturesTrade(trade futuresTrade, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil {
		return nil, err
	}
	side := "buy"
	if trade.Size < 0 {
		side = "sell"
	}
	timestamp := parseGateTimeMS(trade.CreateTimeMS, trade.CreateTime)
	update := pools.GetTradeUpdate()
	update.Exchange = "gate"
	update.Symbol = TranslateSymbolFromExchange(trade.Contract, "swap")
	update.MarketType = "swap"
	update.Timestamp = timestamp
	update.GoTimestamp = goTimestamp
	update.IngestUnixNano = ingestUnixNano
	update.Price = price
	update.Amount = math.Abs(float64(trade.Size))
	update.Side = side
	update.TradeID = trade.ID.String()
	update.DataType = "trades"
	return update, nil
}

func decodeSpotTrade(raw json.RawMessage) (spotTrade, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var trade spotTrade
	if err := dec.Decode(&trade); err != nil {
		return spotTrade{}, err
	}
	if trade.CurrencyPair == "" {
		return spotTrade{}, fmt.Errorf("missing currency_pair")
	}
	return trade, nil
}

func decodeFuturesTrades(raw json.RawMessage) ([]futuresTrade, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var trades []futuresTrade
	if err := dec.Decode(&trades); err != nil {
		return nil, err
	}
	return trades, nil
}

func parseGateTimeMS(value interface{}, fallbackSeconds int64) int64 {
	switch v := value.(type) {
	case json.Number:
		if i, err := v.Int64(); err == nil {
			return i
		}
		if f, err := v.Float64(); err == nil {
			return int64(f)
		}
	case float64:
		return int64(v)
	case string:
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i
		}
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int64(f)
		}
	}
	return fallbackSeconds * 1000
}

func tradeChannel(marketType string) string {
	if marketType == "swap" {
		return channelSwapTrades
	}
	return channelSpotTrades
}
