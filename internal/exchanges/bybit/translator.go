package bybit

import (
	"bybit-watcher/internal/shared_types"
	"strconv"
	"strings"
)

func TranslateSymbolToExchange(ccxtSymbol string) string {
	s := strings.Split(ccxtSymbol, ":")[0]
	s = strings.ReplaceAll(s, "/", "")
	s = strings.ReplaceAll(s, "-", "")
	return s
}

func TranslateSymbolFromExchange(bybitSymbol, marketType string) string {
	var base string
	if strings.HasSuffix(bybitSymbol, "USDT") {
		base = strings.TrimSuffix(bybitSymbol, "USDT")
	} else {
		base = bybitSymbol
	}
	ccxtBase := base + "/USDT"
	if marketType == "swap" {
		return ccxtBase + ":USDT"
	}
	return ccxtBase
}

func NormalizeTrade(trade wsTrade, marketType string, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil {
		return nil, err
	}
	amount, err := strconv.ParseFloat(trade.Volume, 64)
	if err != nil {
		return nil, err
	}

	return &shared_types.TradeUpdate{
		Exchange:       "bybit",
		Symbol:         TranslateSymbolFromExchange(trade.Symbol, marketType),
		MarketType:     marketType,
		Timestamp:      trade.Timestamp,
		GoTimestamp:    goTimestamp,
		IngestUnixNano: ingestUnixNano,
		Price:          price,
		Amount:         amount,
		Side:           strings.ToLower(trade.Side),
		TradeID:        trade.TradeID,
	}, nil
}
