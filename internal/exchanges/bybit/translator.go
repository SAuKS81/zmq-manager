package bybit

import (
	"bybit-watcher/internal/shared_types" // KORREKTUR
	"strconv"
	"strings"
)

// TranslateSymbolToExchange bleibt gleich...
func TranslateSymbolToExchange(ccxtSymbol string) string {
	s := strings.Split(ccxtSymbol, ":")[0]
	s = strings.ReplaceAll(s, "/", "")
	s = strings.ReplaceAll(s, "-", "") // NEUE ZEILE
	return s
}

// TranslateSymbolFromExchange bleibt gleich...
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

// NormalizeTrade wandelt eine Bybit-WebSocket-Nachricht in die standardisierte TradeUpdate-Struktur um.
func NormalizeTrade(trade wsTrade, marketType string, goTimestamp int64) (*shared_types.TradeUpdate, error) {
	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil {
		return nil, err
	}
	amount, err := strconv.ParseFloat(trade.Volume, 64)
	if err != nil {
		return nil, err
	}

	return &shared_types.TradeUpdate{
		Exchange:    "bybit",
		Symbol:      TranslateSymbolFromExchange(trade.Symbol, marketType),
		MarketType:  marketType,
		Timestamp:   trade.Timestamp, // Bybit Timestamp
		GoTimestamp: goTimestamp,     // NEU: Hinzugefügt
		Price:       price,
		Amount:      amount,
		Side:        strings.ToLower(trade.Side),
		TradeID:     trade.TradeID,
	}, nil
}


