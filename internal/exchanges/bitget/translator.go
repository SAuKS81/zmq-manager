package bitget

import (
	"fmt"
	"bybit-watcher/internal/shared_types"
	"strconv"
	"strings"
)

// getInstType übersetzt den internen Market-Typ in den Bitget-spezifischen `instType`.
func getInstType(marketType string) (string, error) {
	switch marketType {
	case "spot":
		return "SPOT", nil
	case "swap":
		return "USDT-FUTURES", nil // USDT-FUTURES = USDT-M Perpetual Contracts
	default:
		return "", fmt.Errorf("unbekannter marketType für Bitget: %s", marketType)
	}
}

// TranslateSymbolToExchange wandelt ein CCXT-Symbol (z.B. "BTC/USDT:USDT")
// in das Bitget-Format ("BTCUSDT") um.
func TranslateSymbolToExchange(ccxtSymbol string) string {
	s := strings.Split(ccxtSymbol, ":")[0]
	return strings.ReplaceAll(s, "/", "")
}

// TranslateSymbolFromExchange wandelt ein Bitget-Symbol ("BTCUSDT")
// zurück in das CCXT-Format um.
func TranslateSymbolFromExchange(bitgetSymbol, marketType string) string {
	var base, quote string
	if strings.HasSuffix(bitgetSymbol, "USDT") {
		base = strings.TrimSuffix(bitgetSymbol, "USDT")
		quote = "USDT"
	} else {
		// Fallback für andere Paare, falls nötig
		base = bitgetSymbol
		quote = ""
	}
	
	ccxtSymbol := base + "/" + quote
	if marketType == "swap" {
		return ccxtSymbol + ":" + quote
	}
	return ccxtSymbol
}

// NormalizeTrade wandelt eine Bitget-Trade-Nachricht in die standardisierte TradeUpdate-Struktur um.
// Bitget-Trade-Format: [Timestamp (string ms), Preis (string), Menge (string), Seite (string)]
func NormalizeTrade(tradeData wsTradeData, arg wsSubArg, goTimestamp int64) (*shared_types.TradeUpdate, error) {
	// Konvertiere die String-Werte in die benötigten Typen.
	timestamp, err := strconv.ParseInt(tradeData.Timestamp, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("fehler beim Parsen des Timestamps: %v", err)
	}
	price, err := strconv.ParseFloat(tradeData.Price, 64)
	if err != nil {
		return nil, fmt.Errorf("fehler beim Parsen des Preises: %v", err)
	}
	amount, err := strconv.ParseFloat(tradeData.Size, 64)
	if err != nil {
		return nil, fmt.Errorf("fehler beim Parsen der Menge: %v", err)
	}

	// Übersetze den Market-Typ zurück in unseren internen Standard (spot, swap).
	marketType := "unknown"
	if arg.InstType == "SPOT" {
		marketType = "spot"
	} else if arg.InstType == "USDT-FUTURES" {
		marketType = "swap"
	}

	return &shared_types.TradeUpdate{
		Exchange:    "bitget",
		Symbol:      TranslateSymbolFromExchange(arg.InstID, marketType),
		MarketType:  marketType,
		Timestamp:   timestamp,
		GoTimestamp: goTimestamp,
		Price:       price,
		Amount:      amount,
		Side:        strings.ToLower(tradeData.Side),
		TradeID:     tradeData.TradeID, // Wir verwenden jetzt die echte Trade-ID von der API.
	}, nil
}

