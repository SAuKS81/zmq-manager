package ccxt

import (
	"fmt"
	"log"

	"bybit-watcher/internal/pools" // NEUER IMPORT
	"bybit-watcher/internal/shared_types"
	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
)

// NormalizeTrade wandelt eine CCXT-Trade-Struktur in die standardisierte TradeUpdate-Struktur um.
// Diese Version verwendet einen sync.Pool, um Speicher-Allokationen zu minimieren.
func NormalizeTrade(trade ccxtpro.Trade, exchangeName, marketType string, goTimestamp int64) (*shared_types.TradeUpdate, error) {

	// 1. Kritische Felder prüfen (bleibt unverändert)
	if trade.Symbol == nil || trade.Price == nil || trade.Side == nil || trade.Timestamp == nil {
		log.Printf("[NORMALIZATION-ERROR] Kritische CCXT-Standardfelder sind nil. Trade wird verworfen. Data: %+v", trade)
		return nil, fmt.Errorf("kritische standardfelder sind nil: %+v", trade)
	}

	// 2. Werte extrahieren (bleibt unverändert)
	symbol := *trade.Symbol
	price := *trade.Price
	side := *trade.Side
	timestamp := *trade.Timestamp

	// 3. Fallback-Logik für 'Amount' (bleibt unverändert)
	var amount float64
	if trade.Amount != nil {
		amount = *trade.Amount
	} else if amountVal, ok := trade.Info["amount"].(float64); ok {
		amount = amountVal
	} else if sizeVal, ok := trade.Info["size"].(float64); ok {
		amount = sizeVal
	}
	if amount == 0 {
		return nil, nil // Trade ohne Menge verwerfen
	}

	// 4. Fallback-Logik für 'Trade ID' (bleibt unverändert)
	var tradeID string
	if trade.Id != nil {
		tradeID = *trade.Id
	} else if idVal, ok := trade.Info["id"].(string); ok && idVal != "" {
		tradeID = idVal
	} else {
		tradeID = fmt.Sprintf("%d-%.8f", timestamp, price)
	}

	// ======================================================================
	// OPTIMIERUNG: Objekt aus dem Pool holen statt neu zu erstellen
	// ======================================================================
	// 5. Hole ein recyceltes TradeUpdate-Objekt aus dem Pool.
	normalized := pools.GetTradeUpdate()

	// 6. Fülle die Felder des recycelten Objekts mit den neuen Daten.
	normalized.Exchange = exchangeName
	normalized.Symbol = symbol
	normalized.MarketType = marketType
	normalized.Timestamp = timestamp
	normalized.GoTimestamp = goTimestamp
	normalized.Price = price
	normalized.Amount = amount
	normalized.Side = side
	normalized.TradeID = tradeID
	
	// 7. Gib das gefüllte Objekt zurück.
	return normalized, nil
}
