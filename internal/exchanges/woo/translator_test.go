package woo

import (
	"encoding/json"
	"testing"

	"bybit-watcher/internal/pools"
)

func TestTranslateSymbolRoundTripSpot(t *testing.T) {
	exchangeSymbol := TranslateSymbolToExchange("BTC/USDT", "spot")
	if exchangeSymbol != "SPOT_BTC_USDT" {
		t.Fatalf("unexpected exchange symbol: %s", exchangeSymbol)
	}
	if got := TranslateSymbolFromExchange(exchangeSymbol, "spot"); got != "BTC/USDT" {
		t.Fatalf("unexpected unified symbol: %s", got)
	}
}

func TestTranslateSymbolRoundTripSwap(t *testing.T) {
	exchangeSymbol := TranslateSymbolToExchange("BTC/USDT:USDT", "swap")
	if exchangeSymbol != "PERP_BTC_USDT" {
		t.Fatalf("unexpected exchange symbol: %s", exchangeSymbol)
	}
	if got := TranslateSymbolFromExchange(exchangeSymbol, "swap"); got != "BTC/USDT:USDT" {
		t.Fatalf("unexpected unified swap symbol: %s", got)
	}
}

func TestNormalizeTradeSpot(t *testing.T) {
	item := trade{
		Symbol: "SPOT_ADA_USDT",
		Price:  json.Number("1.27988"),
		Size:   json.Number("300"),
		Side:   "BUY",
	}
	update, err := NormalizeTrade(item, "spot", 1618820361552, 1618820362000, 1618820362000000000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer pools.PutTradeUpdate(update)
	if update.Exchange != "woo" || update.Symbol != "ADA/USDT" || update.MarketType != "spot" {
		t.Fatalf("unexpected identity fields: %+v", update)
	}
	if update.Timestamp != 1618820361552 || update.Price != 1.27988 || update.Amount != 300 || update.Side != "buy" {
		t.Fatalf("unexpected trade fields: %+v", update)
	}
}

func TestNormalizeTradeSwap(t *testing.T) {
	item := trade{
		Symbol:    "PERP_BTC_USDT",
		Price:     "42598.27",
		Size:      "0.5",
		Side:      "SELL",
		Timestamp: json.Number("1618820361"),
	}
	update, err := NormalizeTrade(item, "swap", 0, 1618820362000, 1618820362000000000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer pools.PutTradeUpdate(update)
	if update.Symbol != "BTC/USDT:USDT" || update.MarketType != "swap" {
		t.Fatalf("unexpected identity fields: %+v", update)
	}
	if update.Timestamp != 1618820361000 || update.Price != 42598.27 || update.Amount != 0.5 || update.Side != "sell" {
		t.Fatalf("unexpected trade fields: %+v", update)
	}
}
