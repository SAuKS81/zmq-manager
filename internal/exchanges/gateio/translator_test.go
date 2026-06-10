package gateio

import (
	"encoding/json"
	"testing"

	"bybit-watcher/internal/pools"
)

func TestTranslateSymbolRoundTripSpot(t *testing.T) {
	exchangeSymbol := TranslateSymbolToExchange("BTC/USDT")
	if exchangeSymbol != "BTC_USDT" {
		t.Fatalf("unexpected exchange symbol: %s", exchangeSymbol)
	}
	if got := TranslateSymbolFromExchange(exchangeSymbol, "spot"); got != "BTC/USDT" {
		t.Fatalf("unexpected unified symbol: %s", got)
	}
}

func TestTranslateSymbolRoundTripSwap(t *testing.T) {
	exchangeSymbol := TranslateSymbolToExchange("BTC/USDT:USDT")
	if exchangeSymbol != "BTC_USDT" {
		t.Fatalf("unexpected exchange symbol: %s", exchangeSymbol)
	}
	if got := TranslateSymbolFromExchange(exchangeSymbol, "swap"); got != "BTC/USDT:USDT" {
		t.Fatalf("unexpected unified swap symbol: %s", got)
	}
}

func TestNormalizeSpotTrade(t *testing.T) {
	trade := spotTrade{
		ID:           json.Number("309143071"),
		CreateTime:   1606292218,
		CreateTimeMS: "1606292218213.4578",
		Side:         "sell",
		CurrencyPair: "GT_USDT",
		Amount:       "16.47",
		Price:        "0.4705",
	}
	update, err := NormalizeSpotTrade(trade, 1606292219000, 1606292219000000000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer pools.PutTradeUpdate(update)
	if update.Exchange != "gate" || update.Symbol != "GT/USDT" || update.MarketType != "spot" {
		t.Fatalf("unexpected identity fields: %+v", update)
	}
	if update.Timestamp != 1606292218213 || update.Price != 0.4705 || update.Amount != 16.47 || update.Side != "sell" {
		t.Fatalf("unexpected trade fields: %+v", update)
	}
}

func TestNormalizeFuturesTrade(t *testing.T) {
	trade := futuresTrade{
		ID:           json.Number("27753479"),
		CreateTime:   1545136464,
		CreateTimeMS: float64(1545136464123),
		Price:        "96.4",
		Size:         -108,
		Contract:     "BTC_USDT",
	}
	update, err := NormalizeFuturesTrade(trade, 1545136465000, 1545136465000000000)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer pools.PutTradeUpdate(update)
	if update.Symbol != "BTC/USDT:USDT" || update.MarketType != "swap" || update.Side != "sell" {
		t.Fatalf("unexpected normalized trade: %+v", update)
	}
	if update.Timestamp != 1545136464123 || update.Amount != 108 || update.Price != 96.4 {
		t.Fatalf("unexpected trade values: %+v", update)
	}
}
