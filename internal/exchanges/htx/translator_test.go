package htx

import "testing"

func TestTranslateSpotSymbol(t *testing.T) {
	if got := TranslateSymbolToExchange("BTC/USDT", "spot"); got != "btcusdt" {
		t.Fatalf("expected btcusdt, got %s", got)
	}
	if got := TranslateSymbolFromExchange("btcusdt", "spot"); got != "BTC/USDT" {
		t.Fatalf("expected BTC/USDT, got %s", got)
	}
}

func TestTranslateSwapSymbol(t *testing.T) {
	if got := TranslateSymbolToExchange("BTC/USDT:USDT", "swap"); got != "BTC-USDT" {
		t.Fatalf("expected BTC-USDT, got %s", got)
	}
	if got := TranslateSymbolFromExchange("BTC-USDT", "swap"); got != "BTC/USDT:USDT" {
		t.Fatalf("expected BTC/USDT:USDT, got %s", got)
	}
}
