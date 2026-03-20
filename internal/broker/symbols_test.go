package broker

import "testing"

func TestIsUnifiedSymbolSpot(t *testing.T) {
	if !isUnifiedSymbol("BTC/USDT", "spot") {
		t.Fatal("expected BTC/USDT to be valid unified spot symbol")
	}
	if isUnifiedSymbol("BTCUSDT", "spot") {
		t.Fatal("expected BTCUSDT to be rejected for spot")
	}
}

func TestIsUnifiedSymbolSwap(t *testing.T) {
	if !isUnifiedSymbol("BTC/USDT:USDT", "swap") {
		t.Fatal("expected BTC/USDT:USDT to be valid unified swap symbol")
	}
	if isUnifiedSymbol("BTCUSDT", "swap") {
		t.Fatal("expected BTCUSDT to be rejected for swap")
	}
	if isUnifiedSymbol("BTC/USDT", "swap") {
		t.Fatal("expected spot-style symbol to be rejected for swap")
	}
}
