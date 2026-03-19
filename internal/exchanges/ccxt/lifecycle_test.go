//go:build ccxt
// +build ccxt

package ccxt

import "testing"

func TestFeatureHardDisabledBlocksBybitOrderBookUnwatch(t *testing.T) {
	if !featureHardDisabled("bybit", "unWatchOrderBook") {
		t.Fatal("expected bybit single orderbook unwatch to be hard-disabled")
	}
	if !featureHardDisabled("bybit", "unWatchOrderBookForSymbols") {
		t.Fatal("expected bybit batch orderbook unwatch to be hard-disabled")
	}
}

func TestFeatureHardDisabledLeavesOtherBybitFeaturesUntouched(t *testing.T) {
	if featureHardDisabled("bybit", "unWatchTradesForSymbols") {
		t.Fatal("expected bybit trade unwatch features to remain available")
	}
}

func TestFeatureHardDisabledBlocksBinanceTradeUnwatch(t *testing.T) {
	if !featureHardDisabled("binance", "unWatchTrades") {
		t.Fatal("expected binance single trade unwatch to be hard-disabled")
	}
	if !featureHardDisabled("binance", "unWatchTradesForSymbols") {
		t.Fatal("expected binance batch trade unwatch to be hard-disabled")
	}
}

func TestFeatureHardDisabledBlocksMEXCGlobally(t *testing.T) {
	if !featureHardDisabled("mexc", "unWatchTrades") {
		t.Fatal("expected mexc features to stay hard-disabled")
	}
}

func TestEffectiveCCXTExchangeNameCanonicalizesHuobiToHtx(t *testing.T) {
	if got := effectiveCCXTExchangeName("huobi"); got != "htx" {
		t.Fatalf("expected huobi to canonicalize to htx, got %q", got)
	}
}

func TestEffectiveCCXTExchangeNameLeavesCanonicalNamesUntouched(t *testing.T) {
	if got := effectiveCCXTExchangeName("htx"); got != "htx" {
		t.Fatalf("expected htx to remain htx, got %q", got)
	}
}
