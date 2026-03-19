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

func TestFeatureHardDisabledBlocksBatchWatchPathsGlobally(t *testing.T) {
	if !featureHardDisabled("binance", "watchTradesForSymbols") {
		t.Fatal("expected batch trade watch path to be hard-disabled globally")
	}
	if !featureHardDisabled("kucoin", "watchOrderBookForSymbols") {
		t.Fatal("expected batch orderbook watch path to be hard-disabled globally")
	}
}
