//go:build ccxt
// +build ccxt

package ccxt

import (
	"errors"
	"testing"
)

func TestExtractMissingMarketSymbolFromDirectBadSymbol(t *testing.T) {
	err := errors.New("[ccxtError]::[BadSymbol]::[bitmart does not have market symbol QRL/USDT]")
	symbol, ok := extractMissingMarketSymbol(err)
	if !ok {
		t.Fatal("expected symbol extraction to succeed")
	}
	if symbol != "QRL/USDT" {
		t.Fatalf("expected QRL/USDT, got %q", symbol)
	}
}

func TestExtractMissingMarketSymbolIgnoresSymbolsOnlyPresentInStack(t *testing.T) {
	err := errors.New("panic in WatchTradesForSymbols: panic:runtime.goexit:panic:[ccxtError]::[BadSymbol]::[bitmart request failed]\nStack trace:\ngithub.com/example.fn({ADA/USDT,ETH/USDT})")
	if symbol, ok := extractMissingMarketSymbol(err); ok {
		t.Fatalf("expected no symbol extraction, got %q", symbol)
	}
}

func TestExtractMissingMarketSymbolUsesHeadlineBeforeStackTrace(t *testing.T) {
	err := errors.New("panic in WatchTradesForSymbols: panic:runtime.goexit:panic:[ccxtError]::[BadSymbol]::[bitmart does not have market symbol NFP/USDT]\nStack trace:\ngithub.com/example.fn({ADA/USDT,ETH/USDT})")
	symbol, ok := extractMissingMarketSymbol(err)
	if !ok {
		t.Fatal("expected symbol extraction to succeed")
	}
	if symbol != "NFP/USDT" {
		t.Fatalf("expected NFP/USDT, got %q", symbol)
	}
}
