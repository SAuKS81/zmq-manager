package broker

import (
	"regexp"
	"strings"
)

var (
	unifiedSpotSymbolPattern = regexp.MustCompile(`^[A-Za-z0-9]+/[A-Za-z0-9]+$`)
	unifiedSwapSymbolPattern = regexp.MustCompile(`^[A-Za-z0-9]+/[A-Za-z0-9]+:[A-Za-z0-9]+$`)
)

func isUnifiedSymbol(symbol, marketType string) bool {
	symbol = strings.TrimSpace(symbol)
	if symbol == "" {
		return false
	}

	switch strings.ToLower(strings.TrimSpace(marketType)) {
	case "spot":
		return unifiedSpotSymbolPattern.MatchString(symbol)
	case "swap":
		return unifiedSwapSymbolPattern.MatchString(symbol)
	default:
		return unifiedSpotSymbolPattern.MatchString(symbol) || unifiedSwapSymbolPattern.MatchString(symbol)
	}
}

func unifiedSymbolFormatHint(marketType string) string {
	switch strings.ToLower(strings.TrimSpace(marketType)) {
	case "spot":
		return "expected CCXT unified spot symbol like BTC/USDT"
	case "swap":
		return "expected CCXT unified swap symbol like BTC/USDT:USDT"
	default:
		return "expected CCXT unified symbol like BTC/USDT or BTC/USDT:USDT"
	}
}
