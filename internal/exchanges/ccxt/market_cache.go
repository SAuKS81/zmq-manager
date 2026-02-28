//go:build ccxt
// +build ccxt

package ccxt

import (
	"fmt"
	"log"
	"runtime/debug"
	"strings"
	"sync"

	ccxt "github.com/ccxt/ccxt/go/v4"
	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
)

var supportedSymbolsCache sync.Map

func supportedSymbolsCacheKey(exchangeName, marketType string) string {
	return strings.ToLower(exchangeName) + ":" + strings.ToLower(marketType)
}

func getSupportedSymbols(exchangeName, marketType string) (map[string]struct{}, error) {
	key := supportedSymbolsCacheKey(exchangeName, marketType)
	if cached, ok := supportedSymbolsCache.Load(key); ok {
		return cached.(map[string]struct{}), nil
	}

	supported, err := loadSupportedSymbols(exchangeName, marketType)
	if err != nil {
		return nil, err
	}
	actual, _ := supportedSymbolsCache.LoadOrStore(key, supported)
	return actual.(map[string]struct{}), nil
}

func loadSupportedSymbols(exchangeName, marketType string) (map[string]struct{}, error) {
	exchange := createCCXTExchange(exchangeName, marketType)
	if exchange == nil {
		return nil, fmt.Errorf("create exchange failed for %s/%s", exchangeName, marketType)
	}
	defer closeCCXTExchange(exchangeName, marketType, exchange)

	markets, err := safeLoadMarkets(exchange)
	if err != nil {
		return nil, err
	}

	supported := make(map[string]struct{}, len(markets))
	for symbol := range markets {
		supported[symbol] = struct{}{}
	}
	log.Printf("[CCXT-MARKETS] cached %d supported symbols for %s/%s", len(supported), exchangeName, marketType)
	return supported, nil
}

func safeLoadMarkets(exchange ccxtpro.IExchange) (markets map[string]ccxt.MarketInterface, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in LoadMarkets: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return exchange.LoadMarkets()
}
