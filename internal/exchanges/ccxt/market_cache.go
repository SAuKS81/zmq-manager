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
var marketSnapshotCache sync.Map
var marketSnapshotMu sync.Mutex

type marketSnapshot struct {
	markets map[string]ccxt.MarketInterface
}

func supportedSymbolsCacheKey(exchangeName, marketType string) string {
	return strings.ToLower(exchangeName) + ":" + strings.ToLower(marketType)
}

func getSupportedSymbols(exchangeName, marketType string) (map[string]struct{}, error) {
	key := supportedSymbolsCacheKey(exchangeName, marketType)
	if cached, ok := supportedSymbolsCache.Load(key); ok {
		return cached.(map[string]struct{}), nil
	}

	snapshot, err := getMarketSnapshot(exchangeName, marketType)
	if err != nil {
		return nil, err
	}
	supported := buildSupportedSymbols(snapshot.markets)
	actual, _ := supportedSymbolsCache.LoadOrStore(key, supported)
	return actual.(map[string]struct{}), nil
}

func getMarketSnapshot(exchangeName, marketType string) (*marketSnapshot, error) {
	key := supportedSymbolsCacheKey(exchangeName, marketType)
	if cached, ok := marketSnapshotCache.Load(key); ok {
		return cached.(*marketSnapshot), nil
	}

	marketSnapshotMu.Lock()
	defer marketSnapshotMu.Unlock()

	if cached, ok := marketSnapshotCache.Load(key); ok {
		return cached.(*marketSnapshot), nil
	}

	snapshot, err := loadMarketSnapshot(exchangeName, marketType)
	if err != nil {
		return nil, err
	}
	actual, _ := marketSnapshotCache.LoadOrStore(key, snapshot)
	return actual.(*marketSnapshot), nil
}

func loadMarketSnapshot(exchangeName, marketType string) (*marketSnapshot, error) {
	exchange := newCCXTExchange(exchangeName, marketType)
	if exchange == nil {
		return nil, fmt.Errorf("create exchange failed for %s/%s", exchangeName, marketType)
	}
	defer closeCCXTExchange(exchangeName, marketType, exchange)

	markets, err := safeLoadMarkets(exchange)
	if err != nil {
		return nil, err
	}

	snapshot := &marketSnapshot{markets: markets}
	log.Printf("[CCXT-MARKETS] cached %d supported symbols for %s/%s", len(markets), exchangeName, marketType)
	return snapshot, nil
}

func buildSupportedSymbols(markets map[string]ccxt.MarketInterface) map[string]struct{} {
	supported := make(map[string]struct{}, len(markets))
	for symbol := range markets {
		supported[symbol] = struct{}{}
	}
	return supported
}

func safeLoadMarkets(exchange ccxtpro.IExchange) (markets map[string]ccxt.MarketInterface, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in LoadMarkets: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return exchange.LoadMarkets()
}

type marketSetter interface {
	SetMarkets(markets interface{}, optionalArgs ...interface{}) interface{}
}

func applyMarketSnapshot(exchangeName, marketType string, exchange ccxtpro.IExchange) {
	if exchange == nil {
		return
	}
	setter, ok := exchange.(marketSetter)
	if !ok {
		return
	}
	snapshot, err := getMarketSnapshot(exchangeName, marketType)
	if err != nil {
		log.Printf("[CCXT-MARKETS-WARN] unable to load shared markets for %s/%s: %v", exchangeName, marketType, err)
		return
	}
	if snapshot == nil || len(snapshot.markets) == 0 {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[CCXT-MARKETS-WARN] SetMarkets failed for %s/%s: %v", exchangeName, marketType, r)
		}
	}()
	setter.SetMarkets(snapshot.markets, nil)
}
