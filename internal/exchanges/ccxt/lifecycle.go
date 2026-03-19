//go:build ccxt
// +build ccxt

package ccxt

import (
	"log"
	"strings"

	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
)

type describableExchange interface {
	Describe() interface{}
}

func effectiveCCXTExchangeName(exchangeName string) string {
	return canonicalExchangeName(exchangeName)
}

func createCCXTExchange(exchangeName, marketType string, tradeLimit ...int) ccxtpro.IExchange {
	return newCCXTExchange(exchangeName, marketType, tradeLimit...)
}

func newCCXTExchange(exchangeName, marketType string, tradeLimit ...int) ccxtpro.IExchange {
	effectiveExchangeName := effectiveCCXTExchangeName(exchangeName)
	effectiveTradeLimit := 0
	if len(tradeLimit) > 0 {
		effectiveTradeLimit = tradeLimit[0]
	}
	if effectiveTradeLimit > 0 {
		log.Printf("[CCXT-LIFECYCLE] Create exchange=%s effective_exchange=%s market_type=%s tradesLimit=%d", exchangeName, effectiveExchangeName, marketType, effectiveTradeLimit)
	}
	return ccxtpro.CreateExchange(effectiveExchangeName, makeExchangeOptions(effectiveExchangeName, marketType, effectiveTradeLimit))
}

func closeCCXTExchange(exchangeName, marketType string, exchange ccxtpro.IExchange) {
	if exchange == nil {
		return
	}
	if errs := exchange.Close(); len(errs) > 0 {
		for _, err := range errs {
			if err != nil {
				log.Printf("[CCXT-LIFECYCLE-WARN] Close exchange failed (%s/%s): %v", exchangeName, marketType, err)
			}
		}
	}
}

func exchangeHasFeature(exchangeName string, exchange ccxtpro.IExchange, feature string) bool {
	if exchange == nil || feature == "" {
		return false
	}
	if featureHardDisabled(exchangeName, feature) {
		return false
	}

	describer, ok := exchange.(describableExchange)
	if !ok {
		return false
	}

	describe := describer.Describe()
	describeMap, ok := describe.(map[string]interface{})
	if !ok {
		return false
	}

	hasRaw, ok := describeMap["has"]
	if !ok {
		return false
	}

	hasMap, ok := hasRaw.(map[string]interface{})
	if !ok {
		return false
	}

	flag, ok := hasMap[feature]
	if !ok {
		return false
	}

	boolFlag, ok := flag.(bool)
	return ok && boolFlag
}

func featureHardDisabled(exchangeName, feature string) bool {
	if strings.EqualFold(exchangeName, "binance") {
		switch feature {
		case "unWatchTrades", "unWatchTradesForSymbols":
			return true
		}
	}
	if strings.EqualFold(exchangeName, "mexc") {
		return true
	}
	if strings.EqualFold(exchangeName, "bybit") {
		switch feature {
		case "unWatchOrderBook", "unWatchOrderBookForSymbols":
			return true
		}
	}
	return false
}
