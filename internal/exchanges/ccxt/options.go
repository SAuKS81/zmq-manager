//go:build ccxt
// +build ccxt

package ccxt

import "strings"

func makeExchangeOptions(exchangeName, marketType string, tradeLimit int) map[string]interface{} {
	ccxtOptions := map[string]interface{}{
		"defaultType": marketType,
	}
	if tradeLimit > 0 {
		ccxtOptions["tradesLimit"] = tradeLimit
	}
	options := map[string]interface{}{
		"options": ccxtOptions,
	}

	// Binance can panic on strict checksum validation in watchOrderBook streams.
	// Disable checksum to keep long-running broker processes stable.
	if strings.HasPrefix(strings.ToLower(exchangeName), "binance") {
		ccxtOptions["watchOrderBook"] = map[string]interface{}{
			"checksum": false,
		}
	}

	return options
}
