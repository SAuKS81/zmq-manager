//go:build ccxt
// +build ccxt

package ccxt

import "strings"

func makeExchangeOptions(exchangeName, marketType string) map[string]interface{} {
	options := map[string]interface{}{
		"options": map[string]interface{}{
			"defaultType": marketType,
		},
	}

	// Binance can panic on strict checksum validation in watchOrderBook streams.
	// Disable checksum to keep long-running broker processes stable.
	if strings.HasPrefix(strings.ToLower(exchangeName), "binance") {
		options["options"].(map[string]interface{})["watchOrderBook"] = map[string]interface{}{
			"checksum": false,
		}
	}

	return options
}
