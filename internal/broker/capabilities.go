package broker

import (
	"sort"
	"time"

	"bybit-watcher/internal/shared_types"
)

const runtimeTotalsTickInterval = 5 * time.Second

func capabilitiesCatalog() []shared_types.CapabilitiesItem {
	return []shared_types.CapabilitiesItem{
		{
			Exchange:            "binance_native",
			Adapter:             "native",
			MarketTypes:         []string{"spot", "swap"},
			DataTypes:           []string{"trades", "orderbooks"},
			OrderBookDepths:     []int{5, 10, 20},
			SupportsCacheN:      false,
			SupportsRequestID:   true,
			SupportsDeployQueue: true,
		},
		{
			Exchange:            "bybit_native",
			Adapter:             "native",
			MarketTypes:         []string{"spot", "swap"},
			DataTypes:           []string{"trades", "orderbooks"},
			OrderBookDepths:     []int{1, 50, 200, 1000},
			SupportsCacheN:      false,
			SupportsRequestID:   true,
			SupportsDeployQueue: true,
		},
		{
			Exchange:            "bitget_native",
			Adapter:             "native",
			MarketTypes:         []string{"spot", "swap"},
			DataTypes:           []string{"trades"},
			SupportsCacheN:      false,
			SupportsRequestID:   true,
			SupportsDeployQueue: true,
		},
		{
			Exchange:                      "binance",
			Adapter:                       "ccxt",
			MarketTypes:                   []string{"spot", "swap"},
			DataTypes:                     []string{"trades", "orderbooks"},
			OrderBookDepths:               []int{5, 20},
			UsesBatchSymbols:              true,
			SupportsTradeUnwatch:          true,
			SupportsTradeBatchUnwatch:     true,
			SupportsOrderBookUnwatch:      true,
			SupportsOrderBookBatchUnwatch: true,
			SupportsCacheN:                true,
			SupportsRequestID:             true,
			SupportsDeployQueue:           true,
		},
		{
			Exchange:                  "bitget",
			Adapter:                   "ccxt",
			MarketTypes:               []string{"spot", "swap"},
			DataTypes:                 []string{"trades", "orderbooks"},
			UsesBatchSymbols:          true,
			SupportsTradeBatchUnwatch: true,
			SupportsCacheN:            true,
			SupportsRequestID:         true,
			SupportsDeployQueue:       true,
		},
		{
			Exchange:            "bitmart",
			Adapter:             "ccxt",
			MarketTypes:         []string{"spot"},
			DataTypes:           []string{"trades", "orderbooks"},
			SupportsCacheN:      true,
			SupportsRequestID:   true,
			SupportsDeployQueue: true,
		},
		{
			Exchange:                  "bybit",
			Adapter:                   "ccxt",
			MarketTypes:               []string{"spot", "swap"},
			DataTypes:                 []string{"trades", "orderbooks"},
			OrderBookDepths:           []int{1, 50, 200, 1000},
			UsesBatchSymbols:          true,
			SupportsTradeBatchUnwatch: true,
			SupportsCacheN:            true,
			SupportsRequestID:         true,
			SupportsDeployQueue:       true,
		},
		{
			Exchange:            "coinex",
			Adapter:             "ccxt",
			MarketTypes:         []string{"spot"},
			DataTypes:           []string{"trades", "orderbooks"},
			UsesBatchSymbols:    true,
			SupportsCacheN:      true,
			SupportsRequestID:   true,
			SupportsDeployQueue: true,
		},
		{
			Exchange:             "htx",
			Adapter:              "ccxt",
			MarketTypes:          []string{"spot", "swap"},
			DataTypes:            []string{"trades", "orderbooks"},
			SupportsTradeUnwatch: true,
			SupportsCacheN:       true,
			SupportsRequestID:    true,
			SupportsDeployQueue:  true,
		},
		{
			Exchange:             "huobi",
			Adapter:              "ccxt",
			MarketTypes:          []string{"spot", "swap"},
			DataTypes:            []string{"trades", "orderbooks"},
			SupportsTradeUnwatch: true,
			SupportsCacheN:       true,
			SupportsRequestID:    true,
			SupportsDeployQueue:  true,
		},
		{
			Exchange:                  "kucoin",
			Adapter:                   "ccxt",
			MarketTypes:               []string{"spot"},
			DataTypes:                 []string{"trades", "orderbooks"},
			UsesBatchSymbols:          true,
			SupportsTradeUnwatch:      true,
			SupportsTradeBatchUnwatch: true,
			SupportsCacheN:            true,
			SupportsRequestID:         true,
			SupportsDeployQueue:       true,
		},
		{
			Exchange:            "mexc",
			Adapter:             "ccxt",
			MarketTypes:         []string{"spot"},
			DataTypes:           []string{"trades", "orderbooks"},
			SupportsCacheN:      true,
			SupportsRequestID:   true,
			SupportsDeployQueue: true,
		},
		{
			Exchange:             "woo",
			Adapter:              "ccxt",
			MarketTypes:          []string{"spot", "swap"},
			DataTypes:            []string{"trades", "orderbooks"},
			SupportsTradeUnwatch: true,
			SupportsCacheN:       true,
			SupportsRequestID:    true,
			SupportsDeployQueue:  true,
		},
		{
			Exchange:            "ccxt_default",
			Adapter:             "ccxt",
			MarketTypes:         []string{"spot", "swap"},
			DataTypes:           []string{"trades", "orderbooks"},
			SupportsCacheN:      true,
			SupportsRequestID:   true,
			SupportsDeployQueue: true,
		},
	}
}

func canonicalCapabilityExchange(exchange string) string {
	switch exchange {
	case "huobi":
		return "htx"
	default:
		return exchange
	}
}

func capabilityForExchange(exchange string) (shared_types.CapabilitiesItem, bool) {
	exchange = canonicalCapabilityExchange(exchange)
	for _, item := range capabilitiesCatalog() {
		if item.Exchange == exchange {
			return item, true
		}
	}
	return shared_types.CapabilitiesItem{}, false
}

func buildCapabilitiesSnapshotResponse(requestID string) *shared_types.CapabilitiesSnapshotResponse {
	items := capabilitiesCatalog()

	sort.Slice(items, func(i, j int) bool {
		if items[i].Adapter != items[j].Adapter {
			return items[i].Adapter < items[j].Adapter
		}
		return items[i].Exchange < items[j].Exchange
	})

	return &shared_types.CapabilitiesSnapshotResponse{
		Type:      "capabilities_snapshot",
		RequestID: requestID,
		TS:        time.Now().UnixMilli(),
		Items:     items,
	}
}
