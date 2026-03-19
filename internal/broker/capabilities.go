package broker

import (
	"sort"
	"time"

	"bybit-watcher/internal/shared_types"
)

const runtimeTotalsTickInterval = 5 * time.Second

func capabilitiesCatalog() []shared_types.CapabilitiesItem {
	mexcPushInterval := map[string]map[string]map[string]shared_types.CapabilityParameter{
		"spot": {
			"trades": {
				"push_interval_ms": {
					Type:          "int",
					Required:      false,
					Default:       100,
					Min:           10,
					Max:           100,
					Step:          10,
					AllowedValues: []int{10, 100},
				},
			},
			"orderbooks": {
				"push_interval_ms": {
					Type:          "int",
					Required:      false,
					Default:       100,
					Min:           10,
					Max:           100,
					Step:          10,
					AllowedValues: []int{10, 100},
				},
			},
		},
	}

	return []shared_types.CapabilitiesItem{
		newCapabilityItem("binance", "binance_native", "native", []string{"spot", "swap"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			orderBookDepths:     []int{5, 10, 20},
			supportsRequestID:   true,
			supportsDeployQueue: true,
		}),
		newCapabilityItem("bybit", "bybit_native", "native", []string{"spot", "swap"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			orderBookDepths:     []int{1, 50, 200, 1000},
			supportsRequestID:   true,
			supportsDeployQueue: true,
		}),
		newCapabilityItem("bitget", "bitget_native", "native", []string{"spot", "swap"}, []string{"trades"}, nil, capabilityFlags{
			supportsRequestID:   true,
			supportsDeployQueue: true,
		}),
		newCapabilityItem("kucoin", "kucoin_native", "native", []string{"spot"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			orderBookDepths:     []int{1, 5, 50},
			supportsRequestID:   true,
			supportsDeployQueue: true,
		}),
		newCapabilityItem("mexc", "mexc_native", "native", []string{"spot"}, []string{"trades", "orderbooks"}, mexcPushInterval, capabilityFlags{
			orderBookDepths:     []int{5, 10, 20},
			supportsRequestID:   true,
			supportsDeployQueue: true,
		}),
		newCapabilityItem("binance", "binance", "ccxt", []string{"spot", "swap"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			orderBookDepths:               []int{5, 20},
			usesBatchSymbols:              true,
			supportsTradeUnwatch:          true,
			supportsTradeBatchUnwatch:     true,
			supportsOrderBookUnwatch:      true,
			supportsOrderBookBatchUnwatch: true,
			supportsCacheN:                true,
			supportsRequestID:             true,
			supportsDeployQueue:           true,
		}),
		newCapabilityItem("bitget", "bitget", "ccxt", []string{"spot", "swap"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			usesBatchSymbols:          true,
			supportsTradeBatchUnwatch: true,
			supportsCacheN:            true,
			supportsRequestID:         true,
			supportsDeployQueue:       true,
		}),
		newCapabilityItem("bitmart", "bitmart", "ccxt", []string{"spot"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			supportsCacheN:      true,
			supportsRequestID:   true,
			supportsDeployQueue: true,
		}),
		newCapabilityItem("bybit", "bybit", "ccxt", []string{"spot", "swap"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			orderBookDepths:           []int{1, 50, 200, 1000},
			usesBatchSymbols:          true,
			supportsTradeBatchUnwatch: true,
			supportsCacheN:            true,
			supportsRequestID:         true,
			supportsDeployQueue:       true,
		}),
		newCapabilityItem("coinex", "coinex", "ccxt", []string{"spot"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			usesBatchSymbols:    true,
			supportsCacheN:      true,
			supportsRequestID:   true,
			supportsDeployQueue: true,
		}),
		newCapabilityItem("htx", "htx", "ccxt", []string{"spot", "swap"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			supportsTradeUnwatch: true,
			supportsCacheN:       true,
			supportsRequestID:    true,
			supportsDeployQueue:  true,
		}),
		newCapabilityItem("huobi", "huobi", "ccxt", []string{"spot", "swap"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			supportsTradeUnwatch: true,
			supportsCacheN:       true,
			supportsRequestID:    true,
			supportsDeployQueue:  true,
		}),
		newCapabilityItem("kucoin", "kucoin", "ccxt", []string{"spot"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			usesBatchSymbols:          true,
			supportsTradeUnwatch:      true,
			supportsTradeBatchUnwatch: true,
			supportsCacheN:            true,
			supportsRequestID:         true,
			supportsDeployQueue:       true,
		}),
		newCapabilityItem("mexc", "mexc", "ccxt", []string{"spot"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			supportsCacheN:      true,
			supportsRequestID:   true,
			supportsDeployQueue: true,
		}),
		newCapabilityItem("woo", "woo", "ccxt", []string{"spot", "swap"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			supportsTradeUnwatch: true,
			supportsCacheN:       true,
			supportsRequestID:    true,
			supportsDeployQueue:  true,
		}),
		newCapabilityItem("ccxt_default", "ccxt_default", "ccxt", []string{"spot", "swap"}, []string{"trades", "orderbooks"}, nil, capabilityFlags{
			supportsCacheN:      true,
			supportsRequestID:   true,
			supportsDeployQueue: true,
		}),
	}
}

type capabilityFlags struct {
	orderBookDepths               []int
	usesBatchSymbols              bool
	supportsTradeUnwatch          bool
	supportsTradeBatchUnwatch     bool
	supportsOrderBookUnwatch      bool
	supportsOrderBookBatchUnwatch bool
	supportsCacheN                bool
	supportsRequestID             bool
	supportsDeployQueue           bool
}

func newCapabilityItem(
	exchange string,
	managerExchange string,
	adapter string,
	marketTypes []string,
	dataTypes []string,
	parameters map[string]map[string]map[string]shared_types.CapabilityParameter,
	flags capabilityFlags,
) shared_types.CapabilitiesItem {
	return shared_types.CapabilitiesItem{
		Exchange:                      canonicalCapabilityExchange(exchange),
		ManagerExchange:               managerExchange,
		Adapter:                       adapter,
		MarketTypes:                   append([]string(nil), marketTypes...),
		DataTypes:                     append([]string(nil), dataTypes...),
		Channels:                      buildChannelCapabilities(marketTypes, dataTypes, flags.orderBookDepths, flags.supportsRequestID, parameters),
		OrderBookDepths:               append([]int(nil), flags.orderBookDepths...),
		UsesBatchSymbols:              flags.usesBatchSymbols,
		SupportsTradeUnwatch:          flags.supportsTradeUnwatch,
		SupportsTradeBatchUnwatch:     flags.supportsTradeBatchUnwatch,
		SupportsOrderBookUnwatch:      flags.supportsOrderBookUnwatch,
		SupportsOrderBookBatchUnwatch: flags.supportsOrderBookBatchUnwatch,
		SupportsCacheN:                flags.supportsCacheN,
		SupportsRequestID:             flags.supportsRequestID,
		SupportsDeployQueue:           flags.supportsDeployQueue,
	}
}

func buildChannelCapabilities(
	marketTypes []string,
	dataTypes []string,
	orderBookDepths []int,
	supportsRequestID bool,
	parameters map[string]map[string]map[string]shared_types.CapabilityParameter,
) map[string]map[string]shared_types.ChannelCapability {
	channels := make(map[string]map[string]shared_types.ChannelCapability, len(marketTypes))
	for _, marketType := range marketTypes {
		channelByType := make(map[string]shared_types.ChannelCapability, len(dataTypes))
		for _, dataType := range dataTypes {
			channelCapability := shared_types.ChannelCapability{
				Subscribe:         true,
				Unsubscribe:       true,
				BulkSubscribe:     true,
				BulkUnsubscribe:   true,
				SupportsRequestID: supportsRequestID,
			}
			paramMap := make(map[string]shared_types.CapabilityParameter)
			if dataType == "orderbooks" && len(orderBookDepths) > 0 {
				paramMap["depth"] = shared_types.CapabilityParameter{
					Type:          "int",
					Required:      false,
					Default:       orderBookDepths[0],
					Min:           orderBookDepths[0],
					Max:           orderBookDepths[len(orderBookDepths)-1],
					AllowedValues: append([]int(nil), orderBookDepths...),
				}
			}
			if marketParams, ok := parameters[marketType]; ok {
				if dataTypeParams, ok := marketParams[dataType]; ok && len(dataTypeParams) > 0 {
					for key, value := range cloneCapabilityParameterMap(dataTypeParams) {
						paramMap[key] = value
					}
				}
			}
			if len(paramMap) > 0 {
				channelCapability.Parameters = paramMap
			}
			channelByType[dataType] = channelCapability
		}
		channels[marketType] = channelByType
	}
	return channels
}

func cloneCapabilityParameterMap(input map[string]shared_types.CapabilityParameter) map[string]shared_types.CapabilityParameter {
	if len(input) == 0 {
		return nil
	}
	out := make(map[string]shared_types.CapabilityParameter, len(input))
	for key, value := range input {
		cloned := value
		if len(value.AllowedValues) > 0 {
			cloned.AllowedValues = append([]int(nil), value.AllowedValues...)
		}
		out[key] = cloned
	}
	return out
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
		if item.ManagerExchange == exchange {
			return item, true
		}
		if item.Exchange == exchange && item.ManagerExchange == exchange {
			return item, true
		}
	}
	return shared_types.CapabilitiesItem{}, false
}

func buildCapabilitiesSnapshotResponse(requestID string) *shared_types.CapabilitiesSnapshotResponse {
	items := capabilitiesCatalog()

	sort.Slice(items, func(i, j int) bool {
		if items[i].Exchange != items[j].Exchange {
			return items[i].Exchange < items[j].Exchange
		}
		if items[i].Adapter != items[j].Adapter {
			return items[i].Adapter < items[j].Adapter
		}
		return items[i].ManagerExchange < items[j].ManagerExchange
	})

	return &shared_types.CapabilitiesSnapshotResponse{
		Type:      "capabilities_snapshot",
		RequestID: requestID,
		TS:        time.Now().UnixMilli(),
		Items:     items,
	}
}
