package broker

import "testing"

func TestCapabilityForExchangeSupportsHuobiAlias(t *testing.T) {
	htx, ok := capabilityForExchange("htx")
	if !ok {
		t.Fatal("expected htx capability entry")
	}

	huobi, ok := capabilityForExchange("huobi")
	if !ok {
		t.Fatal("expected huobi alias capability entry")
	}

	if htx.Exchange != "htx" {
		t.Fatalf("expected canonical exchange htx, got %q", htx.Exchange)
	}
	if huobi.Exchange != "htx" {
		t.Fatalf("expected huobi alias to resolve to htx, got %q", huobi.Exchange)
	}
}

func TestCapabilitiesCatalogIncludesMexcNativeChannelParameters(t *testing.T) {
	item, ok := capabilityForExchange("mexc_native")
	if !ok {
		t.Fatal("expected mexc_native capability entry")
	}

	if item.Exchange != "mexc" {
		t.Fatalf("expected logical exchange mexc, got %q", item.Exchange)
	}
	if item.ManagerExchange != "mexc_native" {
		t.Fatalf("expected manager exchange mexc_native, got %q", item.ManagerExchange)
	}
	if item.Adapter != "native" {
		t.Fatalf("expected adapter native, got %q", item.Adapter)
	}

	spotChannels := item.Channels["spot"]
	if len(spotChannels) == 0 {
		t.Fatalf("expected spot channels for mexc_native, got %+v", item.Channels)
	}

	for _, dataType := range []string{"trades", "orderbooks"} {
		channel, ok := spotChannels[dataType]
		if !ok {
			t.Fatalf("expected %s channel for mexc_native spot, got %+v", dataType, spotChannels)
		}
		if dataType == "orderbooks" {
			depthParam, ok := channel.Parameters["depth"]
			if !ok {
				t.Fatalf("expected depth parameter for orderbooks, got %+v", channel.Parameters)
			}
			if depthParam.Type != "int" || depthParam.Default != 5 {
				t.Fatalf("unexpected depth parameter metadata: %+v", depthParam)
			}
			if len(depthParam.AllowedValues) != 3 || depthParam.AllowedValues[0] != 5 || depthParam.AllowedValues[1] != 10 || depthParam.AllowedValues[2] != 20 {
				t.Fatalf("unexpected depth allowed values: %+v", depthParam.AllowedValues)
			}
		}
		param, ok := channel.Parameters["push_interval_ms"]
		if !ok {
			t.Fatalf("expected push_interval_ms parameter for %s, got %+v", dataType, channel.Parameters)
		}
		if param.Type != "int" {
			t.Fatalf("expected int parameter type, got %+v", param)
		}
		if param.Required {
			t.Fatalf("expected optional push_interval_ms, got %+v", param)
		}
		if param.Default != 100 {
			t.Fatalf("expected default 100, got %+v", param.Default)
		}
		if param.Min != 10 || param.Max != 100 || param.Step != 10 {
			t.Fatalf("unexpected range metadata: %+v", param)
		}
		if len(param.AllowedValues) != 2 || param.AllowedValues[0] != 10 || param.AllowedValues[1] != 100 {
			t.Fatalf("unexpected allowed values: %+v", param.AllowedValues)
		}
	}
}

func TestCapabilitiesCatalogIncludesBybitNativeDepthParameter(t *testing.T) {
	item, ok := capabilityForExchange("bybit_native")
	if !ok {
		t.Fatal("expected bybit_native capability entry")
	}

	channel, ok := item.Channels["spot"]["orderbooks"]
	if !ok {
		t.Fatalf("expected bybit_native spot orderbooks channel, got %+v", item.Channels)
	}
	depthParam, ok := channel.Parameters["depth"]
	if !ok {
		t.Fatalf("expected depth parameter for bybit_native orderbooks, got %+v", channel.Parameters)
	}
	if depthParam.Type != "int" {
		t.Fatalf("expected int depth parameter, got %+v", depthParam)
	}
	want := []int{1, 50, 200, 1000}
	if len(depthParam.AllowedValues) != len(want) {
		t.Fatalf("unexpected depth allowed values: %+v", depthParam.AllowedValues)
	}
	for i, depth := range want {
		if depthParam.AllowedValues[i] != depth {
			t.Fatalf("expected depth %d at index %d, got %+v", depth, i, depthParam.AllowedValues)
		}
	}
}

func TestCapabilitiesCatalogIncludesWaveOneExchanges(t *testing.T) {
	required := map[string]bool{
		"mexc":    false,
		"kucoin":  false,
		"htx":     false,
		"bitmart": false,
	}

	for _, item := range capabilitiesCatalog() {
		if _, ok := required[item.Exchange]; ok {
			required[item.Exchange] = true
		}
	}

	for exchange, found := range required {
		if !found {
			t.Fatalf("expected capabilities catalog to include %s", exchange)
		}
	}
}

func TestCapabilitiesCatalogIncludesUpdatedBybitDepths(t *testing.T) {
	bybitNative, ok := capabilityForExchange("bybit_native")
	if !ok {
		t.Fatal("expected bybit_native capability entry")
	}

	want := []int{1, 50, 200, 1000}
	if len(bybitNative.OrderBookDepths) != len(want) {
		t.Fatalf("unexpected bybit depth count: %+v", bybitNative.OrderBookDepths)
	}
	for i, depth := range want {
		if bybitNative.OrderBookDepths[i] != depth {
			t.Fatalf("expected bybit depth %d at index %d, got %+v", depth, i, bybitNative.OrderBookDepths)
		}
	}
}

func TestCapabilitiesCatalogDisablesBybitCCXTOrderBookUnwatch(t *testing.T) {
	bybit, ok := capabilityForExchange("bybit")
	if !ok {
		t.Fatal("expected bybit capability entry")
	}
	if bybit.SupportsOrderBookUnwatch {
		t.Fatalf("expected bybit ccxt orderbook unwatch disabled, got %+v", bybit)
	}
	if bybit.SupportsOrderBookBatchUnwatch {
		t.Fatalf("expected bybit ccxt batch orderbook unwatch disabled, got %+v", bybit)
	}
}

func TestCapabilitiesCatalogAdvertisesCCXTTradeCacheSupport(t *testing.T) {
	required := []string{"binance", "bitget", "bitmart", "bybit", "coinex", "htx", "kucoin", "mexc", "woo", "ccxt_default"}

	for _, exchange := range required {
		item, ok := capabilityForExchange(exchange)
		if !ok {
			t.Fatalf("expected capability entry for %s", exchange)
		}
		if !item.SupportsCacheN {
			t.Fatalf("expected SupportsCacheN for %s, got %+v", exchange, item)
		}
	}
}
