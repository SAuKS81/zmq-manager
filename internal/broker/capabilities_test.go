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
