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
