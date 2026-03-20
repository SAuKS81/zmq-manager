package coinex

import "testing"

func TestTranslateCoinexSymbols(t *testing.T) {
	if got := TranslateSymbolToExchange("BTC/USDT"); got != "BTCUSDT" {
		t.Fatalf("expected BTCUSDT, got %s", got)
	}
	if got := TranslateSymbolFromExchange("BTCUSDT", "spot"); got != "BTC/USDT" {
		t.Fatalf("expected BTC/USDT, got %s", got)
	}
	if got := TranslateSymbolFromExchange("BTCUSDT", "swap"); got != "BTC/USDT:USDT" {
		t.Fatalf("expected BTC/USDT:USDT, got %s", got)
	}
}

func TestNormalizeCoinexOrderBook(t *testing.T) {
	update, err := NormalizeOrderBook("spot", "BTCUSDT", wsDepthBooks{
		Bids:      [][]string{{"100.0", "1.5"}},
		Asks:      [][]string{{"100.5", "2.5"}},
		UpdatedAt: 123456789,
	}, 123456790, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if update.Symbol != "BTC/USDT" {
		t.Fatalf("expected BTC/USDT, got %s", update.Symbol)
	}
	if len(update.Bids) != 1 || len(update.Asks) != 1 {
		t.Fatalf("expected 1 bid and 1 ask")
	}
	if update.Bids[0].Price != 100.0 || update.Bids[0].Amount != 1.5 {
		t.Fatalf("unexpected bid %+v", update.Bids[0])
	}
	if update.Asks[0].Price != 100.5 || update.Asks[0].Amount != 2.5 {
		t.Fatalf("unexpected ask %+v", update.Asks[0])
	}
}

func TestNormalizeCoinexOrderBookDepth(t *testing.T) {
	if got := NormalizeOrderBookDepth(5); got != 5 {
		t.Fatalf("expected 5, got %d", got)
	}
	if got := NormalizeOrderBookDepth(13); got != 50 {
		t.Fatalf("expected fallback 50, got %d", got)
	}
}
