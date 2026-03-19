package mexc

import (
	"testing"

	"bybit-watcher/internal/exchanges/mexc/protoc"
)

func TestTranslateSymbolRoundTripUSDT(t *testing.T) {
	exchangeSymbol := TranslateSymbolToExchange("BTC/USDT")
	if exchangeSymbol != "BTCUSDT" {
		t.Fatalf("expected BTCUSDT, got %s", exchangeSymbol)
	}

	ccxtSymbol := TranslateSymbolFromExchange(exchangeSymbol)
	if ccxtSymbol != "BTC/USDT" {
		t.Fatalf("expected BTC/USDT, got %s", ccxtSymbol)
	}
}

func TestNormalizeTradeFromProto(t *testing.T) {
	trade := &protoc.PublicAggreDealsV3ApiItem{
		Price:     "93220.00",
		Quantity:  "0.04438243",
		TradeType: 2,
		Time:      1736409765051,
	}

	out, err := NormalizeTrade("BTCUSDT", trade, 111, 222)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out.Exchange != "mexc" {
		t.Fatalf("expected exchange mexc, got %s", out.Exchange)
	}
	if out.Symbol != "BTC/USDT" {
		t.Fatalf("expected BTC/USDT, got %s", out.Symbol)
	}
	if out.Side != "sell" {
		t.Fatalf("expected sell, got %s", out.Side)
	}
}
