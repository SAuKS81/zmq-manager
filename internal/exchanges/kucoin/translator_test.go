package kucoin

import "testing"

func TestTranslateSymbolRoundTripSpot(t *testing.T) {
	exchangeSymbol := TranslateSymbolToExchange("BTC/USDT")
	if exchangeSymbol != "BTC-USDT" {
		t.Fatalf("expected BTC-USDT, got %q", exchangeSymbol)
	}

	unified := TranslateSymbolFromExchange(exchangeSymbol)
	if unified != "BTC/USDT" {
		t.Fatalf("expected BTC/USDT, got %q", unified)
	}
}

func TestTranslateCompactSpotSymbolFromExchange(t *testing.T) {
	unified := TranslateSymbolFromExchange("BTCUSDT")
	if unified != "BTC/USDT" {
		t.Fatalf("expected BTC/USDT, got %q", unified)
	}
}

func TestParseTradeTimestampMSHandlesNanoseconds(t *testing.T) {
	got := parseTradeTimestampMS("1729843222921000000")
	if got != 1729843222921 {
		t.Fatalf("expected nanoseconds to convert to ms, got %d", got)
	}
}

func TestNormalizeTradeParsesSpotTrade(t *testing.T) {
	trade, err := NormalizeTrade(&wsClassicTrade{
		Time:    "1729843222921000000",
		Side:    "buy",
		Price:   "67523",
		Amount:  "0.003",
		Symbol:  "BTC-USDT",
		TradeID: "11067996711960577",
	}, 123, 456)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if trade.Exchange != "kucoin" {
		t.Fatalf("expected exchange kucoin, got %q", trade.Exchange)
	}
	if trade.Symbol != "BTC/USDT" {
		t.Fatalf("expected symbol BTC/USDT, got %q", trade.Symbol)
	}
	if trade.Timestamp != 1729843222921 {
		t.Fatalf("expected timestamp 1729843222921, got %d", trade.Timestamp)
	}
	if trade.Side != "buy" {
		t.Fatalf("expected side buy, got %q", trade.Side)
	}
}
