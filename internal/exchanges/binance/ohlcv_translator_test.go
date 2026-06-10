package binance

import (
	"testing"

	"bybit-watcher/internal/pools"
)

func TestBuildKlineStream(t *testing.T) {
	stream, err := BuildKlineStream("BTCUSDT", "1m")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if stream != "btcusdt@kline_1m" {
		t.Fatalf("unexpected stream: %s", stream)
	}
}

func TestBuildKlineStreamRejectsInvalidInterval(t *testing.T) {
	if _, err := BuildKlineStream("BTCUSDT", "2m"); err == nil {
		t.Fatal("expected invalid interval error")
	}
}

func TestNormalizeOHLCVSpot(t *testing.T) {
	kline := wsKline{
		Symbol: "BTCUSDT",
		Kline: wsKlineData{
			StartTime:   1700000000000,
			Symbol:      "BTCUSDT",
			Interval:    "1m",
			Open:        "100.1",
			High:        "101.2",
			Low:         "99.9",
			Close:       "100.8",
			Volume:      "12.34",
			QuoteVolume: "1234.56",
			Closed:      true,
		},
	}

	update, err := NormalizeOHLCV(kline, "", "spot", 1700000000123, 1700000000123456789)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer pools.PutOHLCVUpdate(update)

	if update.Exchange != "binance" || update.Symbol != "BTC/USDT" || update.MarketType != "spot" {
		t.Fatalf("unexpected identity fields: %+v", update)
	}
	if update.Interval != "1m" || update.Timestamp != 1700000000000 || !update.Confirm {
		t.Fatalf("unexpected timing fields: %+v", update)
	}
	if update.Open != 100.1 || update.High != 101.2 || update.Low != 99.9 || update.Close != 100.8 {
		t.Fatalf("unexpected price fields: %+v", update)
	}
	if update.Volume != 12.34 || update.Turnover != 1234.56 || update.DataType != "ohlcv" {
		t.Fatalf("unexpected volume fields: %+v", update)
	}
}

func TestNormalizeOHLCVSwap(t *testing.T) {
	kline := wsKline{
		Kline: wsKlineData{
			StartTime: 1700000000000,
			Interval:  "5m",
			Open:      "100",
			High:      "101",
			Low:       "99",
			Close:     "100.5",
			Volume:    "2",
			Closed:    false,
		},
	}

	update, err := NormalizeOHLCV(kline, "ETHUSDT", "swap", 1700000000123, 1700000000123456789)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer pools.PutOHLCVUpdate(update)

	if update.Symbol != "ETH/USDT:USDT" || update.Interval != "5m" || update.Confirm {
		t.Fatalf("unexpected normalized swap update: %+v", update)
	}
}
