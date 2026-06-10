package bybit

import "testing"

func TestBuildKlineTopicNormalizesIntervals(t *testing.T) {
	tests := []struct {
		name     string
		interval string
		want     string
	}{
		{name: "default", interval: "", want: "kline.1.BTCUSDT"},
		{name: "minutes", interval: "5m", want: "kline.5.BTCUSDT"},
		{name: "hour", interval: "1h", want: "kline.60.BTCUSDT"},
		{name: "day", interval: "1d", want: "kline.D.BTCUSDT"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := BuildKlineTopic("BTCUSDT", tt.interval)
			if err != nil {
				t.Fatalf("BuildKlineTopic returned error: %v", err)
			}
			if got != tt.want {
				t.Fatalf("expected topic %q, got %q", tt.want, got)
			}
		})
	}
}

func TestBuildKlineTopicRejectsInvalidInterval(t *testing.T) {
	if _, err := BuildKlineTopic("BTCUSDT", "2m"); err == nil {
		t.Fatal("expected invalid interval error")
	}
}

func TestNormalizeOHLCV(t *testing.T) {
	update, err := NormalizeOHLCV("kline.5.BTCUSDT", wsKlineData{
		Start:    1710000000000,
		Interval: "5",
		Open:     "100.5",
		High:     "102.25",
		Low:      "99.75",
		Close:    "101.5",
		Volume:   "12.34",
		Turnover: "1234.56",
		Confirm:  true,
	}, "swap", 1710000000123, 1710000000123000000)
	if err != nil {
		t.Fatalf("NormalizeOHLCV returned error: %v", err)
	}

	if update.Exchange != "bybit" || update.Symbol != "BTC/USDT:USDT" || update.MarketType != "swap" {
		t.Fatalf("unexpected routing fields: %+v", update)
	}
	if update.DataType != "ohlcv" || update.Interval != "5m" || !update.Confirm {
		t.Fatalf("unexpected ohlcv metadata: %+v", update)
	}
	if update.Timestamp != 1710000000000 || update.GoTimestamp != 1710000000123 || update.IngestUnixNano != 1710000000123000000 {
		t.Fatalf("unexpected timestamps: %+v", update)
	}
	if update.Open != 100.5 || update.High != 102.25 || update.Low != 99.75 || update.Close != 101.5 || update.Volume != 12.34 || update.Turnover != 1234.56 {
		t.Fatalf("unexpected prices/volume: %+v", update)
	}
}
