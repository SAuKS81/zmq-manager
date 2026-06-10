package bybit

import (
	"bybit-watcher/internal/shared_types"
	"fmt"
	"strconv"
	"strings"
)

var bybitKlineIntervals = map[string]string{
	"1m":  "1",
	"3m":  "3",
	"5m":  "5",
	"15m": "15",
	"30m": "30",
	"1h":  "60",
	"4h":  "240",
	"1d":  "D",
}

var bybitKlineTopicToInterval = map[string]string{
	"1":   "1m",
	"3":   "3m",
	"5":   "5m",
	"15":  "15m",
	"30":  "30m",
	"60":  "1h",
	"240": "4h",
	"D":   "1d",
}

func TranslateSymbolToExchange(ccxtSymbol string) string {
	s := strings.Split(ccxtSymbol, ":")[0]
	s = strings.ReplaceAll(s, "/", "")
	s = strings.ReplaceAll(s, "-", "")
	return s
}

func TranslateSymbolFromExchange(bybitSymbol, marketType string) string {
	var base string
	if strings.HasSuffix(bybitSymbol, "USDT") {
		base = strings.TrimSuffix(bybitSymbol, "USDT")
	} else {
		base = bybitSymbol
	}
	ccxtBase := base + "/USDT"
	if marketType == "swap" {
		return ccxtBase + ":USDT"
	}
	return ccxtBase
}

func NormalizeKlineInterval(interval string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(interval))
	if normalized == "" {
		normalized = "1m"
	}
	topicInterval, ok := bybitKlineIntervals[normalized]
	return topicInterval, ok
}

func NormalizeUserKlineInterval(interval string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(interval))
	if normalized == "" {
		normalized = "1m"
	}
	_, ok := bybitKlineIntervals[normalized]
	return normalized, ok
}

func BuildKlineTopic(symbol, interval string) (string, error) {
	topicInterval, ok := NormalizeKlineInterval(interval)
	if !ok {
		return "", fmt.Errorf("unsupported interval: %s", interval)
	}
	return "kline." + topicInterval + "." + symbol, nil
}

func ParseKlineTopic(topic string, fallbackMarketType string) (symbol string, interval string, err error) {
	parts := strings.Split(topic, ".")
	if len(parts) != 3 || parts[0] != "kline" {
		return "", "", fmt.Errorf("invalid kline topic: %s", topic)
	}
	interval, ok := bybitKlineTopicToInterval[parts[1]]
	if !ok {
		return "", "", fmt.Errorf("unsupported bybit kline interval: %s", parts[1])
	}
	return parts[2], interval, nil
}

func NormalizeTrade(trade wsTrade, marketType string, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil {
		return nil, err
	}
	amount, err := strconv.ParseFloat(trade.Volume, 64)
	if err != nil {
		return nil, err
	}

	return &shared_types.TradeUpdate{
		Exchange:       "bybit",
		Symbol:         TranslateSymbolFromExchange(trade.Symbol, marketType),
		MarketType:     marketType,
		Timestamp:      trade.Timestamp,
		GoTimestamp:    goTimestamp,
		IngestUnixNano: ingestUnixNano,
		Price:          price,
		Amount:         amount,
		Side:           strings.ToLower(trade.Side),
		TradeID:        trade.TradeID,
	}, nil
}

func NormalizeOHLCV(topic string, candle wsKlineData, marketType string, goTimestamp int64, ingestUnixNano int64) (*shared_types.OHLCVUpdate, error) {
	exchangeSymbol, interval, err := ParseKlineTopic(topic, marketType)
	if err != nil {
		return nil, err
	}
	if candle.Interval != "" {
		if mapped, ok := bybitKlineTopicToInterval[candle.Interval]; ok {
			interval = mapped
		}
	}
	open, err := strconv.ParseFloat(candle.Open, 64)
	if err != nil {
		return nil, err
	}
	high, err := strconv.ParseFloat(candle.High, 64)
	if err != nil {
		return nil, err
	}
	low, err := strconv.ParseFloat(candle.Low, 64)
	if err != nil {
		return nil, err
	}
	closePrice, err := strconv.ParseFloat(candle.Close, 64)
	if err != nil {
		return nil, err
	}
	volume, err := strconv.ParseFloat(candle.Volume, 64)
	if err != nil {
		return nil, err
	}
	turnover := 0.0
	if candle.Turnover != "" {
		if turnover, err = strconv.ParseFloat(candle.Turnover, 64); err != nil {
			return nil, err
		}
	}

	return &shared_types.OHLCVUpdate{
		Exchange:       "bybit",
		Symbol:         TranslateSymbolFromExchange(exchangeSymbol, marketType),
		MarketType:     marketType,
		Interval:       interval,
		Timestamp:      candle.Start,
		GoTimestamp:    goTimestamp,
		IngestUnixNano: ingestUnixNano,
		Open:           open,
		High:           high,
		Low:            low,
		Close:          closePrice,
		Volume:         volume,
		Turnover:       turnover,
		Confirm:        candle.Confirm,
		DataType:       "ohlcv",
	}, nil
}
