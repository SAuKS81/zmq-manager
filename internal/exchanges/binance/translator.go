package binance

import (
	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
	"fmt"
	"strconv"
	"strings"
	"time"
)

var binanceKlineIntervals = map[string]string{
	"1m":  "1m",
	"3m":  "3m",
	"5m":  "5m",
	"15m": "15m",
	"30m": "30m",
	"1h":  "1h",
	"4h":  "4h",
	"1d":  "1d",
}

func TranslateSymbolToExchange(ccxtSymbol string) string {
	s := strings.Split(ccxtSymbol, ":")[0]
	return strings.ToLower(strings.ReplaceAll(s, "/", ""))
}

func TranslateSymbolFromExchange(binanceSymbol, marketType string) string {
	var base, quote string
	binanceSymbol = strings.ToUpper(binanceSymbol)
	if strings.HasSuffix(binanceSymbol, "USDT") {
		base = strings.TrimSuffix(binanceSymbol, "USDT")
		quote = "USDT"
	} else if strings.HasSuffix(binanceSymbol, "USDC") {
		base = strings.TrimSuffix(binanceSymbol, "USDC")
		quote = "USDC"
	} else {
		base = binanceSymbol
		quote = ""
	}
	ccxtBase := base + "/" + quote
	if marketType == "swap" {
		return ccxtBase + ":" + quote
	}
	return ccxtBase
}

func NormalizeKlineInterval(interval string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(interval))
	if normalized == "" {
		normalized = "1m"
	}
	topicInterval, ok := binanceKlineIntervals[normalized]
	return topicInterval, ok
}

func NormalizeUserKlineInterval(interval string) (string, bool) {
	normalized := strings.ToLower(strings.TrimSpace(interval))
	if normalized == "" {
		normalized = "1m"
	}
	_, ok := binanceKlineIntervals[normalized]
	return normalized, ok
}

func BuildKlineStream(symbol, interval string) (string, error) {
	topicInterval, ok := NormalizeKlineInterval(interval)
	if !ok {
		return "", fmt.Errorf("unsupported interval: %s", interval)
	}
	return strings.ToLower(symbol) + "@kline_" + topicInterval, nil
}

func NormalizeTrade(trade wsTrade, marketType string, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil {
		return nil, err
	}
	amount, err := strconv.ParseFloat(trade.Quantity, 64)
	if err != nil {
		return nil, err
	}
	if amount == 0 {
		return nil, nil
	}

	side := "buy"
	if trade.IsBuyerMaker {
		side = "sell"
	}

	t := pools.GetTradeUpdate()
	t.Exchange = "binance"
	t.Symbol = TranslateSymbolFromExchange(trade.Symbol, marketType)
	t.MarketType = marketType
	t.Timestamp = trade.TradeTime
	t.GoTimestamp = goTimestamp
	t.IngestUnixNano = ingestUnixNano
	t.Price = price
	t.Amount = amount
	t.Side = side
	t.TradeID = strconv.FormatInt(trade.TradeID, 10)
	return t, nil
}

func NormalizeOHLCV(kline wsKline, symbolRaw, marketType string, goTimestamp int64, ingestUnixNano int64) (*shared_types.OHLCVUpdate, error) {
	data := kline.Kline
	symbol := data.Symbol
	if symbol == "" {
		symbol = kline.Symbol
	}
	if symbol == "" {
		symbol = symbolRaw
	}
	interval, ok := NormalizeUserKlineInterval(data.Interval)
	if !ok {
		return nil, fmt.Errorf("unsupported interval: %s", data.Interval)
	}
	open, err := strconv.ParseFloat(data.Open, 64)
	if err != nil {
		return nil, err
	}
	high, err := strconv.ParseFloat(data.High, 64)
	if err != nil {
		return nil, err
	}
	low, err := strconv.ParseFloat(data.Low, 64)
	if err != nil {
		return nil, err
	}
	closePrice, err := strconv.ParseFloat(data.Close, 64)
	if err != nil {
		return nil, err
	}
	volume, err := strconv.ParseFloat(data.Volume, 64)
	if err != nil {
		return nil, err
	}
	turnover := 0.0
	if data.QuoteVolume != "" {
		if turnover, err = strconv.ParseFloat(data.QuoteVolume, 64); err != nil {
			return nil, err
		}
	}

	normalized := pools.GetOHLCVUpdate()
	normalized.Exchange = "binance"
	normalized.Symbol = TranslateSymbolFromExchange(symbol, marketType)
	normalized.MarketType = marketType
	normalized.Interval = interval
	normalized.Timestamp = data.StartTime
	normalized.GoTimestamp = goTimestamp
	normalized.IngestUnixNano = ingestUnixNano
	normalized.Open = open
	normalized.High = high
	normalized.Low = low
	normalized.Close = closePrice
	normalized.Volume = volume
	normalized.Turnover = turnover
	normalized.Confirm = data.Closed
	normalized.DataType = "ohlcv"
	return normalized, nil
}

func NormalizeOrderBook(ob wsOrderBookPartial, symbolRaw, marketType string, goTimestamp int64, ingestUnixNano int64) (*shared_types.OrderBookUpdate, error) {
	normalized := pools.GetOrderBookUpdate()

	// INTELLIGENTE AUSWAHL: Spot vs Futures Felder
	var rawBids, rawAsks []wsOrderBookLevel

	if len(ob.BidsFut) > 0 || len(ob.AsksFut) > 0 {
		// Futures Format ("b", "a")
		rawBids = ob.BidsFut
		rawAsks = ob.AsksFut
	} else {
		// Spot Format ("bids", "asks")
		rawBids = ob.BidsSpot
		rawAsks = ob.AsksSpot
	}

	bids := normalized.Bids[:0]
	for _, level := range rawBids {
		if len(level) < 2 {
			continue
		}
		p, _ := strconv.ParseFloat(level[0], 64)
		a, _ := strconv.ParseFloat(level[1], 64)
		bids = append(bids, shared_types.OrderBookLevel{Price: p, Amount: a})
	}

	asks := normalized.Asks[:0]
	for _, level := range rawAsks {
		if len(level) < 2 {
			continue
		}
		p, _ := strconv.ParseFloat(level[0], 64)
		a, _ := strconv.ParseFloat(level[1], 64)
		asks = append(asks, shared_types.OrderBookLevel{Price: p, Amount: a})
	}

	normalized.Exchange = "binance"
	normalized.Symbol = TranslateSymbolFromExchange(symbolRaw, marketType)
	normalized.MarketType = marketType

	if ob.EventTime > 0 {
		normalized.Timestamp = ob.EventTime
	} else {
		normalized.Timestamp = time.Now().UnixMilli()
	}
	normalized.GoTimestamp = goTimestamp
	normalized.IngestUnixNano = ingestUnixNano
	normalized.UpdateType = "ob_update"
	normalized.Bids = bids
	normalized.Asks = asks

	return normalized, nil
}
