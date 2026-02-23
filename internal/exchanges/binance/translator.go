package binance

import (
	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
	"strconv"
	"strings"
	"time"
)

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

func NormalizeTrade(trade wsTrade, marketType string, goTimestamp int64) (*shared_types.TradeUpdate, error) {
	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil { return nil, err }
	amount, err := strconv.ParseFloat(trade.Quantity, 64)
	if err != nil { return nil, err }
	if amount == 0 { return nil, nil }

	side := "buy"
	if trade.IsBuyerMaker { side = "sell" }
	
	t := pools.GetTradeUpdate()
	t.Exchange = "binance"
	t.Symbol = TranslateSymbolFromExchange(trade.Symbol, marketType)
	t.MarketType = marketType
	t.Timestamp = trade.TradeTime
	t.GoTimestamp = goTimestamp
	t.Price = price
	t.Amount = amount
	t.Side = side
	t.TradeID = strconv.FormatInt(trade.TradeID, 10)
	return t, nil
}

func NormalizeOrderBook(ob wsOrderBookPartial, symbolRaw, marketType string, goTimestamp int64) (*shared_types.OrderBookUpdate, error) {
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
		if len(level) < 2 { continue }
		p, _ := strconv.ParseFloat(level[0], 64)
		a, _ := strconv.ParseFloat(level[1], 64)
		bids = append(bids, shared_types.OrderBookLevel{Price: p, Amount: a})
	}

	asks := normalized.Asks[:0]
	for _, level := range rawAsks {
		if len(level) < 2 { continue }
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
	normalized.Bids = bids
	normalized.Asks = asks

	return normalized, nil
}