package mexc

import (
	"fmt"
	"strconv"
	"strings"

	"bybit-watcher/internal/exchanges/mexc/mexcproto"
	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
)

func TranslateSymbolToExchange(ccxtSymbol string) string {
	s := strings.Split(ccxtSymbol, ":")[0]
	return strings.ReplaceAll(s, "/", "")
}

func TranslateSymbolFromExchange(exchangeSymbol string) string {
	if strings.HasSuffix(exchangeSymbol, "USDT") {
		base := strings.TrimSuffix(exchangeSymbol, "USDT")
		return base + "/USDT"
	}
	return exchangeSymbol
}

func tradeChannel(symbol string) string {
	return tradeChannelWithFrequency(symbol, mexcDefaultStreamFreq)
}

func tradeChannelWithFrequency(symbol string, freq string) string {
	return "spot@public.aggre.deals.v3.api.pb@" + normalizeStreamFrequency(freq) + "@" + symbol
}

func aggreOrderBookChannel(symbol string, freq string) string {
	return "spot@public.aggre.depth.v3.api.pb@" + normalizeStreamFrequency(freq) + "@" + symbol
}

func normalizeOrderBookDepth(depth int) int {
	for _, supported := range validOrderBookDepths {
		if depth <= supported {
			return supported
		}
	}
	return validOrderBookDepths[len(validOrderBookDepths)-1]
}

func normalizeOrderBookFrequency(freq string) string {
	return normalizeStreamFrequency(freq)
}

func normalizeStreamFrequency(freq string) string {
	if validOrderBookFrequencies[freq] {
		return freq
	}
	return mexcDefaultStreamFreq
}

func normalizePushIntervalMS(pushIntervalMS int, fallback string) string {
	switch pushIntervalMS {
	case 10:
		return "10ms"
	case 100:
		return "100ms"
	default:
		return normalizeStreamFrequency(fallback)
	}
}

func NormalizeTrade(symbol string, trade *mexcproto.PublicAggreDealsV3ApiItem, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	return normalizeTradeWithUnifiedSymbol(TranslateSymbolFromExchange(symbol), trade, goTimestamp, ingestUnixNano)
}

func normalizeTradeWithUnifiedSymbol(unifiedSymbol string, trade *mexcproto.PublicAggreDealsV3ApiItem, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	if trade == nil {
		return nil, fmt.Errorf("trade is nil")
	}

	price, err := strconv.ParseFloat(trade.GetPrice(), 64)
	if err != nil {
		return nil, fmt.Errorf("parse price: %w", err)
	}
	amount, err := strconv.ParseFloat(trade.GetQuantity(), 64)
	if err != nil {
		return nil, fmt.Errorf("parse quantity: %w", err)
	}

	side := "sell"
	if trade.GetTradeType() == 1 {
		side = "buy"
	}

	update := pools.GetTradeUpdate()
	update.Exchange = "mexc"
	update.Symbol = unifiedSymbol
	update.MarketType = "spot"
	update.Timestamp = trade.GetTime()
	update.GoTimestamp = goTimestamp
	update.IngestUnixNano = ingestUnixNano
	update.Price = price
	update.Amount = amount
	update.Side = side
	update.TradeID = ""
	return update, nil
}
