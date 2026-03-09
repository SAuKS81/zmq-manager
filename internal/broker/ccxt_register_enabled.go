//go:build ccxt
// +build ccxt

package broker

import "bybit-watcher/internal/exchanges/ccxt"

func registerCCXT(sm *SubscriptionManager) {
	sm.exchangeRegistry["ccxt_generic"] = ccxt.NewCCXTExchange(sm.TradeDataCh, sm.OrderBookCh, sm.StatusCh)
}
