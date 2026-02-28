//go:build !ccxt
// +build !ccxt

package ccxt

import (
	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

type disabledExchange struct{}

func (d *disabledExchange) HandleRequest(req *shared_types.ClientRequest) {}
func (d *disabledExchange) Stop()                                         {}

func NewCCXTExchange(tradeDataCh chan<- *shared_types.TradeUpdate, obDataCh chan<- *shared_types.OrderBookUpdate) exchanges.Exchange {
	return &disabledExchange{}
}
