package woo

import (
	"log"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

type WooExchange struct {
	spotMgr *ConnectionManager
	swapMgr *ConnectionManager

	requestCh chan<- *shared_types.ClientRequest
	dataCh    chan<- *shared_types.TradeUpdate
	statusCh  chan<- *shared_types.StreamStatusEvent
}

func NewWooExchange(requestCh chan<- *shared_types.ClientRequest, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent) exchanges.Exchange {
	return &WooExchange{
		requestCh: requestCh,
		dataCh:    dataCh,
		statusCh:  statusCh,
	}
}

func (e *WooExchange) HandleRequest(req *shared_types.ClientRequest) {
	if req == nil || req.DataType != "trades" {
		return
	}
	action := "remove"
	if req.Action == "subscribe" {
		action = "add"
	}
	exchangeSymbol := TranslateSymbolToExchange(req.Symbol, req.MarketType)
	if exchangeSymbol == "" {
		e.emitFailure(req, "invalid_symbol")
		return
	}
	cmd := managerCommand{Action: action, Symbol: exchangeSymbol}
	switch req.MarketType {
	case "spot":
		if e.spotMgr == nil {
			log.Println("[WOO-EXCHANGE] Starte Spot Trade Manager.")
			e.spotMgr = NewConnectionManager(publicWsURL(), "spot", e.dataCh, e.statusCh)
			go e.spotMgr.Run()
		}
		e.spotMgr.commandCh <- cmd
	case "swap":
		if e.swapMgr == nil {
			log.Println("[WOO-EXCHANGE] Starte Swap Trade Manager.")
			e.swapMgr = NewConnectionManager(publicWsURL(), "swap", e.dataCh, e.statusCh)
			go e.swapMgr.Run()
		}
		e.swapMgr.commandCh <- cmd
	default:
		e.emitFailure(req, "unsupported_market_type")
	}
}

func (e *WooExchange) Stop() {
	if e.spotMgr != nil {
		e.spotMgr.Stop()
	}
	if e.swapMgr != nil {
		e.swapMgr.Stop()
	}
}

func (e *WooExchange) emitFailure(req *shared_types.ClientRequest, reason string) {
	if e.statusCh == nil || req == nil {
		return
	}
	e.statusCh <- &shared_types.StreamStatusEvent{
		Type:       "stream_subscribe_failed",
		Exchange:   "woo",
		MarketType: req.MarketType,
		DataType:   "trades",
		Symbol:     req.Symbol,
		Reason:     reason,
		RequestID:  req.RequestID,
	}
}
