package gateio

import (
	"log"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

type GateioExchange struct {
	spotMgr *ConnectionManager
	swapMgr *ConnectionManager

	requestCh chan<- *shared_types.ClientRequest
	dataCh    chan<- *shared_types.TradeUpdate
	statusCh  chan<- *shared_types.StreamStatusEvent
}

func NewGateioExchange(requestCh chan<- *shared_types.ClientRequest, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent) exchanges.Exchange {
	return &GateioExchange{
		requestCh: requestCh,
		dataCh:    dataCh,
		statusCh:  statusCh,
	}
}

func (e *GateioExchange) HandleRequest(req *shared_types.ClientRequest) {
	if req == nil || req.DataType != "trades" {
		return
	}
	action := "remove"
	if req.Action == "subscribe" {
		action = "add"
	}
	cmd := managerCommand{Action: action, Symbol: TranslateSymbolToExchange(req.Symbol)}
	switch req.MarketType {
	case "spot":
		if e.spotMgr == nil {
			log.Println("[GATEIO-EXCHANGE] Starte Spot Trade Manager.")
			e.spotMgr = NewConnectionManager(spotWsURL, "spot", e.dataCh, e.statusCh)
			go e.spotMgr.Run()
		}
		e.spotMgr.commandCh <- cmd
	case "swap":
		if e.swapMgr == nil {
			log.Println("[GATEIO-EXCHANGE] Starte Swap Trade Manager.")
			e.swapMgr = NewConnectionManager(swapWsURL, "swap", e.dataCh, e.statusCh)
			go e.swapMgr.Run()
		}
		e.swapMgr.commandCh <- cmd
	default:
		if e.statusCh != nil {
			e.statusCh <- &shared_types.StreamStatusEvent{
				Type:       "stream_subscribe_failed",
				Exchange:   "gate",
				MarketType: req.MarketType,
				DataType:   "trades",
				Symbol:     req.Symbol,
				Reason:     "unsupported_market_type",
			}
		}
	}
}

func (e *GateioExchange) Stop() {
	if e.spotMgr != nil {
		e.spotMgr.Stop()
	}
	if e.swapMgr != nil {
		e.swapMgr.Stop()
	}
}
