package htx

import (
	"log"
	"sync"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

type HtxExchange struct {
	mu        sync.Mutex
	spotMgr   *ConnectionManager
	swapMgr   *ConnectionManager
	spotOBMgr *OrderBookConnectionManager
	swapOBMgr *OrderBookConnectionManager
	requestCh chan<- *shared_types.ClientRequest
	dataCh    chan<- *shared_types.TradeUpdate
	obDataCh  chan<- *shared_types.OrderBookUpdate
	statusCh  chan<- *shared_types.StreamStatusEvent
}

func NewHtxExchange(
	requestCh chan<- *shared_types.ClientRequest,
	dataCh chan<- *shared_types.TradeUpdate,
	obDataCh chan<- *shared_types.OrderBookUpdate,
	statusCh chan<- *shared_types.StreamStatusEvent,
) exchanges.Exchange {
	return &HtxExchange{
		requestCh: requestCh,
		dataCh:    dataCh,
		obDataCh:  obDataCh,
		statusCh:  statusCh,
	}
}

func (e *HtxExchange) HandleRequest(req *shared_types.ClientRequest) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if req.DataType != "" && req.DataType != "trades" && req.DataType != "orderbooks" {
		log.Printf("[HTX-EXCHANGE] Ignoriere nicht unterstuetzten Daten-Typ: %s", req.DataType)
		return
	}

	exchangeSymbol := TranslateSymbolToExchange(req.Symbol, req.MarketType)
	var managerAction string
	switch req.Action {
	case "subscribe":
		managerAction = "add"
	case "unsubscribe":
		managerAction = "remove"
	default:
		return
	}

	cmd := ManagerCommand{Action: managerAction, Symbol: exchangeSymbol}
	switch req.MarketType {
	case "spot":
		if req.DataType == "orderbooks" {
			cmd.Depth = normalizeOrderBookDepth(req.OrderBookDepth)
			if e.spotOBMgr == nil {
				e.spotOBMgr = NewOrderBookConnectionManager(wsSpotURL, "spot", e.obDataCh, e.statusCh)
				go e.spotOBMgr.Run()
			}
			e.spotOBMgr.commandCh <- cmd
			return
		}
		if e.spotMgr == nil {
			e.spotMgr = NewConnectionManager(wsSpotURL, "spot", e.dataCh, e.statusCh)
			go e.spotMgr.Run()
		}
		e.spotMgr.commandCh <- cmd
	case "swap":
		if req.DataType == "orderbooks" {
			cmd.Depth = normalizeOrderBookDepth(req.OrderBookDepth)
			if e.swapOBMgr == nil {
				e.swapOBMgr = NewOrderBookConnectionManager(wsSwapURL, "swap", e.obDataCh, e.statusCh)
				go e.swapOBMgr.Run()
			}
			e.swapOBMgr.commandCh <- cmd
			return
		}
		if e.swapMgr == nil {
			e.swapMgr = NewConnectionManager(wsSwapURL, "swap", e.dataCh, e.statusCh)
			go e.swapMgr.Run()
		}
		e.swapMgr.commandCh <- cmd
	default:
		log.Printf("[HTX-EXCHANGE] Ignoriere nicht unterstuetzten Markt-Typ: %s", req.MarketType)
	}
}

func (e *HtxExchange) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.spotMgr != nil {
		e.spotMgr.Stop()
	}
	if e.swapMgr != nil {
		e.swapMgr.Stop()
	}
	if e.spotOBMgr != nil {
		e.spotOBMgr.Stop()
	}
	if e.swapOBMgr != nil {
		e.swapOBMgr.Stop()
	}
}
