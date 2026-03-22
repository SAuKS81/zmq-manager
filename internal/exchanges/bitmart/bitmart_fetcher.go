package bitmart

import (
	"log"
	"sync"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

type BitmartExchange struct {
	mu        sync.Mutex
	spotMgr   *ConnectionManager
	spotOBMgr *OrderBookConnectionManager
	requestCh chan<- *shared_types.ClientRequest
	dataCh    chan<- *shared_types.TradeUpdate
	obDataCh  chan<- *shared_types.OrderBookUpdate
	statusCh  chan<- *shared_types.StreamStatusEvent
}

func NewBitmartExchange(
	requestCh chan<- *shared_types.ClientRequest,
	dataCh chan<- *shared_types.TradeUpdate,
	obDataCh chan<- *shared_types.OrderBookUpdate,
	statusCh chan<- *shared_types.StreamStatusEvent,
) exchanges.Exchange {
	return &BitmartExchange{
		requestCh: requestCh,
		dataCh:    dataCh,
		obDataCh:  obDataCh,
		statusCh:  statusCh,
	}
}

func (e *BitmartExchange) HandleRequest(req *shared_types.ClientRequest) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if req.MarketType != "spot" {
		log.Printf("[BITMART-EXCHANGE] Ignoriere nicht unterstuetzten Markt-Typ: %s", req.MarketType)
		return
	}
	if req.DataType != "" && req.DataType != "trades" && req.DataType != "orderbooks" {
		log.Printf("[BITMART-EXCHANGE] Ignoriere nicht unterstuetzten Daten-Typ: %s", req.DataType)
		return
	}
	exchangeSymbol := TranslateSymbolToExchange(req.Symbol)
	action := ""
	switch req.Action {
	case "subscribe":
		action = "add"
	case "unsubscribe":
		action = "remove"
	default:
		return
	}

	if req.DataType == "orderbooks" {
		if e.spotOBMgr == nil {
			e.spotOBMgr = NewOrderBookConnectionManager(e.obDataCh, e.statusCh)
			go e.spotOBMgr.Run()
		}
		e.spotOBMgr.commandCh <- ManagerCommand{
			Action: action,
			Symbol: exchangeSymbol,
			Depth:  NormalizeOrderBookDepth(req.OrderBookDepth),
			Mode:   NormalizeOrderBookMode(req.OrderBookMode),
		}
		return
	}

	if e.spotMgr == nil {
		e.spotMgr = NewConnectionManager(e.dataCh, e.statusCh)
		go e.spotMgr.Run()
	}
	e.spotMgr.commandCh <- ManagerCommand{Action: action, Symbol: exchangeSymbol}
}

func (e *BitmartExchange) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.spotMgr != nil {
		e.spotMgr.Stop()
	}
	if e.spotOBMgr != nil {
		e.spotOBMgr.Stop()
	}
}
