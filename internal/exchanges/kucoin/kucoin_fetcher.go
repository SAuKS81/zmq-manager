package kucoin

import (
	"log"
	"sync"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

type KucoinExchange struct {
	mu        sync.Mutex
	spotMgr   *ConnectionManager
	spotOBMgr *OrderBookConnectionManager
	requestCh chan<- *shared_types.ClientRequest
	dataCh    chan<- *shared_types.TradeUpdate
	obDataCh  chan<- *shared_types.OrderBookUpdate
	statusCh  chan<- *shared_types.StreamStatusEvent
}

func NewKucoinExchange(
	requestCh chan<- *shared_types.ClientRequest,
	dataCh chan<- *shared_types.TradeUpdate,
	obDataCh chan<- *shared_types.OrderBookUpdate,
	statusCh chan<- *shared_types.StreamStatusEvent,
) exchanges.Exchange {
	return &KucoinExchange{
		requestCh: requestCh,
		dataCh:    dataCh,
		obDataCh:  obDataCh,
		statusCh:  statusCh,
	}
}

func (e *KucoinExchange) HandleRequest(req *shared_types.ClientRequest) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if req.MarketType != "spot" {
		log.Printf("[KUCOIN-EXCHANGE] Ignoriere nicht unterstuetzten Markt-Typ: %s", req.MarketType)
		return
	}
	if req.DataType != "" && req.DataType != "trades" && req.DataType != "orderbooks" {
		log.Printf("[KUCOIN-EXCHANGE] Ignoriere nicht unterstuetzten Daten-Typ: %s", req.DataType)
		return
	}

	exchangeSymbol := TranslateSymbolToExchange(req.Symbol)

	var managerAction string
	switch req.Action {
	case "subscribe":
		managerAction = "add"
	case "unsubscribe":
		managerAction = "remove"
	default:
		return
	}

	if req.DataType == "orderbooks" {
		if e.spotOBMgr == nil {
			e.spotOBMgr = NewOrderBookConnectionManager("spot", e.obDataCh, e.statusCh)
			go e.spotOBMgr.Run()
		}
		e.spotOBMgr.commandCh <- ManagerCommand{
			Action: managerAction,
			Symbol: exchangeSymbol,
			Depth:  normalizeOrderBookDepth(req.OrderBookDepth),
		}
		return
	}

	if e.spotMgr == nil {
		e.spotMgr = NewConnectionManager("spot", e.dataCh, e.statusCh)
		go e.spotMgr.Run()
	}

	e.spotMgr.commandCh <- ManagerCommand{
		Action: managerAction,
		Symbol: exchangeSymbol,
	}
}

func (e *KucoinExchange) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.spotMgr != nil {
		e.spotMgr.Stop()
	}
	if e.spotOBMgr != nil {
		e.spotOBMgr.Stop()
	}
}
