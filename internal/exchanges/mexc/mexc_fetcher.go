package mexc

import (
	"log"
	"sync"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

type MexcExchange struct {
	mu        sync.Mutex
	spotMgr   *ConnectionManager
	spotOBMgr *OrderBookConnectionManager
	requestCh chan<- *shared_types.ClientRequest
	dataCh    chan<- *shared_types.TradeUpdate
	obDataCh  chan<- *shared_types.OrderBookUpdate
	statusCh  chan<- *shared_types.StreamStatusEvent
}

func NewMexcExchange(
	requestCh chan<- *shared_types.ClientRequest,
	dataCh chan<- *shared_types.TradeUpdate,
	obDataCh chan<- *shared_types.OrderBookUpdate,
	statusCh chan<- *shared_types.StreamStatusEvent,
) exchanges.Exchange {
	return &MexcExchange{
		requestCh: requestCh,
		dataCh:    dataCh,
		obDataCh:  obDataCh,
		statusCh:  statusCh,
	}
}

func (e *MexcExchange) HandleRequest(req *shared_types.ClientRequest) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if req.MarketType != "spot" {
		log.Printf("[MEXC-EXCHANGE] Ignoriere nicht unterstuetzten Markt-Typ: %s", req.MarketType)
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

	switch req.DataType {
	case "", "trades":
		if e.spotMgr == nil {
			e.spotMgr = NewConnectionManager("spot", e.dataCh, e.statusCh)
			go e.spotMgr.Run()
		}
		freq := normalizePushIntervalMS(req.PushIntervalMS, req.OrderBookFreq)
		e.spotMgr.commandCh <- ManagerCommand{
			Action: managerAction,
			Symbol: exchangeSymbol,
			Freq:   freq,
		}
	case "orderbooks":
		if e.spotOBMgr == nil {
			e.spotOBMgr = NewOrderBookConnectionManager("spot", e.obDataCh, e.statusCh)
			go e.spotOBMgr.Run()
		}
		freq := normalizePushIntervalMS(req.PushIntervalMS, req.OrderBookFreq)
		e.spotOBMgr.commandCh <- ManagerCommand{
			Action: managerAction,
			Symbol: exchangeSymbol,
			Depth:  normalizeOrderBookDepth(req.OrderBookDepth),
			Freq:   freq,
		}
	default:
		return
	}
}

func (e *MexcExchange) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.spotMgr != nil {
		e.spotMgr.Stop()
	}
	if e.spotOBMgr != nil {
		e.spotOBMgr.Stop()
	}
}
