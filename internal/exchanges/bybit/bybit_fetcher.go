package bybit

import (
	"log"
	"sync"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

// BybitExchange verwaltet jetzt Manager für Trades UND Orderbücher.
type BybitExchange struct {
	mu           sync.Mutex
	spotTradeMgr *ConnectionManager
	swapTradeMgr *ConnectionManager
	spotOBMgr    *OrderBookConnectionManager // NEU
	swapOBMgr    *OrderBookConnectionManager // NEU
	spotOHLCVMgr *OHLCVConnectionManager
	swapOHLCVMgr *OHLCVConnectionManager

	requestCh   chan<- *shared_types.ClientRequest
	tradeDataCh chan<- *shared_types.TradeUpdate
	obDataCh    chan<- *shared_types.OrderBookUpdate
	ohlcvDataCh chan<- *shared_types.OHLCVUpdate
	statusCh    chan<- *shared_types.StreamStatusEvent
}

// NewBybitExchange akzeptiert jetzt auch einen Orderbuch-Kanal.
func NewBybitExchange(requestCh chan<- *shared_types.ClientRequest, tradeDataCh chan<- *shared_types.TradeUpdate, obDataCh chan<- *shared_types.OrderBookUpdate, ohlcvDataCh chan<- *shared_types.OHLCVUpdate, statusCh chan<- *shared_types.StreamStatusEvent) exchanges.Exchange {
	return &BybitExchange{
		requestCh:   requestCh,
		tradeDataCh: tradeDataCh,
		obDataCh:    obDataCh,
		ohlcvDataCh: ohlcvDataCh,
		statusCh:    statusCh,
	}
}

// HandleRequest leitet die Anfrage an den korrekten Manager weiter.
func (e *BybitExchange) HandleRequest(req *shared_types.ClientRequest) {
	e.mu.Lock()
	defer e.mu.Unlock()

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

	cmd := ManagerCommand{
		Action:   managerAction,
		Symbol:   exchangeSymbol,
		Depth:    req.OrderBookDepth, // Tiefe weitergeben
		Interval: req.Interval,
	}

	// Route basierend auf Datentyp und Markt
	if req.DataType == "ohlcv" {
		switch req.MarketType {
		case "spot":
			if e.spotOHLCVMgr == nil {
				log.Println("[BYBIT-EXCHANGE] Erster Spot-OHLCV-Abonnent. Starte Manager.")
				e.spotOHLCVMgr = NewOHLCVConnectionManager(spotWsURL, "spot", e.ohlcvDataCh, e.statusCh)
				go e.spotOHLCVMgr.Run()
			}
			e.spotOHLCVMgr.commandCh <- cmd
		case "swap":
			if e.swapOHLCVMgr == nil {
				log.Println("[BYBIT-EXCHANGE] Erster Swap-OHLCV-Abonnent. Starte Manager.")
				e.swapOHLCVMgr = NewOHLCVConnectionManager(linearWsURL, "swap", e.ohlcvDataCh, e.statusCh)
				go e.swapOHLCVMgr.Run()
			}
			e.swapOHLCVMgr.commandCh <- cmd
		}
	} else if req.DataType == "orderbooks" {
		switch req.MarketType {
		case "spot":
			if e.spotOBMgr == nil {
				log.Println("[BYBIT-EXCHANGE] Erster Spot-OrderBook-Abonnent. Starte Manager.")
				e.spotOBMgr = NewOrderBookConnectionManager(spotWsURL, "spot", e.obDataCh, e.statusCh)
				go e.spotOBMgr.Run()
			}
			e.spotOBMgr.commandCh <- cmd
		case "swap":
			if e.swapOBMgr == nil {
				log.Println("[BYBIT-EXCHANGE] Erster Swap-OrderBook-Abonnent. Starte Manager.")
				e.swapOBMgr = NewOrderBookConnectionManager(linearWsURL, "swap", e.obDataCh, e.statusCh)
				go e.swapOBMgr.Run()
			}
			e.swapOBMgr.commandCh <- cmd
		}
	} else { // Fallback auf Trades
		switch req.MarketType {
		case "spot":
			if e.spotTradeMgr == nil {
				log.Println("[BYBIT-EXCHANGE] Erster Spot-Trade-Abonnent. Starte Manager.")
				e.spotTradeMgr = NewConnectionManager(spotWsURL, "spot", e.tradeDataCh, e.statusCh)
				go e.spotTradeMgr.Run()
			}
			e.spotTradeMgr.commandCh <- cmd
		case "swap":
			if e.swapTradeMgr == nil {
				log.Println("[BYBIT-EXCHANGE] Erster Swap-Trade-Abonnent. Starte Manager.")
				e.swapTradeMgr = NewConnectionManager(linearWsURL, "swap", e.tradeDataCh, e.statusCh)
				go e.swapTradeMgr.Run()
			}
			e.swapTradeMgr.commandCh <- cmd
		}
	}
}

func (e *BybitExchange) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.spotTradeMgr != nil {
		e.spotTradeMgr.Stop()
	}
	if e.swapTradeMgr != nil {
		e.swapTradeMgr.Stop()
	}
	if e.spotOBMgr != nil {
		e.spotOBMgr.Stop()
	}
	if e.swapOBMgr != nil {
		e.swapOBMgr.Stop()
	}
	if e.spotOHLCVMgr != nil {
		e.spotOHLCVMgr.Stop()
	}
	if e.swapOHLCVMgr != nil {
		e.swapOHLCVMgr.Stop()
	}
}
