package bybit

import (
	"log"
	"sync"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

// BybitExchange verwaltet jetzt Manager für Trades UND Orderbücher.
type BybitExchange struct {
	mu         sync.Mutex
	spotTradeMgr *ConnectionManager
	swapTradeMgr *ConnectionManager
	spotOBMgr    *OrderBookConnectionManager // NEU
	swapOBMgr    *OrderBookConnectionManager // NEU
	
	requestCh  chan<- *shared_types.ClientRequest
	tradeDataCh chan<- *shared_types.TradeUpdate
	obDataCh    chan<- *shared_types.OrderBookUpdate // NEU
}

// NewBybitExchange akzeptiert jetzt auch einen Orderbuch-Kanal.
func NewBybitExchange(requestCh chan<- *shared_types.ClientRequest, tradeDataCh chan<- *shared_types.TradeUpdate, obDataCh chan<- *shared_types.OrderBookUpdate) exchanges.Exchange {
	return &BybitExchange{
		requestCh:   requestCh,
		tradeDataCh: tradeDataCh,
		obDataCh:    obDataCh,
	}
}

// HandleRequest leitet die Anfrage an den korrekten Manager weiter.
func (e *BybitExchange) HandleRequest(req *shared_types.ClientRequest) {
	e.mu.Lock()
	defer e.mu.Unlock()

	exchangeSymbol := TranslateSymbolToExchange(req.Symbol)
	var managerAction string
	switch req.Action {
	case "subscribe": managerAction = "add"
	case "unsubscribe": managerAction = "remove"
	default: return
	}

	cmd := ManagerCommand{
		Action: managerAction,
		Symbol: exchangeSymbol,
		Depth:  req.OrderBookDepth, // Tiefe weitergeben
	}

	// Route basierend auf Datentyp und Markt
	if req.DataType == "orderbooks" {
		switch req.MarketType {
		case "spot":
			if e.spotOBMgr == nil {
				log.Println("[BYBIT-EXCHANGE] Erster Spot-OrderBook-Abonnent. Starte Manager.")
				e.spotOBMgr = NewOrderBookConnectionManager(spotWsURL, "spot", e.obDataCh)
				go e.spotOBMgr.Run()
			}
			e.spotOBMgr.commandCh <- cmd
		case "swap":
			if e.swapOBMgr == nil {
				log.Println("[BYBIT-EXCHANGE] Erster Swap-OrderBook-Abonnent. Starte Manager.")
				e.swapOBMgr = NewOrderBookConnectionManager(linearWsURL, "swap", e.obDataCh)
				go e.swapOBMgr.Run()
			}
			e.swapOBMgr.commandCh <- cmd
		}
	} else { // Fallback auf Trades
		switch req.MarketType {
		case "spot":
			if e.spotTradeMgr == nil {
				log.Println("[BYBIT-EXCHANGE] Erster Spot-Trade-Abonnent. Starte Manager.")
				e.spotTradeMgr = NewConnectionManager(spotWsURL, "spot", e.tradeDataCh)
				go e.spotTradeMgr.Run()
			}
			e.spotTradeMgr.commandCh <- cmd
		case "swap":
			if e.swapTradeMgr == nil {
				log.Println("[BYBIT-EXCHANGE] Erster Swap-Trade-Abonnent. Starte Manager.")
				e.swapTradeMgr = NewConnectionManager(linearWsURL, "swap", e.tradeDataCh)
				go e.swapTradeMgr.Run()
			}
			e.swapTradeMgr.commandCh <- cmd
		}
	}
}

func (e *BybitExchange) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.spotTradeMgr != nil { e.spotTradeMgr.Stop() }
	if e.swapTradeMgr != nil { e.swapTradeMgr.Stop() }
	if e.spotOBMgr != nil { e.spotOBMgr.Stop() }
	if e.swapOBMgr != nil { e.swapOBMgr.Stop() }
}