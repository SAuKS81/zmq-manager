package bitget

import (
	"log"
	"sync"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

// BitgetExchange implementiert das Exchange-Interface für Bitget.
type BitgetExchange struct {
	mu        sync.Mutex
	spotMgr   *ConnectionManager
	swapMgr   *ConnectionManager
	requestCh chan<- *shared_types.ClientRequest
	dataCh    chan<- *shared_types.TradeUpdate
	statusCh  chan<- *shared_types.StreamStatusEvent
}

// NewBitgetExchange erstellt eine neue Instanz der Bitget-Implementierung.
func NewBitgetExchange(requestCh chan<- *shared_types.ClientRequest, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent) exchanges.Exchange {
	return &BitgetExchange{
		requestCh: requestCh,
		dataCh:    dataCh,
		statusCh:  statusCh,
	}
}

// HandleRequest verarbeitet eine Anfrage vom SubscriptionManager.
func (e *BitgetExchange) HandleRequest(req *shared_types.ClientRequest) {
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
		Action: managerAction,
		Symbol: exchangeSymbol,
	}

	switch req.MarketType {
	case "spot":
		if e.spotMgr == nil {
			log.Println("[BITGET-EXCHANGE] Erster Spot-Abonnent. Starte Spot Connection Manager.")
			e.spotMgr = NewConnectionManager("spot", e.dataCh)
			go e.spotMgr.Run()
		}
		e.spotMgr.commandCh <- cmd

	case "swap":
		if e.swapMgr == nil {
			log.Println("[BITGET-EXCHANGE] Erster Swap-Abonnent. Starte Swap Connection Manager.")
			e.swapMgr = NewConnectionManager("swap", e.dataCh)
			go e.swapMgr.Run()
		}
		e.swapMgr.commandCh <- cmd

	default:
		log.Printf("[BITGET-EXCHANGE] Unbekannter Markt-Typ in Anfrage: %s", req.MarketType)
	}
}

// Stop beendet alle laufenden Manager.
func (e *BitgetExchange) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.spotMgr != nil {
		e.spotMgr.Stop()
	}
	if e.swapMgr != nil {
		e.swapMgr.Stop()
	}
}
