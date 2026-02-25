//go:build ccxt
// +build ccxt

package ccxt

import (
	"fmt"
	"log"
	"sync"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

type CCXTExchange struct {
	mu            sync.Mutex
	tradeDataCh   chan<- *shared_types.TradeUpdate
	obDataCh      chan<- *shared_types.OrderBookUpdate // NEU: Kanal für Orderbücher
	batchManagers map[string]*ConnectionManager
}

// GEÄNDERT: Die Signatur des Konstruktors wird um den Orderbuch-Kanal erweitert.
func NewCCXTExchange(tradeDataCh chan<- *shared_types.TradeUpdate, obDataCh chan<- *shared_types.OrderBookUpdate) exchanges.Exchange {
	return &CCXTExchange{
		tradeDataCh:   tradeDataCh,
		obDataCh:      obDataCh,
		batchManagers: make(map[string]*ConnectionManager),
	}
}

func (e *CCXTExchange) HandleRequest(req *shared_types.ClientRequest) {
	e.mu.Lock()
	defer e.mu.Unlock()

	config := getConfig(req.Exchange, req.MarketType)
	if !config.Enabled {
		log.Printf("[CCXT-ROUTER] Ignoriere Anfrage für deaktivierte Börse/Markt: %s/%s", req.Exchange, req.MarketType)
		return
	}

	managerID := fmt.Sprintf("%s-%s", req.Exchange, req.MarketType)
	manager, exists := e.batchManagers[managerID]
	if !exists {
		log.Printf("[CCXT-ROUTER] Erstelle neuen Connection Manager für %s", managerID)
		// GEÄNDERT: Der ConnectionManager wird jetzt mit beiden Kanälen erstellt.
		manager = NewConnectionManager(req.Exchange, req.MarketType, config, e.tradeDataCh, e.obDataCh)
		e.batchManagers[managerID] = manager
		go manager.Run()
	}

	var managerAction string
	switch req.Action {
	case "subscribe":
		managerAction = "add"
	case "unsubscribe":
		managerAction = "remove"
	default:
		return
	}
	
	// GEÄNDERT: Die ManagerCommand enthält jetzt den DataType, damit der
	// ConnectionManager weiß, welche Art von Worker er verwenden muss.
	manager.commandCh <- ManagerCommand{
		Action:   managerAction,
		Symbol:   req.Symbol,
		DataType: req.DataType,
	}
}

func (e *CCXTExchange) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	var wg sync.WaitGroup
	log.Println("[CCXT-ROUTER] Stoppe alle Connection Manager...")
	for _, manager := range e.batchManagers {
		wg.Add(1)
		go manager.Stop(&wg)
	}
	wg.Wait()
	log.Println("[CCXT-ROUTER] Alle Connection Manager gestoppt.")
}
