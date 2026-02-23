package binance

import (
	"log"
	"sync"

	"bybit-watcher/internal/exchanges"
	"bybit-watcher/internal/shared_types"
)

type BinanceExchange struct {
	mu           sync.Mutex
	spotTradeMgr *ConnectionManager
	swapTradeMgr *ConnectionManager
	spotOBMgr    *OrderBookConnectionManager // Separate Struktur wie bei Bybit
	swapOBMgr    *OrderBookConnectionManager
	
	requestCh    chan<- *shared_types.ClientRequest
	tradeDataCh  chan<- *shared_types.TradeUpdate
	obDataCh     chan<- *shared_types.OrderBookUpdate
}

func NewBinanceExchange(requestCh chan<- *shared_types.ClientRequest, tradeDataCh chan<- *shared_types.TradeUpdate, obDataCh chan<- *shared_types.OrderBookUpdate) exchanges.Exchange {
	return &BinanceExchange{
		requestCh:   requestCh,
		tradeDataCh: tradeDataCh,
		obDataCh:    obDataCh,
	}
}

func (e *BinanceExchange) HandleRequest(req *shared_types.ClientRequest) {
	e.mu.Lock()
	defer e.mu.Unlock()

	exchangeSymbol := TranslateSymbolToExchange(req.Symbol)
	var managerAction string
	switch req.Action {
	case "subscribe": managerAction = "add"
	case "unsubscribe": managerAction = "remove"
	default: return
	}

	// Route basierend auf Datentyp
	if req.DataType == "orderbooks" {
		// OrderBook Command mit Depth
		cmd := OBManagerCommand{
			Action: managerAction,
			Symbol: exchangeSymbol,
			Depth:  req.OrderBookDepth,
		}
		if cmd.Depth == 0 { cmd.Depth = 20 } // Default

		switch req.MarketType {
		case "spot":
			if e.spotOBMgr == nil {
				log.Println("[BINANCE-EXCHANGE] Starte Spot OrderBook Manager.")
				e.spotOBMgr = NewOrderBookConnectionManager(spotWsURL, "spot", spotSymbolsPerShard, e.obDataCh)
				go e.spotOBMgr.Run()
			}
			e.spotOBMgr.commandCh <- cmd
		case "swap":
			if e.swapOBMgr == nil {
				log.Println("[BINANCE-EXCHANGE] Starte Swap OrderBook Manager.")
				e.swapOBMgr = NewOrderBookConnectionManager(futuresWsURL, "swap", swapSymbolsPerShard, e.obDataCh)
				go e.swapOBMgr.Run()
			}
			e.swapOBMgr.commandCh <- cmd
		}
	} else {
		// Trade Command (einfacher)
		cmd := ManagerCommand{
			Action: managerAction,
			Symbol: exchangeSymbol,
		}
		
		switch req.MarketType {
		case "spot":
			if e.spotTradeMgr == nil {
				log.Println("[BINANCE-EXCHANGE] Starte Spot Trade Manager.")
				e.spotTradeMgr = NewConnectionManager(spotWsURL, "spot", spotSymbolsPerShard, e.tradeDataCh)
				go e.spotTradeMgr.Run()
			}
			e.spotTradeMgr.commandCh <- cmd
		case "swap":
			if e.swapTradeMgr == nil {
				log.Println("[BINANCE-EXCHANGE] Starte Swap Trade Manager.")
				e.swapTradeMgr = NewConnectionManager(futuresWsURL, "swap", swapSymbolsPerShard, e.tradeDataCh)
				go e.swapTradeMgr.Run()
			}
			e.swapTradeMgr.commandCh <- cmd
		}
	}
}

func (e *BinanceExchange) Stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.spotTradeMgr != nil { e.spotTradeMgr.Stop() }
	if e.swapTradeMgr != nil { e.swapTradeMgr.Stop() }
	if e.spotOBMgr != nil { e.spotOBMgr.Stop() }
	if e.swapOBMgr != nil { e.swapOBMgr.Stop() }
}