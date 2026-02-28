package bybit

import (
	"log"
	"sync"

	"bybit-watcher/internal/shared_types"
)

// OrderBookConnectionManager verwaltet Shards für einen Orderbuch-Markt-Typ.
type OrderBookConnectionManager struct {
	wsURL         string
	marketType    string
	commandCh     chan ManagerCommand // Wiederverwendet, enthält jetzt auch Depth
	stopCh        chan struct{}
	dataCh        chan<- *shared_types.OrderBookUpdate
	statusCh      chan<- *shared_types.StreamStatusEvent
	shards        []*OrderBookShardWorker
	symbolToShard map[string]*OrderBookShardWorker
	symbolDepth   map[string]int
	shardLoad     map[*OrderBookShardWorker]int
	wg            sync.WaitGroup
}

func NewOrderBookConnectionManager(wsURL, marketType string, dataCh chan<- *shared_types.OrderBookUpdate, statusCh chan<- *shared_types.StreamStatusEvent) *OrderBookConnectionManager {
	return &OrderBookConnectionManager{
		wsURL:         wsURL,
		marketType:    marketType,
		commandCh:     make(chan ManagerCommand, 100),
		stopCh:        make(chan struct{}),
		dataCh:        dataCh,
		statusCh:      statusCh,
		symbolToShard: make(map[string]*OrderBookShardWorker),
		symbolDepth:   make(map[string]int),
		shardLoad:     make(map[*OrderBookShardWorker]int),
	}
}

func (cm *OrderBookConnectionManager) Run() {
	log.Printf("[BYBIT-OB-CONN-MANAGER] Starte Manager für %s", cm.marketType)
	for {
		select {
		case cmd := <-cm.commandCh:
			if cmd.Action == "add" {
				cm.addSubscription(cmd.Symbol, cmd.Depth)
			} else if cmd.Action == "remove" {
				cm.removeSubscription(cmd.Symbol, cmd.Depth)
			}
		case <-cm.stopCh:
			log.Printf("[BYBIT-OB-CONN-MANAGER] Stoppe Manager für %s", cm.marketType)
			return
		}
	}
}

func (cm *OrderBookConnectionManager) Stop() {
	close(cm.stopCh)
}

func (cm *OrderBookConnectionManager) addSubscription(symbol string, depth int) {
	if depth <= 0 {
		depth = 20
	}
	// Finde einen Shard mit freiem Platz
	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < symbolsPerShard {
			shard.commandCh <- ManagerCommand{Action: "subscribe", Symbol: symbol, Depth: depth}
			cm.symbolToShard[symbol] = shard
			cm.symbolDepth[symbol] = depth
			cm.shardLoad[shard]++
			return
		}
	}

	// Kein freier Shard, erstelle einen neuen
	log.Printf("[BYBIT-OB-CONN-MANAGER] Erstelle neuen Shard für %s.", symbol)
	stopCh := make(chan struct{})
	newShard := NewOrderBookShardWorker(cm.wsURL, cm.marketType, stopCh, cm.dataCh, cm.statusCh, &cm.wg)
	cm.shards = append(cm.shards, newShard)
	cm.symbolToShard[symbol] = newShard
	cm.symbolDepth[symbol] = depth
	cm.shardLoad[newShard] = 1
	cm.wg.Add(1)
	go newShard.Run()

	// Sende den ersten Befehl an den neuen Shard
	newShard.commandCh <- ManagerCommand{Action: "subscribe", Symbol: symbol, Depth: depth}
}

func (cm *OrderBookConnectionManager) removeSubscription(symbol string, depth int) {
	if shard, ok := cm.symbolToShard[symbol]; ok {
		if depth <= 0 {
			depth = cm.symbolDepth[symbol]
		}
		shard.commandCh <- ManagerCommand{Action: "unsubscribe", Symbol: symbol, Depth: depth}
		delete(cm.symbolToShard, symbol)
		delete(cm.symbolDepth, symbol)
		cm.shardLoad[shard]--
	}
}
