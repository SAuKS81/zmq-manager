package bybit

import (
	"log"
	"sync"

	"bybit-watcher/internal/shared_types"
)

// OrderBookConnectionManager verwaltet Shards fuer einen Orderbuch-Markt-Typ.
type OrderBookConnectionManager struct {
	wsURL         string
	marketType    string
	commandCh     chan ManagerCommand
	stopCh        chan struct{}
	dataCh        chan<- *shared_types.OrderBookUpdate
	statusCh      chan<- *shared_types.StreamStatusEvent
	shards        []*OrderBookShardWorker
	symbolToShard map[string]*OrderBookShardWorker
	symbolDepth   map[string]int
	shardLoad     map[*OrderBookShardWorker]int
	shardStops    map[*OrderBookShardWorker]chan struct{}
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
		shards:        []*OrderBookShardWorker{},
		symbolToShard: make(map[string]*OrderBookShardWorker),
		symbolDepth:   make(map[string]int),
		shardLoad:     make(map[*OrderBookShardWorker]int),
		shardStops:    make(map[*OrderBookShardWorker]chan struct{}),
	}
}

func (cm *OrderBookConnectionManager) Run() {
	log.Printf("[BYBIT-OB-CONN-MANAGER] Starte Manager fuer %s", cm.marketType)
	for {
		select {
		case cmd := <-cm.commandCh:
			if cmd.Action == "add" {
				cm.addSubscription(cmd.Symbol, cmd.Depth)
			} else if cmd.Action == "remove" {
				cm.removeSubscription(cmd.Symbol, cmd.Depth)
			}
		case <-cm.stopCh:
			log.Printf("[BYBIT-OB-CONN-MANAGER] Stoppe Manager fuer %s", cm.marketType)
			cm.stopAllShards()
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
	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < symbolsPerShard {
			shard.commandCh <- ManagerCommand{Action: "subscribe", Symbol: symbol, Depth: depth}
			cm.symbolToShard[symbol] = shard
			cm.symbolDepth[symbol] = depth
			cm.shardLoad[shard]++
			return
		}
	}

	log.Printf("[BYBIT-OB-CONN-MANAGER] Erstelle neuen Shard fuer %s.", symbol)
	stopCh := make(chan struct{})
	newShard := NewOrderBookShardWorker(cm.wsURL, cm.marketType, stopCh, cm.dataCh, cm.statusCh, &cm.wg)
	cm.shards = append(cm.shards, newShard)
	cm.symbolToShard[symbol] = newShard
	cm.symbolDepth[symbol] = depth
	cm.shardLoad[newShard] = 1
	cm.shardStops[newShard] = stopCh
	cm.wg.Add(1)
	go newShard.Run()

	newShard.commandCh <- ManagerCommand{Action: "subscribe", Symbol: symbol, Depth: depth}
}

func (cm *OrderBookConnectionManager) removeSubscription(symbol string, depth int) {
	shard, ok := cm.symbolToShard[symbol]
	if !ok {
		return
	}

	if depth <= 0 {
		depth = cm.symbolDepth[symbol]
	}
	shard.commandCh <- ManagerCommand{Action: "unsubscribe", Symbol: symbol, Depth: depth}
	delete(cm.symbolToShard, symbol)
	delete(cm.symbolDepth, symbol)
	cm.shardLoad[shard]--
	if cm.shardLoad[shard] <= 0 {
		cm.retireShard(shard)
	}
}

func (cm *OrderBookConnectionManager) retireShard(shard *OrderBookShardWorker) {
	if cm.shardLoad[shard] < 0 {
		cm.shardLoad[shard] = 0
	}
	log.Printf("[BYBIT-OB-CONN-MANAGER] Shard ist jetzt leer (Load: %d). Entferne ihn aus dem aktiven Satz.", cm.shardLoad[shard])

	if stopCh, ok := cm.shardStops[shard]; ok {
		close(stopCh)
		delete(cm.shardStops, shard)
	}
	delete(cm.shardLoad, shard)

	filtered := cm.shards[:0]
	for _, existing := range cm.shards {
		if existing == shard {
			continue
		}
		filtered = append(filtered, existing)
	}
	cm.shards = filtered
}

func (cm *OrderBookConnectionManager) stopAllShards() {
	for shard, stopCh := range cm.shardStops {
		close(stopCh)
		delete(cm.shardStops, shard)
	}
	cm.shards = nil
	cm.symbolToShard = make(map[string]*OrderBookShardWorker)
	cm.symbolDepth = make(map[string]int)
	cm.shardLoad = make(map[*OrderBookShardWorker]int)
}
