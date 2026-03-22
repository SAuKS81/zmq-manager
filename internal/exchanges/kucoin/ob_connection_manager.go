package kucoin

import (
	"sync"

	"bybit-watcher/internal/shared_types"
)

type OrderBookConnectionManager struct {
	marketType string
	commandCh  chan ManagerCommand
	stopCh     chan struct{}
	dataCh     chan<- *shared_types.OrderBookUpdate
	statusCh   chan<- *shared_types.StreamStatusEvent

	shards        []*OrderBookShardWorker
	symbolToShard map[string]*OrderBookShardWorker
	symbolDepth   map[string]int
	shardStops    map[*OrderBookShardWorker]chan struct{}
	shardLoad     map[*OrderBookShardWorker]int
	wg            sync.WaitGroup
}

func NewOrderBookConnectionManager(marketType string, dataCh chan<- *shared_types.OrderBookUpdate, statusCh chan<- *shared_types.StreamStatusEvent) *OrderBookConnectionManager {
	return &OrderBookConnectionManager{
		marketType:    marketType,
		commandCh:     make(chan ManagerCommand, 100),
		stopCh:        make(chan struct{}),
		dataCh:        dataCh,
		statusCh:      statusCh,
		symbolToShard: make(map[string]*OrderBookShardWorker),
		symbolDepth:   make(map[string]int),
		shardStops:    make(map[*OrderBookShardWorker]chan struct{}),
		shardLoad:     make(map[*OrderBookShardWorker]int),
	}
}

func (cm *OrderBookConnectionManager) Run() {
	for {
		select {
		case cmd := <-cm.commandCh:
			switch cmd.Action {
			case "add":
				cm.addSubscription(cmd.Symbol, cmd.Depth)
			case "remove":
				cm.removeSubscription(cmd.Symbol)
			}
		case <-cm.stopCh:
			cm.stopAllShards()
			return
		}
	}
}

func (cm *OrderBookConnectionManager) Stop() {
	close(cm.stopCh)
}

func (cm *OrderBookConnectionManager) addSubscription(symbol string, depth int) {
	depth = normalizeOrderBookDepth(depth)

	if shard, ok := cm.symbolToShard[symbol]; ok {
		if cm.symbolDepth[symbol] == depth {
			return
		}
		shard.commandCh <- ShardCommand{Action: "subscribe", Symbols: []string{symbol}, Depth: depth}
		cm.symbolDepth[symbol] = depth
		return
	}

	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < symbolsPerShard {
			shard.commandCh <- ShardCommand{Action: "subscribe", Symbols: []string{symbol}, Depth: depth}
			cm.symbolToShard[symbol] = shard
			cm.symbolDepth[symbol] = depth
			cm.shardLoad[shard]++
			return
		}
	}

	stopCh := make(chan struct{})
	newShard := NewOrderBookShardWorker(cm.marketType, stopCh, cm.dataCh, cm.statusCh, &cm.wg)
	cm.shards = append(cm.shards, newShard)
	cm.symbolToShard[symbol] = newShard
	cm.symbolDepth[symbol] = depth
	cm.shardStops[newShard] = stopCh
	cm.shardLoad[newShard] = 1
	cm.wg.Add(1)
	go newShard.Run()

	newShard.commandCh <- ShardCommand{Action: "subscribe", Symbols: []string{symbol}, Depth: depth}
}

func (cm *OrderBookConnectionManager) removeSubscription(symbol string) {
	shard, ok := cm.symbolToShard[symbol]
	if !ok {
		return
	}

	shard.commandCh <- ShardCommand{Action: "unsubscribe", Symbols: []string{symbol}, Depth: cm.symbolDepth[symbol]}
	delete(cm.symbolToShard, symbol)
	delete(cm.symbolDepth, symbol)
	cm.shardLoad[shard]--
	if cm.shardLoad[shard] <= 0 {
		cm.retireShard(shard)
	}
}

func (cm *OrderBookConnectionManager) retireShard(shard *OrderBookShardWorker) {
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
