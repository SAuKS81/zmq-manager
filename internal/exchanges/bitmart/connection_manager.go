package bitmart

import (
	"sync"

	"bybit-watcher/internal/shared_types"
)

type ConnectionManager struct {
	commandCh     chan ManagerCommand
	stopCh        chan struct{}
	dataCh        chan<- *shared_types.TradeUpdate
	statusCh      chan<- *shared_types.StreamStatusEvent
	shards        []*ShardWorker
	shardLoad     map[*ShardWorker]int
	shardStops    map[*ShardWorker]chan struct{}
	symbolToShard map[string]*ShardWorker
	wg            sync.WaitGroup
}

func NewConnectionManager(dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent) *ConnectionManager {
	return &ConnectionManager{
		commandCh:     make(chan ManagerCommand, 100),
		stopCh:        make(chan struct{}),
		dataCh:        dataCh,
		statusCh:      statusCh,
		shardLoad:     make(map[*ShardWorker]int),
		shardStops:    make(map[*ShardWorker]chan struct{}),
		symbolToShard: make(map[string]*ShardWorker),
	}
}

func (cm *ConnectionManager) Run() {
	for {
		select {
		case cmd := <-cm.commandCh:
			switch cmd.Action {
			case "add":
				cm.addSubscription(cmd.Symbol)
			case "remove":
				cm.removeSubscription(cmd.Symbol)
			}
		case <-cm.stopCh:
			cm.stopAllShards()
			return
		}
	}
}

func (cm *ConnectionManager) Stop() { close(cm.stopCh) }

func (cm *ConnectionManager) addSubscription(symbol string) {
	if shard, ok := cm.symbolToShard[symbol]; ok {
		shard.commandCh <- ShardCommand{Action: "subscribe", Symbols: []string{symbol}}
		return
	}
	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < symbolsPerShard {
			shard.commandCh <- ShardCommand{Action: "subscribe", Symbols: []string{symbol}}
			cm.symbolToShard[symbol] = shard
			cm.shardLoad[shard]++
			return
		}
	}
	stopCh := make(chan struct{})
	newShard := NewShardWorker([]string{symbol}, stopCh, cm.dataCh, cm.statusCh, &cm.wg)
	cm.shards = append(cm.shards, newShard)
	cm.shardStops[newShard] = stopCh
	cm.shardLoad[newShard] = 1
	cm.symbolToShard[symbol] = newShard
	cm.wg.Add(1)
	go newShard.Run()
}

func (cm *ConnectionManager) removeSubscription(symbol string) {
	shard, ok := cm.symbolToShard[symbol]
	if !ok {
		return
	}
	shard.commandCh <- ShardCommand{Action: "unsubscribe", Symbols: []string{symbol}}
	delete(cm.symbolToShard, symbol)
	cm.shardLoad[shard]--
	if cm.shardLoad[shard] <= 0 {
		cm.retireShard(shard)
	}
}

func (cm *ConnectionManager) retireShard(shard *ShardWorker) {
	if stopCh, ok := cm.shardStops[shard]; ok {
		close(stopCh)
		delete(cm.shardStops, shard)
	}
	delete(cm.shardLoad, shard)
	filtered := cm.shards[:0]
	for _, existing := range cm.shards {
		if existing != shard {
			filtered = append(filtered, existing)
		}
	}
	cm.shards = filtered
}

func (cm *ConnectionManager) stopAllShards() {
	for shard, stopCh := range cm.shardStops {
		close(stopCh)
		delete(cm.shardStops, shard)
	}
	cm.shards = nil
	cm.symbolToShard = make(map[string]*ShardWorker)
	cm.shardLoad = make(map[*ShardWorker]int)
}
