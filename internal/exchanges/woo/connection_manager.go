package woo

import (
	"log"
	"sync"

	"bybit-watcher/internal/shared_types"
)

type ConnectionManager struct {
	wsURL      string
	marketType string
	commandCh  chan managerCommand
	stopCh     chan struct{}
	dataCh     chan<- *shared_types.TradeUpdate
	statusCh   chan<- *shared_types.StreamStatusEvent
	wg         sync.WaitGroup

	shards        []*ShardWorker
	symbolToShard map[string]*ShardWorker
	shardStops    map[*ShardWorker]chan struct{}
	shardLoad     map[*ShardWorker]int
}

func NewConnectionManager(wsURL, marketType string, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent) *ConnectionManager {
	return &ConnectionManager{
		wsURL:         wsURL,
		marketType:    marketType,
		commandCh:     make(chan managerCommand, 1000),
		stopCh:        make(chan struct{}),
		dataCh:        dataCh,
		statusCh:      statusCh,
		symbolToShard: make(map[string]*ShardWorker),
		shardStops:    make(map[*ShardWorker]chan struct{}),
		shardLoad:     make(map[*ShardWorker]int),
	}
}

func (cm *ConnectionManager) Run() {
	log.Printf("[WOO-CONN-MANAGER] Starte Manager fuer %s", cm.marketType)
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
	if symbol == "" {
		return
	}
	if _, ok := cm.symbolToShard[symbol]; ok {
		return
	}
	var target *ShardWorker
	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < symbolsPerShard {
			target = shard
			break
		}
	}
	if target == nil {
		stopCh := make(chan struct{})
		target = NewShardWorker(cm.wsURL, cm.marketType, stopCh, cm.dataCh, cm.statusCh, &cm.wg)
		cm.shards = append(cm.shards, target)
		cm.shardStops[target] = stopCh
		cm.shardLoad[target] = 0
		go target.Run()
	}
	cm.symbolToShard[symbol] = target
	cm.shardLoad[target]++
	target.commandCh <- shardCommand{Action: "subscribe", Symbols: []string{symbol}}
}

func (cm *ConnectionManager) removeSubscription(symbol string) {
	shard, ok := cm.symbolToShard[symbol]
	if !ok {
		return
	}
	delete(cm.symbolToShard, symbol)
	if cm.shardLoad[shard] > 0 {
		cm.shardLoad[shard]--
	}
	shard.commandCh <- shardCommand{Action: "unsubscribe", Symbols: []string{symbol}}
	if cm.shardLoad[shard] == 0 {
		cm.retireShard(shard)
	}
}

func (cm *ConnectionManager) retireShard(shard *ShardWorker) {
	if stop, ok := cm.shardStops[shard]; ok {
		close(stop)
		delete(cm.shardStops, shard)
	}
	delete(cm.shardLoad, shard)
	for i, candidate := range cm.shards {
		if candidate == shard {
			cm.shards = append(cm.shards[:i], cm.shards[i+1:]...)
			break
		}
	}
}

func (cm *ConnectionManager) stopAllShards() {
	for _, stop := range cm.shardStops {
		close(stop)
	}
	cm.wg.Wait()
	cm.shards = nil
	cm.symbolToShard = make(map[string]*ShardWorker)
	cm.shardStops = make(map[*ShardWorker]chan struct{})
	cm.shardLoad = make(map[*ShardWorker]int)
}
