package mexc

import (
	"sync"

	"bybit-watcher/internal/shared_types"
)

type ConnectionManager struct {
	marketType string
	commandCh  chan ManagerCommand
	stopCh     chan struct{}
	dataCh     chan<- *shared_types.TradeUpdate
	statusCh   chan<- *shared_types.StreamStatusEvent

	activeSubscriptions map[string]bool
	shards              []*ShardWorker
	symbolToShard       map[string]*ShardWorker
	symbolFreq          map[string]string
	shardStops          map[*ShardWorker]chan struct{}
	shardLoad           map[*ShardWorker]int
	wg                  sync.WaitGroup
}

func NewConnectionManager(marketType string, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent) *ConnectionManager {
	return &ConnectionManager{
		marketType:          marketType,
		commandCh:           make(chan ManagerCommand, 100),
		stopCh:              make(chan struct{}),
		dataCh:              dataCh,
		statusCh:            statusCh,
		activeSubscriptions: make(map[string]bool),
		symbolToShard:       make(map[string]*ShardWorker),
		symbolFreq:          make(map[string]string),
		shardStops:          make(map[*ShardWorker]chan struct{}),
		shardLoad:           make(map[*ShardWorker]int),
	}
}

func (cm *ConnectionManager) Run() {
	for {
		select {
		case cmd := <-cm.commandCh:
			switch cmd.Action {
			case "add":
				cm.addSubscription(cmd.Symbol, cmd.Freq)
			case "remove":
				cm.removeSubscription(cmd.Symbol)
			}
		case <-cm.stopCh:
			cm.stopAllShards()
			return
		}
	}
}

func (cm *ConnectionManager) Stop() {
	close(cm.stopCh)
}

func (cm *ConnectionManager) addSubscription(symbol string, freq string) {
	freq = normalizeStreamFrequency(freq)
	if cm.activeSubscriptions[symbol] {
		if cm.symbolFreq[symbol] == freq {
			return
		}
		if shard, ok := cm.symbolToShard[symbol]; ok {
			shard.commandCh <- ShardCommand{Action: "subscribe", Symbols: []string{symbol}, Freq: freq}
			cm.symbolFreq[symbol] = freq
		}
		return
	}

	cm.activeSubscriptions[symbol] = true
	cm.symbolFreq[symbol] = freq

	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < symbolsPerShard {
			shard.commandCh <- ShardCommand{Action: "subscribe", Symbols: []string{symbol}, Freq: freq}
			cm.symbolToShard[symbol] = shard
			cm.shardLoad[shard]++
			return
		}
	}

	stopCh := make(chan struct{})
	newShard := NewShardWorker(wsURL, cm.marketType, []symbolSubscription{{Symbol: symbol, Freq: freq}}, stopCh, cm.dataCh, cm.statusCh, &cm.wg)
	cm.shards = append(cm.shards, newShard)
	cm.symbolToShard[symbol] = newShard
	cm.shardStops[newShard] = stopCh
	cm.shardLoad[newShard] = 1
	cm.wg.Add(1)
	go newShard.Run()
}

func (cm *ConnectionManager) removeSubscription(symbol string) {
	if !cm.activeSubscriptions[symbol] {
		return
	}

	delete(cm.activeSubscriptions, symbol)

	shard, ok := cm.symbolToShard[symbol]
	if !ok {
		return
	}

	shard.commandCh <- ShardCommand{Action: "unsubscribe", Symbols: []string{symbol}, Freq: cm.symbolFreq[symbol]}
	delete(cm.symbolToShard, symbol)
	delete(cm.symbolFreq, symbol)
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
		if existing == shard {
			continue
		}
		filtered = append(filtered, existing)
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
	cm.symbolFreq = make(map[string]string)
	cm.shardLoad = make(map[*ShardWorker]int)
}
