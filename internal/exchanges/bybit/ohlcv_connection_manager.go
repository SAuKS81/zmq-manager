package bybit

import (
	"sync"

	"bybit-watcher/internal/shared_types"
)

type OHLCVConnectionManager struct {
	wsURL      string
	marketType string
	commandCh  chan ManagerCommand
	stopCh     chan struct{}
	dataCh     chan<- *shared_types.OHLCVUpdate
	statusCh   chan<- *shared_types.StreamStatusEvent

	activeSubscriptions map[string]bool
	shards              []*OHLCVShardWorker
	topicToShard        map[string]*OHLCVShardWorker
	shardStops          map[*OHLCVShardWorker]chan struct{}
	shardLoad           map[*OHLCVShardWorker]int
	wg                  sync.WaitGroup
}

func NewOHLCVConnectionManager(wsURL, marketType string, dataCh chan<- *shared_types.OHLCVUpdate, statusCh chan<- *shared_types.StreamStatusEvent) *OHLCVConnectionManager {
	return &OHLCVConnectionManager{
		wsURL:               wsURL,
		marketType:          marketType,
		commandCh:           make(chan ManagerCommand, 100),
		stopCh:              make(chan struct{}),
		dataCh:              dataCh,
		statusCh:            statusCh,
		activeSubscriptions: make(map[string]bool),
		topicToShard:        make(map[string]*OHLCVShardWorker),
		shardStops:          make(map[*OHLCVShardWorker]chan struct{}),
		shardLoad:           make(map[*OHLCVShardWorker]int),
	}
}

func (cm *OHLCVConnectionManager) Run() {
	for {
		select {
		case cmd := <-cm.commandCh:
			switch cmd.Action {
			case "add":
				cm.addSubscription(cmd.Symbol, cmd.Interval)
			case "remove":
				cm.removeSubscription(cmd.Symbol, cmd.Interval)
			}
		case <-cm.stopCh:
			cm.stopAllShards()
			return
		}
	}
}

func (cm *OHLCVConnectionManager) Stop() {
	close(cm.stopCh)
}

func (cm *OHLCVConnectionManager) addSubscription(symbol, interval string) {
	topic, err := BuildKlineTopic(symbol, interval)
	if err != nil || cm.activeSubscriptions[topic] {
		return
	}
	cm.activeSubscriptions[topic] = true

	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < symbolsPerShard {
			shard.commandCh <- ShardCommand{Action: "subscribe", Symbols: []string{topic}}
			cm.topicToShard[topic] = shard
			cm.shardLoad[shard]++
			return
		}
	}

	stopCh := make(chan struct{})
	newShard := NewOHLCVShardWorker(cm.wsURL, cm.marketType, []string{topic}, stopCh, cm.dataCh, cm.statusCh, &cm.wg)
	cm.shards = append(cm.shards, newShard)
	cm.topicToShard[topic] = newShard
	cm.shardStops[newShard] = stopCh
	cm.shardLoad[newShard] = 1
	cm.wg.Add(1)
	go newShard.Run()
}

func (cm *OHLCVConnectionManager) removeSubscription(symbol, interval string) {
	topic, err := BuildKlineTopic(symbol, interval)
	if err != nil || !cm.activeSubscriptions[topic] {
		return
	}
	delete(cm.activeSubscriptions, topic)

	shard, ok := cm.topicToShard[topic]
	if !ok {
		return
	}
	shard.commandCh <- ShardCommand{Action: "unsubscribe", Symbols: []string{topic}}
	delete(cm.topicToShard, topic)
	cm.shardLoad[shard]--
	if cm.shardLoad[shard] <= 0 {
		cm.retireShard(shard)
	}
}

func (cm *OHLCVConnectionManager) retireShard(shard *OHLCVShardWorker) {
	if cm.shardLoad[shard] < 0 {
		cm.shardLoad[shard] = 0
	}
	if stopCh, ok := cm.shardStops[shard]; ok {
		close(stopCh)
		delete(cm.shardStops, shard)
	}
	delete(cm.shardLoad, shard)
	for topic, mappedShard := range cm.topicToShard {
		if mappedShard == shard {
			delete(cm.topicToShard, topic)
		}
	}
	filtered := cm.shards[:0]
	for _, existing := range cm.shards {
		if existing != shard {
			filtered = append(filtered, existing)
		}
	}
	cm.shards = filtered
}

func (cm *OHLCVConnectionManager) stopAllShards() {
	for shard, stopCh := range cm.shardStops {
		close(stopCh)
		delete(cm.shardStops, shard)
	}
	cm.shards = nil
	cm.topicToShard = make(map[string]*OHLCVShardWorker)
	cm.shardLoad = make(map[*OHLCVShardWorker]int)
}
