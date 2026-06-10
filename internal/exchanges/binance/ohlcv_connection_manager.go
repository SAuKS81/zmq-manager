package binance

import (
	"log"
	"sync"

	"bybit-watcher/internal/shared_types"
)

type OHLCVManagerCommand struct {
	Action   string
	Symbol   string
	Interval string
}

type OHLCVShardCommand struct {
	Action string
	Stream string
}

type OHLCVConnectionManager struct {
	wsURL           string
	marketType      string
	symbolsPerShard int
	commandCh       chan OHLCVManagerCommand
	stopCh          chan struct{}
	dataCh          chan<- *shared_types.OHLCVUpdate

	activeSubscriptions map[string]bool
	shards              []*OHLCVShardWorker
	streamToShard       map[string]*OHLCVShardWorker
	shardLoad           map[*OHLCVShardWorker]int
	wg                  sync.WaitGroup
}

func NewOHLCVConnectionManager(wsURL, marketType string, symbolsPerShard int, dataCh chan<- *shared_types.OHLCVUpdate) *OHLCVConnectionManager {
	return &OHLCVConnectionManager{
		wsURL:               wsURL,
		marketType:          marketType,
		symbolsPerShard:     symbolsPerShard,
		commandCh:           make(chan OHLCVManagerCommand, 100),
		stopCh:              make(chan struct{}),
		dataCh:              dataCh,
		activeSubscriptions: make(map[string]bool),
		streamToShard:       make(map[string]*OHLCVShardWorker),
		shardLoad:           make(map[*OHLCVShardWorker]int),
	}
}

func (cm *OHLCVConnectionManager) Run() {
	log.Printf("[BINANCE-OHLCV-MANAGER] Starte fuer %s", cm.marketType)
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
			return
		}
	}
}

func (cm *OHLCVConnectionManager) Stop() { close(cm.stopCh) }

func (cm *OHLCVConnectionManager) addSubscription(symbol, interval string) {
	stream, err := BuildKlineStream(symbol, interval)
	if err != nil || cm.activeSubscriptions[stream] {
		return
	}
	cm.activeSubscriptions[stream] = true

	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < cm.symbolsPerShard {
			shard.commandCh <- OHLCVShardCommand{Action: "subscribe", Stream: stream}
			cm.streamToShard[stream] = shard
			cm.shardLoad[shard]++
			return
		}
	}

	log.Printf("[BINANCE-OHLCV-MANAGER] Neuer Shard fuer %s", stream)
	stopCh := make(chan struct{})
	newShard := NewOHLCVShardWorker(cm.wsURL, cm.marketType, stopCh, cm.dataCh, &cm.wg)

	cm.shards = append(cm.shards, newShard)
	cm.wg.Add(1)
	go newShard.Run()

	newShard.commandCh <- OHLCVShardCommand{Action: "subscribe", Stream: stream}
	cm.streamToShard[stream] = newShard
	cm.shardLoad[newShard] = 1
}

func (cm *OHLCVConnectionManager) removeSubscription(symbol, interval string) {
	stream, err := BuildKlineStream(symbol, interval)
	if err != nil || !cm.activeSubscriptions[stream] {
		return
	}
	delete(cm.activeSubscriptions, stream)

	if shard, ok := cm.streamToShard[stream]; ok {
		shard.commandCh <- OHLCVShardCommand{Action: "unsubscribe", Stream: stream}
		delete(cm.streamToShard, stream)
		cm.shardLoad[shard]--
	}
}
