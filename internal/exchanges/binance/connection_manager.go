package binance

import (
	"log"
	"sync"

	"bybit-watcher/internal/shared_types"
)

type ManagerCommand struct {
	Action string
	Symbol string
}

type ConnectionManager struct {
	wsURL           string
	marketType      string
	symbolsPerShard int
	commandCh       chan ManagerCommand
	stopCh          chan struct{}
	dataCh          chan<- *shared_types.TradeUpdate
	
	activeSubscriptions map[string]bool
	shards              []*ShardWorker
	symbolToShard       map[string]*ShardWorker
	shardLoad           map[*ShardWorker]int
	wg                  sync.WaitGroup
}

func NewConnectionManager(wsURL, marketType string, symbolsPerShard int, dataCh chan<- *shared_types.TradeUpdate) *ConnectionManager {
	return &ConnectionManager{
		wsURL:               wsURL,
		marketType:          marketType,
		symbolsPerShard:     symbolsPerShard,
		commandCh:           make(chan ManagerCommand, 100),
		stopCh:              make(chan struct{}),
		dataCh:              dataCh,
		activeSubscriptions: make(map[string]bool),
		symbolToShard:       make(map[string]*ShardWorker),
		shardLoad:           make(map[*ShardWorker]int),
	}
}

func (cm *ConnectionManager) Run() {
	log.Printf("[BINANCE-TRADE-MANAGER] Starte für %s", cm.marketType)
	for {
		select {
		case cmd := <-cm.commandCh:
			if cmd.Action == "add" {
				cm.addSubscription(cmd.Symbol)
			} else {
				cm.removeSubscription(cmd.Symbol)
			}
		case <-cm.stopCh:
			return
		}
	}
}

func (cm *ConnectionManager) Stop() { close(cm.stopCh) }

func (cm *ConnectionManager) addSubscription(symbol string) {
	if cm.activeSubscriptions[symbol] { return }
	cm.activeSubscriptions[symbol] = true

	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < cm.symbolsPerShard {
			// ShardCommand nutzt jetzt Symbol (Singular)
			shard.commandCh <- ShardCommand{Action: "subscribe", Symbol: symbol}
			cm.symbolToShard[symbol] = shard
			cm.shardLoad[shard]++
			return
		}
	}

	log.Printf("[BINANCE-TRADE-MANAGER] Neuer Shard für %s", symbol)
	stopCh := make(chan struct{})
	
	// KORREKTUR: NewShardWorker Aufruf angepasst (kein initialSymbols mehr)
	newShard := NewShardWorker(cm.wsURL, cm.marketType, stopCh, cm.dataCh, &cm.wg)
	
	cm.shards = append(cm.shards, newShard)
	cm.wg.Add(1)
	go newShard.Run()

	// Symbol nachträglich senden
	newShard.commandCh <- ShardCommand{Action: "subscribe", Symbol: symbol}
	cm.symbolToShard[symbol] = newShard
	cm.shardLoad[newShard] = 1
}

func (cm *ConnectionManager) removeSubscription(symbol string) {
	if !cm.activeSubscriptions[symbol] { return }
	delete(cm.activeSubscriptions, symbol)

	if shard, ok := cm.symbolToShard[symbol]; ok {
		shard.commandCh <- ShardCommand{Action: "unsubscribe", Symbol: symbol}
		delete(cm.symbolToShard, symbol)
		cm.shardLoad[shard]--
	}
}