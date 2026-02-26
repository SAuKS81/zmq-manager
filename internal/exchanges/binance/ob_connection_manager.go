package binance

import (
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	"bybit-watcher/internal/shared_types"
)

// OBManagerCommand spezifisch für Orderbooks mit Tiefe
type OBManagerCommand struct {
	Action string
	Symbol string
	Depth  int
}

type OrderBookConnectionManager struct {
	wsURL               string
	marketType          string
	symbolsPerShard     int
	commandCh           chan OBManagerCommand
	stopCh              chan struct{}
	dataCh              chan<- *shared_types.OrderBookUpdate
	
	activeSubscriptions map[string]bool
	shards              []*OrderBookShardWorker
	symbolToShard       map[string]*OrderBookShardWorker
	shardLoad           map[*OrderBookShardWorker]int
	wg                  sync.WaitGroup
}

func NewOrderBookConnectionManager(wsURL, marketType string, symbolsPerShard int, dataCh chan<- *shared_types.OrderBookUpdate) *OrderBookConnectionManager {
	return &OrderBookConnectionManager{
		wsURL:               wsURL,
		marketType:          marketType,
		symbolsPerShard:     symbolsPerShard,
		commandCh:           make(chan OBManagerCommand, 100),
		stopCh:              make(chan struct{}),
		dataCh:              dataCh,
		activeSubscriptions: make(map[string]bool),
		symbolToShard:       make(map[string]*OrderBookShardWorker),
		shardLoad:           make(map[*OrderBookShardWorker]int),
	}
}

func (cm *OrderBookConnectionManager) Run() {
	log.Printf("[BINANCE-OB-MANAGER] Starte für %s", cm.marketType)
	for {
		select {
		case cmd := <-cm.commandCh:
			if cmd.Action == "add" {
				cm.addSubscription(cmd.Symbol, cmd.Depth)
			} else {
				cm.removeSubscription(cmd.Symbol, cmd.Depth)
			}
		case <-cm.stopCh:
			return
		}
	}
}

func (cm *OrderBookConnectionManager) Stop() { close(cm.stopCh) }

func (cm *OrderBookConnectionManager) addSubscription(symbol string, depth int) {
	key := fmt.Sprintf("%s:%d", symbol, depth)
	if cm.activeSubscriptions[key] { return }
	cm.activeSubscriptions[key] = true

	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < cm.symbolsPerShard {
			shard.commandCh <- OBManagerCommand{Action: "subscribe", Symbol: symbol, Depth: depth}
			cm.symbolToShard[key] = shard
			cm.shardLoad[shard]++
			return
		}
	}

	log.Printf("[BINANCE-OB-MANAGER] Neuer Shard für %s (Depth: %d)", symbol, depth)
	stopCh := make(chan struct{})
	newShard := NewOrderBookShardWorker(cm.wsURL, cm.marketType, stopCh, cm.dataCh, &cm.wg)
	cm.shards = append(cm.shards, newShard)
	cm.wg.Add(1)
	go newShard.Run()

	newShard.commandCh <- OBManagerCommand{Action: "subscribe", Symbol: symbol, Depth: depth}
	cm.symbolToShard[key] = newShard
	cm.shardLoad[newShard] = 1
}

func (cm *OrderBookConnectionManager) removeSubscription(symbol string, depth int) {
	if depth <= 0 {
		prefix := symbol + ":"
		keys := make([]string, 0, 4)
		for key := range cm.activeSubscriptions {
			if strings.HasPrefix(key, prefix) {
				keys = append(keys, key)
			}
		}
		for _, key := range keys {
			if !cm.activeSubscriptions[key] {
				continue
			}
			delete(cm.activeSubscriptions, key)
			_, depthPart, ok := strings.Cut(key, ":")
			if !ok {
				continue
			}
			parsedDepth, err := strconv.Atoi(depthPart)
			if err != nil {
				continue
			}
			if shard, ok := cm.symbolToShard[key]; ok {
				shard.commandCh <- OBManagerCommand{Action: "unsubscribe", Symbol: symbol, Depth: parsedDepth}
				delete(cm.symbolToShard, key)
				cm.shardLoad[shard]--
			}
		}
		return
	}

	key := fmt.Sprintf("%s:%d", symbol, depth)
	if !cm.activeSubscriptions[key] { return }
	delete(cm.activeSubscriptions, key)

	if shard, ok := cm.symbolToShard[key]; ok {
		shard.commandCh <- OBManagerCommand{Action: "unsubscribe", Symbol: symbol, Depth: depth}
		delete(cm.symbolToShard, key)
		cm.shardLoad[shard]--
	}
}
