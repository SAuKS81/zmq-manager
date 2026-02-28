package bitget

import (
	"log"
	"sync"
	"time"

	"bybit-watcher/internal/shared_types"
)

type ManagerCommand struct {
	Action string
	Symbol string
}

type ConnectionManager struct {
	marketType string
	commandCh  chan ManagerCommand
	stopCh     chan struct{}
	dataCh     chan<- *shared_types.TradeUpdate
	statusCh   chan<- *shared_types.StreamStatusEvent
	limiter    *bitgetSendLimiter

	activeSubscriptions map[string]bool
	shards              []*ShardWorker
	symbolToShard       map[string]*ShardWorker
	shardLoad           map[*ShardWorker]int
	wg                  sync.WaitGroup

	// Batching state
	mu                   sync.Mutex
	pendingSubscriptions map[*ShardWorker][]string
	subscriptionTimers   map[*ShardWorker]*time.Timer
	pendingUnsubs        map[*ShardWorker][]string
	unsubTimers          map[*ShardWorker]*time.Timer
}

func NewConnectionManager(marketType string, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent, limiter *bitgetSendLimiter) *ConnectionManager {
	return &ConnectionManager{
		marketType:           marketType,
		commandCh:            make(chan ManagerCommand, 100),
		stopCh:               make(chan struct{}),
		dataCh:               dataCh,
		statusCh:             statusCh,
		limiter:              limiter,
		activeSubscriptions:  make(map[string]bool),
		symbolToShard:        make(map[string]*ShardWorker),
		shardLoad:            make(map[*ShardWorker]int),
		pendingSubscriptions: make(map[*ShardWorker][]string),
		subscriptionTimers:   make(map[*ShardWorker]*time.Timer),
		pendingUnsubs:        make(map[*ShardWorker][]string),
		unsubTimers:          make(map[*ShardWorker]*time.Timer),
	}
}

func (cm *ConnectionManager) Run() {
	log.Printf("[BITGET-CONN-MANAGER] Starte Manager fuer %s", cm.marketType)
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
			log.Printf("[BITGET-CONN-MANAGER] Stoppe Manager fuer %s", cm.marketType)
			return
		}
	}
}

func (cm *ConnectionManager) Stop() {
	close(cm.stopCh)
}

func (cm *ConnectionManager) addSubscription(symbol string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.activeSubscriptions[symbol] {
		return
	}

	log.Printf("[BITGET-CONN-MANAGER] Fuege Abonnement hinzu: %s", symbol)
	cm.activeSubscriptions[symbol] = true

	// Find an existing shard with capacity.
	var targetShard *ShardWorker
	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < symbolsPerShard {
			targetShard = shard
			break
		}
	}

	// No shard with capacity, create a new one.
	if targetShard == nil {
		log.Printf("[BITGET-CONN-MANAGER] Erstelle neuen Shard fuer %s. Pausiere fuer 1100ms...", symbol)
		time.Sleep(1100 * time.Millisecond)

		stopCh := make(chan struct{})
		targetShard = NewShardWorker(wsURL, cm.marketType, []string{}, stopCh, cm.dataCh, cm.statusCh, cm.limiter, &cm.wg)
		cm.shards = append(cm.shards, targetShard)
		cm.shardLoad[targetShard] = 0
		cm.wg.Add(1)
		go targetShard.Run()
	}

	cm.pendingSubscriptions[targetShard] = append(cm.pendingSubscriptions[targetShard], symbol)
	cm.symbolToShard[symbol] = targetShard
	cm.shardLoad[targetShard]++

	if timer, ok := cm.subscriptionTimers[targetShard]; ok {
		timer.Reset(200 * time.Millisecond)
	} else {
		cm.subscriptionTimers[targetShard] = time.AfterFunc(200*time.Millisecond, func() {
			cm.flushSubscriptions(targetShard)
		})
	}
}

func (cm *ConnectionManager) flushSubscriptions(shard *ShardWorker) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	symbolsToSub := cm.pendingSubscriptions[shard]
	if len(symbolsToSub) == 0 {
		return
	}

	log.Printf("[BITGET-CONN-MANAGER] Timer abgelaufen. Sende Batch mit %d Symbolen an Shard.", len(symbolsToSub))
	shard.commandCh <- ShardCommand{Action: "subscribe", Symbols: symbolsToSub}

	delete(cm.pendingSubscriptions, shard)
	delete(cm.subscriptionTimers, shard)
}

func (cm *ConnectionManager) removeSubscription(symbol string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if !cm.activeSubscriptions[symbol] {
		return
	}

	log.Printf("[BITGET-CONN-MANAGER] Entferne Abonnement: %s", symbol)
	delete(cm.activeSubscriptions, symbol)

	shard, ok := cm.symbolToShard[symbol]
	if !ok {
		return
	}

	delete(cm.symbolToShard, symbol)
	cm.shardLoad[shard]--
	if cm.shardLoad[shard] < 0 {
		cm.shardLoad[shard] = 0
	}

	// If symbol is still pending subscribe, remove it there and skip unsubscribe.
	wasPending := cm.removePendingSubscriptionLocked(shard, symbol)
	if !wasPending {
		cm.enqueueUnsubscribeLocked(shard, symbol)
	}

	if cm.shardLoad[shard] <= 0 {
		cm.maybeRetireShardLocked(shard)
	}
}

func (cm *ConnectionManager) removePendingSubscriptionLocked(shard *ShardWorker, symbol string) bool {
	pending := cm.pendingSubscriptions[shard]
	if len(pending) == 0 {
		return false
	}

	removed := false
	filtered := pending[:0]
	for _, s := range pending {
		if s == symbol {
			removed = true
			continue
		}
		filtered = append(filtered, s)
	}

	if !removed {
		return false
	}

	if len(filtered) == 0 {
		delete(cm.pendingSubscriptions, shard)
		if timer, ok := cm.subscriptionTimers[shard]; ok {
			timer.Stop()
			delete(cm.subscriptionTimers, shard)
		}
		return true
	}

	cm.pendingSubscriptions[shard] = filtered
	return true
}

func (cm *ConnectionManager) enqueueUnsubscribeLocked(shard *ShardWorker, symbol string) {
	for _, s := range cm.pendingUnsubs[shard] {
		if s == symbol {
			return
		}
	}
	cm.pendingUnsubs[shard] = append(cm.pendingUnsubs[shard], symbol)

	if timer, ok := cm.unsubTimers[shard]; ok {
		timer.Reset(150 * time.Millisecond)
		return
	}

	cm.unsubTimers[shard] = time.AfterFunc(150*time.Millisecond, func() {
		cm.flushUnsubs(shard)
	})
}

func (cm *ConnectionManager) flushUnsubs(shard *ShardWorker) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	symbolsToUnsub := cm.pendingUnsubs[shard]
	if len(symbolsToUnsub) == 0 {
		delete(cm.unsubTimers, shard)
		return
	}

	log.Printf("[BITGET-CONN-MANAGER] Sende Unsubscribe-Batch mit %d Symbolen an Shard.", len(symbolsToUnsub))
	shard.commandCh <- ShardCommand{Action: "unsubscribe", Symbols: symbolsToUnsub}

	delete(cm.pendingUnsubs, shard)
	delete(cm.unsubTimers, shard)
	cm.maybeRetireShardLocked(shard)
}

func (cm *ConnectionManager) retireShardLocked(shard *ShardWorker) {
	log.Printf("[BITGET-CONN-MANAGER] Shard ist jetzt leer (Load: %d). Entferne ihn aus dem aktiven Satz.", cm.shardLoad[shard])

	if timer, ok := cm.subscriptionTimers[shard]; ok {
		timer.Stop()
		delete(cm.subscriptionTimers, shard)
	}
	if timer, ok := cm.unsubTimers[shard]; ok {
		timer.Stop()
		delete(cm.unsubTimers, shard)
	}

	delete(cm.pendingSubscriptions, shard)
	delete(cm.pendingUnsubs, shard)
	delete(cm.shardLoad, shard)

	for symbol, mappedShard := range cm.symbolToShard {
		if mappedShard == shard {
			delete(cm.symbolToShard, symbol)
		}
	}

	filtered := cm.shards[:0]
	for _, existing := range cm.shards {
		if existing == shard {
			continue
		}
		filtered = append(filtered, existing)
	}
	cm.shards = filtered
}

func (cm *ConnectionManager) maybeRetireShardLocked(shard *ShardWorker) {
	if cm.shardLoad[shard] > 0 {
		return
	}
	if len(cm.pendingSubscriptions[shard]) > 0 {
		return
	}
	if len(cm.pendingUnsubs[shard]) > 0 {
		return
	}
	cm.retireShardLocked(shard)
}
