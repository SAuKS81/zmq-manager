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

	activeSubscriptions map[string]bool
	shards              []*ShardWorker
	symbolToShard       map[string]*ShardWorker
	shardLoad           map[*ShardWorker]int
	wg                  sync.WaitGroup

	// ======================================================================
	// NEUE FELDER FÜR DAS INTELLIGENTE BATCHING
	// ======================================================================
	mu                   sync.Mutex
	pendingSubscriptions map[*ShardWorker][]string // Sammelt Symbole für den nächsten Batch
	subscriptionTimers   map[*ShardWorker]*time.Timer  // Timer für jeden Shard
}

func NewConnectionManager(marketType string, dataCh chan<- *shared_types.TradeUpdate) *ConnectionManager {
	return &ConnectionManager{
		marketType:           marketType,
		commandCh:            make(chan ManagerCommand, 100),
		stopCh:               make(chan struct{}),
		dataCh:               dataCh,
		activeSubscriptions:  make(map[string]bool),
		symbolToShard:        make(map[string]*ShardWorker),
		shardLoad:            make(map[*ShardWorker]int),
		pendingSubscriptions: make(map[*ShardWorker][]string),
		subscriptionTimers:   make(map[*ShardWorker]*time.Timer),
	}
}

func (cm *ConnectionManager) Run() {
	log.Printf("[BITGET-CONN-MANAGER] Starte Manager für %s", cm.marketType)
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
			log.Printf("[BITGET-CONN-MANAGER] Stoppe Manager für %s", cm.marketType)
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
		return // Bereits abonniert
	}
	
	log.Printf("[BITGET-CONN-MANAGER] Füge Abonnement hinzu: %s", symbol)
	cm.activeSubscriptions[symbol] = true

	// 1. Finde einen passenden Shard oder erstelle einen neuen.
	var targetShard *ShardWorker
	for _, shard := range cm.shards {
		if cm.shardLoad[shard] < symbolsPerShard {
			targetShard = shard
			break
		}
	}

	if targetShard == nil {
		log.Printf("[BITGET-CONN-MANAGER] Erstelle neuen Shard für %s. Pausiere für 1100ms...", symbol)
		time.Sleep(1100 * time.Millisecond)

		stopCh := make(chan struct{})
		// Wichtig: Der neue Shard wird OHNE initiale Symbole gestartet.
		// Die Abos werden über den Batch-Mechanismus unten gesendet.
		targetShard = NewShardWorker(wsURL, cm.marketType, []string{}, stopCh, cm.dataCh, &cm.wg)
		cm.shards = append(cm.shards, targetShard)
		cm.shardLoad[targetShard] = 0 // Startet mit 0
		cm.wg.Add(1)
		go targetShard.Run()
	}

	// 2. Füge das Symbol der Warteschlange für den Ziel-Shard hinzu.
	cm.pendingSubscriptions[targetShard] = append(cm.pendingSubscriptions[targetShard], symbol)
	cm.symbolToShard[symbol] = targetShard
	cm.shardLoad[targetShard]++

	// 3. Setze oder resette den Timer für diesen Shard.
	if timer, ok := cm.subscriptionTimers[targetShard]; ok {
		// Timer läuft bereits, setze ihn zurück, um mehr Symbole zu sammeln.
		timer.Reset(200 * time.Millisecond)
	} else {
		// Erster neues Abo für diesen Shard, starte einen neuen Timer.
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
	
	// Sende den gebündelten Befehl an den Worker
	shard.commandCh <- ShardCommand{
		Action:  "subscribe",
		Symbols: symbolsToSub,
	}

	// Bereinige die Warteschlange und den Timer
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

	if shard, ok := cm.symbolToShard[symbol]; ok {
		delete(cm.symbolToShard, symbol)
		cm.shardLoad[shard]--
		if cm.shardLoad[shard] < 0 {
			cm.shardLoad[shard] = 0
		}

		// If the symbol is still waiting in the pending subscribe batch, drop it there
		// and skip sending an unsubscribe that the exchange never saw.
		wasPending := cm.removePendingSubscriptionLocked(shard, symbol)
		if !wasPending {
			shard.commandCh <- ShardCommand{Action: "unsubscribe", Symbols: []string{symbol}}
		}

		if cm.shardLoad[shard] <= 0 {
			log.Printf("[BITGET-CONN-MANAGER-TODO] Shard ist jetzt leer (Load: %d). Beendigungslogik fehlt noch.", cm.shardLoad[shard])
		}
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
