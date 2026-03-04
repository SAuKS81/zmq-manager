package bybit

import (
	"log"
	"sync"

	"bybit-watcher/internal/shared_types"
)

// ManagerCommand ist ein Befehl an den ConnectionManager.
type ManagerCommand struct {
	Action string
	Symbol string
	Depth  int // Wird von diesem Manager ignoriert, aber fuer Kompatibilitaet hinzugefuegt
}

// ConnectionManager verwaltet Shards fuer einen Markt-Typ.
type ConnectionManager struct {
	wsURL      string
	marketType string
	commandCh  chan ManagerCommand
	stopCh     chan struct{}
	dataCh     chan<- *shared_types.TradeUpdate
	statusCh   chan<- *shared_types.StreamStatusEvent

	activeSubscriptions map[string]bool
	shards              []*ShardWorker
	symbolToShard       map[string]*ShardWorker
	shardStops          map[*ShardWorker]chan struct{}
	shardLoad           map[*ShardWorker]int
	wg                  sync.WaitGroup
}

// NewConnectionManager erstellt einen neuen Manager.
func NewConnectionManager(wsURL, marketType string, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent) *ConnectionManager {
	return &ConnectionManager{
		wsURL:               wsURL,
		marketType:          marketType,
		commandCh:           make(chan ManagerCommand, 100),
		stopCh:              make(chan struct{}),
		dataCh:              dataCh,
		statusCh:            statusCh,
		activeSubscriptions: make(map[string]bool),
		symbolToShard:       make(map[string]*ShardWorker),
		shardStops:          make(map[*ShardWorker]chan struct{}),
		shardLoad:           make(map[*ShardWorker]int),
	}
}

// Run startet die Hauptschleife des ConnectionManagers.
func (cm *ConnectionManager) Run() {
	log.Printf("[CONN-MANAGER] Starte Manager fuer %s", cm.marketType)
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
			log.Printf("[CONN-MANAGER] Stoppe Manager fuer %s", cm.marketType)
			cm.stopAllShards()
			return
		}
	}
}

// Stop beendet den Manager und alle seine Shards.
func (cm *ConnectionManager) Stop() {
	close(cm.stopCh)
}

func (cm *ConnectionManager) addSubscription(symbol string) {
	if cm.activeSubscriptions[symbol] {
		log.Printf("[CONN-MANAGER-DEBUG] Symbol %s bereits abonniert, ignoriere.", symbol)
		return
	}

	log.Printf("[CONN-MANAGER] Fuege Abonnement hinzu: %s", symbol)
	cm.activeSubscriptions[symbol] = true

	for i, shard := range cm.shards {
		currentLoad := cm.shardLoad[shard]
		log.Printf("[CONN-MANAGER-DEBUG] Pruefe Shard %d, Auslastung: %d/%d", i, currentLoad, symbolsPerShard)
		if currentLoad < symbolsPerShard {
			log.Printf("[CONN-MANAGER] Sende 'subscribe' fuer %s an existierenden Shard %d.", symbol, i)
			shard.commandCh <- ShardCommand{Action: "subscribe", Symbols: []string{symbol}}
			cm.symbolToShard[symbol] = shard
			cm.shardLoad[shard]++
			return
		}
	}

	log.Printf("[CONN-MANAGER] Erstelle neuen Shard fuer %s.", symbol)
	stopCh := make(chan struct{})
	newShard := NewShardWorker(cm.wsURL, cm.marketType, []string{symbol}, stopCh, cm.dataCh, cm.statusCh, &cm.wg)
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

	log.Printf("[CONN-MANAGER] Entferne Abonnement: %s", symbol)
	delete(cm.activeSubscriptions, symbol)

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
	if cm.shardLoad[shard] < 0 {
		cm.shardLoad[shard] = 0
	}
	log.Printf("[CONN-MANAGER] Shard ist jetzt leer (Load: %d). Entferne ihn aus dem aktiven Satz.", cm.shardLoad[shard])

	if stopCh, ok := cm.shardStops[shard]; ok {
		close(stopCh)
		delete(cm.shardStops, shard)
	}
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

func (cm *ConnectionManager) stopAllShards() {
	for shard, stopCh := range cm.shardStops {
		close(stopCh)
		delete(cm.shardStops, shard)
	}
	cm.shards = nil
	cm.symbolToShard = make(map[string]*ShardWorker)
	cm.shardLoad = make(map[*ShardWorker]int)
}
