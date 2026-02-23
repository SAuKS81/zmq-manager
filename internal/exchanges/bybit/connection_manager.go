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
	Depth  int // Wird von diesem Manager ignoriert, aber für Kompatibilität hinzugefügt
}

// ConnectionManager verwaltet Shards für einen Markt-Typ.
type ConnectionManager struct {
	wsURL               string
	marketType          string
	commandCh           chan ManagerCommand
	stopCh              chan struct{}
	dataCh              chan<- *shared_types.TradeUpdate

	activeSubscriptions map[string]bool
	shards              []*ShardWorker
	symbolToShard       map[string]*ShardWorker
	shardLoad           map[*ShardWorker]int // NEU: Zählt Symbole pro Shard
	wg                  sync.WaitGroup
}

// NewConnectionManager erstellt einen neuen Manager.
func NewConnectionManager(wsURL, marketType string, dataCh chan<- *shared_types.TradeUpdate) *ConnectionManager {
	return &ConnectionManager{
		wsURL:               wsURL,
		marketType:          marketType,
		commandCh:           make(chan ManagerCommand, 100),
		stopCh:              make(chan struct{}),
		dataCh:              dataCh,
		activeSubscriptions: make(map[string]bool),
		symbolToShard:       make(map[string]*ShardWorker),
		shardLoad:           make(map[*ShardWorker]int), // NEU: Initialisierung
	}
}

// Run startet die Hauptschleife des ConnectionManagers.
func (cm *ConnectionManager) Run() {
	log.Printf("[CONN-MANAGER] Starte Manager für %s", cm.marketType)
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
			log.Printf("[CONN-MANAGER] Stoppe Manager für %s", cm.marketType)
			// TODO: Alle Shards sauber beenden
			return
		}
	}
}

// Stop beendet den Manager und alle seine Shards.
func (cm *ConnectionManager) Stop() {
	close(cm.stopCh)
}

// ======================================================================
// VOLLSTÄNDIGE, KORRIGIERTE 'addSubscription'-FUNKTION
// ======================================================================
func (cm *ConnectionManager) addSubscription(symbol string) {
	if cm.activeSubscriptions[symbol] {
		log.Printf("[CONN-MANAGER-DEBUG] Symbol %s bereits abonniert, ignoriere.", symbol)
		return
	}
	
	log.Printf("[CONN-MANAGER] Füge Abonnement hinzu: %s", symbol)
	cm.activeSubscriptions[symbol] = true

	// Finde einen Shard mit freiem Platz
	for i, shard := range cm.shards {
		// KORREKTUR: Verwende die neue, interne Zählung 'shardLoad'
		currentLoad := cm.shardLoad[shard]
		log.Printf("[CONN-MANAGER-DEBUG] Prüfe Shard %d, Auslastung: %d/%d", i, currentLoad, symbolsPerShard)
		
		if currentLoad < symbolsPerShard {
			log.Printf("[CONN-MANAGER] Sende 'subscribe' für %s an existierenden Shard %d.", symbol, i)
			shard.commandCh <- ShardCommand{Action: "subscribe", Symbols: []string{symbol}}
			cm.symbolToShard[symbol] = shard
			cm.shardLoad[shard]++ // Zähler erhöhen
			return
		}
	}

	// Kein freier Shard gefunden, erstelle einen neuen.
	log.Printf("[CONN-MANAGER] Erstelle neuen Shard für %s.", symbol)
	stopCh := make(chan struct{}) // TODO: Verwalten dieser Stop-Channels
	
	newShard := NewShardWorker(cm.wsURL, cm.marketType, []string{symbol}, stopCh, cm.dataCh, &cm.wg)
	cm.shards = append(cm.shards, newShard)
	cm.symbolToShard[symbol] = newShard
	cm.shardLoad[newShard] = 1 // Initialen Zähler für den neuen Shard setzen
	cm.wg.Add(1)
	go newShard.Run()
}

// ======================================================================
// VOLLSTÄNDIGE, KORRIGIERTE 'removeSubscription'-FUNKTION
// ======================================================================
func (cm *ConnectionManager) removeSubscription(symbol string) {
	if !cm.activeSubscriptions[symbol] {
		return // Nicht abonniert
	}

	log.Printf("[CONN-MANAGER] Entferne Abonnement: %s", symbol)
	delete(cm.activeSubscriptions, symbol)

	if shard, ok := cm.symbolToShard[symbol]; ok {
		shard.commandCh <- ShardCommand{Action: "unsubscribe", Symbols: []string{symbol}}
		delete(cm.symbolToShard, symbol)
		
		// KORREKTUR: Zähler verringern
		cm.shardLoad[shard]-- 
		
		// TODO: Hier muss noch die Logik implementiert werden, um leere Shards zu beenden
		// und aus der cm.shards-Liste zu entfernen, um Ressourcen freizugeben.
		// Für den Moment ist das aber nicht kritisch für die Funktionalität.
		if cm.shardLoad[shard] <= 0 {
			log.Printf("[CONN-MANAGER-TODO] Shard ist jetzt leer (Load: %d). Beendigungslogik fehlt noch.", cm.shardLoad[shard])
		}
	}
}