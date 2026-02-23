package ccxt

import (
	"log"
	"sync"
	"time"

	"bybit-watcher/internal/shared_types"
)

type ShardCommand struct {
	Action  string
	Symbols map[string]int
}

type ManagerCommand struct {
	Action   string
	Symbol   string
	DataType string
	Depth    int
}

type IShardWorker interface {
	Run()
	getCommandChannel() chan<- ShardCommand
}

type ConnectionManager struct {
	exchangeName string
	marketType   string
	config       ExchangeConfig
	commandCh    chan ManagerCommand
	stopCh       chan struct{}

	tradeDataCh chan<- *shared_types.TradeUpdate
	obDataCh    chan<- *shared_types.OrderBookUpdate

	tradeShards        []IShardWorker
	symbolToTradeShard map[string]IShardWorker
	tradeShardLoad     map[IShardWorker]int

	obShards        []IShardWorker
	symbolToOBShard map[string]IShardWorker
	obShardLoad     map[IShardWorker]int

	wg sync.WaitGroup
	mu sync.Mutex
}

func NewConnectionManager(exchangeName, marketType string, config ExchangeConfig, tradeDataCh chan<- *shared_types.TradeUpdate, obDataCh chan<- *shared_types.OrderBookUpdate) *ConnectionManager {
	return &ConnectionManager{
		exchangeName:       exchangeName,
		marketType:         marketType,
		config:             config,
		commandCh:          make(chan ManagerCommand, 1000),
		stopCh:             make(chan struct{}),
		tradeDataCh:        tradeDataCh,
		obDataCh:           obDataCh,
		symbolToTradeShard: make(map[string]IShardWorker),
		tradeShardLoad:     make(map[IShardWorker]int),
		symbolToOBShard:    make(map[string]IShardWorker),
		obShardLoad:        make(map[IShardWorker]int),
	}
}

func (cm *ConnectionManager) Run() {
	log.Printf("[CCXT-CONN-MANAGER] Starte Manager für %s/%s", cm.exchangeName, cm.marketType)
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	var commandQueue []ManagerCommand
	for {
		select {
		case cmd := <-cm.commandCh:
			commandQueue = append(commandQueue, cmd)
		case <-ticker.C:
			if len(commandQueue) > 0 {
				cm.processCommands(commandQueue)
				commandQueue = nil
			}
		case <-cm.stopCh:
			if len(commandQueue) > 0 {
				cm.processCommands(commandQueue)
			}
			return
		}
	}
}

func (cm *ConnectionManager) processCommands(cmds []ManagerCommand) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	tradeCmds := make([]ManagerCommand, 0)
	obCmds := make([]ManagerCommand, 0)
	for _, cmd := range cmds {
		if cmd.DataType == "orderbooks" {
			obCmds = append(obCmds, cmd)
		} else {
			tradeCmds = append(tradeCmds, cmd)
		}
	}
	if len(tradeCmds) > 0 {
		cm.processTradeCommands(tradeCmds)
	}
	if len(obCmds) > 0 {
		if cm.config.UseForSymbols {
			cm.processBatchOrderBookCommands(obCmds)
		} else {
			cm.processSingleOrderBookCommands(obCmds)
		}
	}
}

// processTradeCommands ist korrekt und bleibt unverändert
func (cm *ConnectionManager) processTradeCommands(cmds []ManagerCommand) {
	symbolsToAdd := make(map[string]int)
	symbolsToRemove := make(map[string]int)
	for _, cmd := range cmds {
		if cmd.Action == "add" {
			if _, exists := cm.symbolToTradeShard[cmd.Symbol]; !exists {
				symbolsToAdd[cmd.Symbol] = 0
			}
		} else if cmd.Action == "remove" {
			symbolsToRemove[cmd.Symbol] = 0
		}
	}
	removalsByShard := make(map[IShardWorker]map[string]int)
	for symbol := range symbolsToRemove {
		if shard, exists := cm.symbolToTradeShard[symbol]; exists {
			if _, ok := removalsByShard[shard]; !ok {
				removalsByShard[shard] = make(map[string]int)
			}
			removalsByShard[shard][symbol] = 0
			delete(cm.symbolToTradeShard, symbol)
			if cm.tradeShardLoad[shard] > 0 {
				cm.tradeShardLoad[shard]--
			}
		}
	}
	for shard, symbols := range removalsByShard {
		shard.getCommandChannel() <- ShardCommand{Action: "unsubscribe", Symbols: symbols}
	}
	if len(symbolsToAdd) == 0 {
		return
	}
	additionsByShard := make(map[IShardWorker]map[string]int)
	for symbol := range symbolsToAdd {
		var targetShard IShardWorker
		for _, shard := range cm.tradeShards {
			if cm.tradeShardLoad[shard] < cm.config.SymbolsPerShard {
				targetShard = shard
				break
			}
		}
		if targetShard == nil {
			stopCh := make(chan struct{})
			var newShard IShardWorker
			if cm.config.UseForSymbols {
				newShard = NewBatchShardWorker(cm.exchangeName, cm.marketType, cm.config, stopCh, cm.tradeDataCh, &cm.wg)
			} else {
				newShard = NewSingleWatchShardWorker(cm.exchangeName, cm.marketType, cm.config, stopCh, cm.tradeDataCh, &cm.wg)
			}
			cm.tradeShards = append(cm.tradeShards, newShard)
			cm.tradeShardLoad[newShard] = 0
			cm.wg.Add(1)
			go newShard.Run()
			time.Sleep(cm.config.NewShardPause)
			targetShard = newShard
		}
		if _, ok := additionsByShard[targetShard]; !ok {
			additionsByShard[targetShard] = make(map[string]int)
		}
		additionsByShard[targetShard][symbol] = 0
		cm.symbolToTradeShard[symbol] = targetShard
		cm.tradeShardLoad[targetShard]++
	}
	for shard, symbols := range additionsByShard {
		shard.getCommandChannel() <- ShardCommand{Action: "subscribe", Symbols: symbols}
	}
}

// processSingleOrderBookCommands ist korrekt und bleibt unverändert
func (cm *ConnectionManager) processSingleOrderBookCommands(cmds []ManagerCommand) {
	symbolsToAdd := make(map[string]int)
	symbolsToRemove := make(map[string]int)
	for _, cmd := range cmds {
		if cmd.Action == "add" {
			if _, exists := cm.symbolToOBShard[cmd.Symbol]; !exists {
				symbolsToAdd[cmd.Symbol] = cmd.Depth
			}
		} else if cmd.Action == "remove" {
			symbolsToRemove[cmd.Symbol] = 0
		}
	}
	removalsByShard := make(map[IShardWorker]map[string]int)
	for symbol := range symbolsToRemove {
		if shard, exists := cm.symbolToOBShard[symbol]; exists {
			if _, ok := removalsByShard[shard]; !ok {
				removalsByShard[shard] = make(map[string]int)
			}
			removalsByShard[shard][symbol] = 0
			delete(cm.symbolToOBShard, symbol)
			if cm.obShardLoad[shard] > 0 {
				cm.obShardLoad[shard]--
			}
		}
	}
	for shard, symbols := range removalsByShard {
		shard.getCommandChannel() <- ShardCommand{Action: "unsubscribe", Symbols: symbols}
	}
	if len(symbolsToAdd) == 0 {
		return
	}
	additionsByShard := make(map[IShardWorker]map[string]int)
	for symbol, depth := range symbolsToAdd {
		var targetShard IShardWorker
		for _, shard := range cm.obShards {
			if cm.obShardLoad[shard] < cm.config.SymbolsPerShard {
				targetShard = shard
				break
			}
		}
		if targetShard == nil {
			log.Printf("[CCXT-CONN-MANAGER] Erstelle neuen SINGLE OrderBook-Shard für %s", cm.exchangeName)
			stopCh := make(chan struct{})
			newShard := NewOrderBookShardWorker(cm.exchangeName, cm.marketType, cm.config, stopCh, cm.obDataCh, &cm.wg)
			cm.obShards = append(cm.obShards, newShard)
			cm.obShardLoad[newShard] = 0
			cm.wg.Add(1)
			go newShard.Run()
			time.Sleep(cm.config.NewShardPause)
			targetShard = newShard
		}
		if _, ok := additionsByShard[targetShard]; !ok {
			additionsByShard[targetShard] = make(map[string]int)
		}
		additionsByShard[targetShard][symbol] = depth
		cm.symbolToOBShard[symbol] = targetShard
		cm.obShardLoad[targetShard]++
	}
	for shard, symbols := range additionsByShard {
		shard.getCommandChannel() <- ShardCommand{Action: "subscribe", Symbols: symbols}
	}
}

// ======================================================================
// KORRIGIERTE FUNKTION: processBatchOrderBookCommands
// ======================================================================
func (cm *ConnectionManager) processBatchOrderBookCommands(cmds []ManagerCommand) {
	symbolsToAdd := make(map[string]int)
	symbolsToRemove := make(map[string]int)
	for _, cmd := range cmds {
		if cmd.Action == "add" {
			if _, exists := cm.symbolToOBShard[cmd.Symbol]; !exists {
				symbolsToAdd[cmd.Symbol] = cmd.Depth
			}
		} else if cmd.Action == "remove" {
			symbolsToRemove[cmd.Symbol] = 0
		}
	}

	// Unsubscribe-Logik bleibt gleich
	removalsByShard := make(map[IShardWorker]map[string]int)
	for symbol := range symbolsToRemove {
		if shard, exists := cm.symbolToOBShard[symbol]; exists {
			if _, ok := removalsByShard[shard]; !ok {
				removalsByShard[shard] = make(map[string]int)
			}
			removalsByShard[shard][symbol] = 0
			delete(cm.symbolToOBShard, symbol)
			if cm.obShardLoad[shard] > 0 {
				cm.obShardLoad[shard]--
			}
		}
	}
	for shard, symbols := range removalsByShard {
		shard.getCommandChannel() <- ShardCommand{Action: "unsubscribe", Symbols: symbols}
	}

	if len(symbolsToAdd) == 0 {
		return
	}

	// NEUE Subscribe-Logik:
	// Wir iterieren jetzt über jedes Symbol und weisen es einem Shard zu,
	// der noch Platz hat. Das ist dieselbe Logik wie im Single-Modus.
	additionsByShard := make(map[IShardWorker]map[string]int)
	for symbol, depth := range symbolsToAdd {
		var targetShard IShardWorker
		for _, shard := range cm.obShards {
			if cm.obShardLoad[shard] < cm.config.SymbolsPerShard {
				targetShard = shard
				break
			}
		}
		if targetShard == nil {
			log.Printf("[CCXT-CONN-MANAGER] Erstelle neuen BATCH OrderBook-Shard für %s (Limit: %d Symbole)", cm.exchangeName, cm.config.SymbolsPerShard)
			stopCh := make(chan struct{})
			// Hier wird der NEUE Batch-Worker erstellt
			newShard := NewBatchOrderBookShardWorker(cm.exchangeName, cm.marketType, cm.config, stopCh, cm.obDataCh, &cm.wg)
			cm.obShards = append(cm.obShards, newShard)
			cm.obShardLoad[newShard] = 0 // Initialisiere die Auslastung
			cm.wg.Add(1)
			go newShard.Run()
			time.Sleep(cm.config.NewShardPause)
			targetShard = newShard
		}

		// Weise Symbol dem Shard zu und bündle die Befehle
		if _, ok := additionsByShard[targetShard]; !ok {
			additionsByShard[targetShard] = make(map[string]int)
		}
		additionsByShard[targetShard][symbol] = depth
		cm.symbolToOBShard[symbol] = targetShard
		cm.obShardLoad[targetShard]++ // Wichtig: Auslastung aktualisieren
	}

	// Sende die gebündelten Befehle an die jeweiligen Shards
	for shard, symbols := range additionsByShard {
		log.Printf("[CCXT-CONN-MANAGER] Sende %d OB-Symbole an BATCH Shard. Neue Auslastung: %d/%d", len(symbols), cm.obShardLoad[shard], cm.config.SymbolsPerShard)
		shard.getCommandChannel() <- ShardCommand{Action: "subscribe", Symbols: symbols}
	}
}

func (cm *ConnectionManager) Stop(wg *sync.WaitGroup) {
	log.Printf("[CCXT-CONN-MANAGER] Stoppe Manager für %s/%s", cm.exchangeName, cm.marketType)
	close(cm.stopCh)
}