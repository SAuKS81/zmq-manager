//go:build ccxt
// +build ccxt

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
	CacheN   int
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
	statusCh    chan<- *shared_types.StreamStatusEvent

	tradeShards        []IShardWorker
	tradeShardStops    map[IShardWorker]chan struct{}
	symbolToTradeShard map[string]IShardWorker
	symbolToTradeCache map[string]int
	tradeShardCache    map[IShardWorker]int
	tradeShardLoad     map[IShardWorker]int

	obShards        []IShardWorker
	obShardStops    map[IShardWorker]chan struct{}
	symbolToOBShard map[string]IShardWorker
	symbolToOBDepth map[string]int
	obShardLoad     map[IShardWorker]int

	wg sync.WaitGroup
	mu sync.Mutex

	stopOnce sync.Once
	runDone  chan struct{}
}

func NewConnectionManager(exchangeName, marketType string, config ExchangeConfig, tradeDataCh chan<- *shared_types.TradeUpdate, obDataCh chan<- *shared_types.OrderBookUpdate, statusCh chan<- *shared_types.StreamStatusEvent) *ConnectionManager {
	return &ConnectionManager{
		exchangeName:       exchangeName,
		marketType:         marketType,
		config:             config,
		commandCh:          make(chan ManagerCommand, 1000),
		stopCh:             make(chan struct{}),
		tradeDataCh:        tradeDataCh,
		obDataCh:           obDataCh,
		statusCh:           statusCh,
		tradeShardStops:    make(map[IShardWorker]chan struct{}),
		symbolToTradeShard: make(map[string]IShardWorker),
		symbolToTradeCache: make(map[string]int),
		tradeShardCache:    make(map[IShardWorker]int),
		tradeShardLoad:     make(map[IShardWorker]int),
		obShardStops:       make(map[IShardWorker]chan struct{}),
		symbolToOBShard:    make(map[string]IShardWorker),
		symbolToOBDepth:    make(map[string]int),
		obShardLoad:        make(map[IShardWorker]int),
		runDone:            make(chan struct{}),
	}
}

func (cm *ConnectionManager) Run() {
	defer close(cm.runDone)
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
		if cm.shouldUseBatchOrderBookMode() {
			cm.processBatchOrderBookCommands(obCmds)
		} else {
			cm.processSingleOrderBookCommands(obCmds)
		}
	}
}

func (cm *ConnectionManager) shouldUseBatchOrderBookMode() bool {
	if !cm.config.UseForSymbols {
		return false
	}
	// Workaround for pinned CCXT-Pro build:
	// bybit swap orderbooks may stall with WatchOrderBookForSymbols.
	// Use single-symbol watchOrderBook path for swap until upstream behavior is stable.
	if cm.exchangeName == "bybit" && cm.marketType == "swap" {
		return false
	}
	return true
}

func (cm *ConnectionManager) tradeShardCapacity() int {
	if cm.config.OneTradeBatchPerShard && cm.config.UseForSymbols && cm.config.BatchSize > 0 {
		return cm.config.BatchSize
	}
	if cm.config.SymbolsPerShard > 0 {
		return cm.config.SymbolsPerShard
	}
	return 1
}

func (cm *ConnectionManager) dispatchSubscribeCommandsSequentially(shards []IShardWorker, additionsByShard map[IShardWorker]map[string]int, dataType string) {
	if len(additionsByShard) == 0 {
		return
	}

	pause := cm.config.ShardSubscribePause
	dispatched := 0
	for _, shard := range shards {
		symbols, ok := additionsByShard[shard]
		if !ok || len(symbols) == 0 {
			continue
		}

		dispatched++
		log.Printf("[CCXT-CONN-MANAGER] Dispatch %s subscribe an Shard %d/%d mit %d Symbolen (%s/%s).", dataType, dispatched, len(additionsByShard), len(symbols), cm.exchangeName, cm.marketType)
		shard.getCommandChannel() <- ShardCommand{Action: "subscribe", Symbols: symbols}
		if dispatched < len(additionsByShard) && pause > 0 {
			time.Sleep(pause)
		}
	}
}

// processTradeCommands ist korrekt und bleibt unverändert
func (cm *ConnectionManager) processTradeCommands(cmds []ManagerCommand) {
	symbolsToAdd := make(map[string]int)
	symbolsToRemove := make(map[string]int)
	for _, cmd := range cmds {
		if cmd.Action == "add" {
			desiredCacheN := cmd.CacheN
			prevCacheN, exists := cm.symbolToTradeCache[cmd.Symbol]
			if !exists {
				symbolsToAdd[cmd.Symbol] = desiredCacheN
			} else if prevCacheN != desiredCacheN {
				symbolsToRemove[cmd.Symbol] = prevCacheN
				symbolsToAdd[cmd.Symbol] = desiredCacheN
			}
		} else if cmd.Action == "remove" {
			symbolsToRemove[cmd.Symbol] = 0
		}
	}
	cm.unsubscribeTradeSymbolsLocked(symbolsToRemove)
	if len(symbolsToAdd) == 0 {
		return
	}
	shardCapacity := cm.tradeShardCapacity()
	additionsByShard := make(map[IShardWorker]map[string]int)
	for symbol := range symbolsToAdd {
		var targetShard IShardWorker
		for _, shard := range cm.tradeShards {
			if cm.config.UseForSymbols && cm.tradeShardLoad[shard] > 0 && cm.tradeShardCache[shard] != symbolsToAdd[symbol] {
				continue
			}
			if cm.tradeShardLoad[shard] < shardCapacity {
				targetShard = shard
				break
			}
		}
		if targetShard == nil {
			stopCh := make(chan struct{})
			var newShard IShardWorker
			if cm.config.UseForSymbols {
				newShard = NewBatchShardWorker(cm.exchangeName, cm.marketType, cm.config, stopCh, cm.tradeDataCh, cm.statusCh, &cm.wg)
			} else {
				newShard = NewSingleWatchShardWorker(cm.exchangeName, cm.marketType, cm.config, stopCh, cm.tradeDataCh, cm.statusCh, &cm.wg)
			}
			cm.tradeShards = append(cm.tradeShards, newShard)
			cm.tradeShardStops[newShard] = stopCh
			cm.tradeShardLoad[newShard] = 0
			cm.tradeShardCache[newShard] = symbolsToAdd[symbol]
			cm.wg.Add(1)
			go newShard.Run()
			time.Sleep(cm.config.NewShardPause)
			targetShard = newShard
		}
		if _, ok := additionsByShard[targetShard]; !ok {
			additionsByShard[targetShard] = make(map[string]int)
		}
		additionsByShard[targetShard][symbol] = symbolsToAdd[symbol]
		if cm.config.UseForSymbols && cm.tradeShardLoad[targetShard] == 0 {
			cm.tradeShardCache[targetShard] = symbolsToAdd[symbol]
		}
		cm.symbolToTradeShard[symbol] = targetShard
		cm.symbolToTradeCache[symbol] = symbolsToAdd[symbol]
		cm.tradeShardLoad[targetShard]++
	}
	cm.dispatchSubscribeCommandsSequentially(cm.tradeShards, additionsByShard, "trade")
}

// processSingleOrderBookCommands ist korrekt und bleibt unverändert
func (cm *ConnectionManager) processSingleOrderBookCommands(cmds []ManagerCommand) {
	symbolsToAdd := make(map[string]int)
	symbolsToRemove := make(map[string]int)
	for _, cmd := range cmds {
		if cmd.Action == "add" {
			prevDepth, exists := cm.symbolToOBDepth[cmd.Symbol]
			if !exists {
				symbolsToAdd[cmd.Symbol] = cmd.Depth
			} else if prevDepth != cmd.Depth {
				symbolsToRemove[cmd.Symbol] = prevDepth
				symbolsToAdd[cmd.Symbol] = cmd.Depth
			}
		} else if cmd.Action == "remove" {
			symbolsToRemove[cmd.Symbol] = 0
		}
	}
	cm.unsubscribeOrderBookSymbolsLocked(symbolsToRemove)
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
			newShard := NewOrderBookShardWorker(cm.exchangeName, cm.marketType, cm.config, stopCh, cm.obDataCh, cm.statusCh, &cm.wg)
			cm.obShards = append(cm.obShards, newShard)
			cm.obShardStops[newShard] = stopCh
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
		cm.symbolToOBDepth[symbol] = depth
		cm.obShardLoad[targetShard]++
	}
	cm.dispatchSubscribeCommandsSequentially(cm.obShards, additionsByShard, "orderbook")
}

// ======================================================================
// KORRIGIERTE FUNKTION: processBatchOrderBookCommands
// ======================================================================
func (cm *ConnectionManager) processBatchOrderBookCommands(cmds []ManagerCommand) {
	symbolsToAdd := make(map[string]int)
	symbolsToRemove := make(map[string]int)
	for _, cmd := range cmds {
		if cmd.Action == "add" {
			prevDepth, exists := cm.symbolToOBDepth[cmd.Symbol]
			if !exists {
				symbolsToAdd[cmd.Symbol] = cmd.Depth
			} else if prevDepth != cmd.Depth {
				symbolsToRemove[cmd.Symbol] = prevDepth
				symbolsToAdd[cmd.Symbol] = cmd.Depth
			}
		} else if cmd.Action == "remove" {
			symbolsToRemove[cmd.Symbol] = 0
		}
	}

	cm.unsubscribeOrderBookSymbolsLocked(symbolsToRemove)

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
			newShard := NewBatchOrderBookShardWorker(cm.exchangeName, cm.marketType, cm.config, stopCh, cm.obDataCh, cm.statusCh, &cm.wg)
			cm.obShards = append(cm.obShards, newShard)
			cm.obShardStops[newShard] = stopCh
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
		cm.symbolToOBDepth[symbol] = depth
		cm.obShardLoad[targetShard]++ // Wichtig: Auslastung aktualisieren
	}

	// Sende die gebündelten Befehle an die jeweiligen Shards
	cm.dispatchSubscribeCommandsSequentially(cm.obShards, additionsByShard, "orderbook")
}

func (cm *ConnectionManager) unsubscribeTradeSymbolsLocked(symbolsToRemove map[string]int) {
	if len(symbolsToRemove) == 0 {
		return
	}

	removalsByShard := make(map[IShardWorker]map[string]int)
	for symbol := range symbolsToRemove {
		shard, exists := cm.symbolToTradeShard[symbol]
		if !exists {
			continue
		}
		if _, ok := removalsByShard[shard]; !ok {
			removalsByShard[shard] = make(map[string]int)
		}
		removalsByShard[shard][symbol] = 0
		delete(cm.symbolToTradeShard, symbol)
		delete(cm.symbolToTradeCache, symbol)
		if cm.tradeShardLoad[shard] > 0 {
			cm.tradeShardLoad[shard]--
			if cm.tradeShardLoad[shard] == 0 {
				delete(cm.tradeShardCache, shard)
			}
		}
	}

	for shard, symbols := range removalsByShard {
		log.Printf("[CCXT-CONN-MANAGER] Reconfigure trade shard: unsubscribe %d Symbole", len(symbols))
		shard.getCommandChannel() <- ShardCommand{Action: "unsubscribe", Symbols: symbols}
		if cm.tradeShardLoad[shard] == 0 {
			cm.retireTradeShardLocked(shard)
		}
	}
}

func (cm *ConnectionManager) unsubscribeOrderBookSymbolsLocked(symbolsToRemove map[string]int) {
	if len(symbolsToRemove) == 0 {
		return
	}

	removalsByShard := make(map[IShardWorker]map[string]int)
	for symbol := range symbolsToRemove {
		shard, exists := cm.symbolToOBShard[symbol]
		if !exists {
			continue
		}
		if _, ok := removalsByShard[shard]; !ok {
			removalsByShard[shard] = make(map[string]int)
		}
		removalsByShard[shard][symbol] = 0
		delete(cm.symbolToOBShard, symbol)
		delete(cm.symbolToOBDepth, symbol)
		if cm.obShardLoad[shard] > 0 {
			cm.obShardLoad[shard]--
		}
	}

	for shard, symbols := range removalsByShard {
		log.Printf("[CCXT-CONN-MANAGER] Reconfigure orderbook shard: unsubscribe %d Symbole", len(symbols))
		shard.getCommandChannel() <- ShardCommand{Action: "unsubscribe", Symbols: symbols}
		if cm.obShardLoad[shard] == 0 {
			cm.retireOrderBookShardLocked(shard)
		}
	}
}

func (cm *ConnectionManager) Stop(wg *sync.WaitGroup) {
	if wg != nil {
		defer wg.Done()
	}

	log.Printf("[CCXT-CONN-MANAGER] Stoppe Manager für %s/%s", cm.exchangeName, cm.marketType)
	cm.stopOnce.Do(func() {
		close(cm.stopCh)
	})

	<-cm.runDone
	cm.mu.Lock()
	cm.stopAllTradeShardsLocked()
	cm.stopAllOrderBookShardsLocked()
	cm.mu.Unlock()
	cm.wg.Wait()
}

func (cm *ConnectionManager) retireTradeShardLocked(shard IShardWorker) {
	stopCh, ok := cm.tradeShardStops[shard]
	if !ok {
		return
	}
	delete(cm.tradeShardStops, shard)
	delete(cm.tradeShardLoad, shard)
	delete(cm.tradeShardCache, shard)
	for idx, candidate := range cm.tradeShards {
		if candidate == shard {
			cm.tradeShards = append(cm.tradeShards[:idx], cm.tradeShards[idx+1:]...)
			break
		}
	}
	close(stopCh)
}

func (cm *ConnectionManager) retireOrderBookShardLocked(shard IShardWorker) {
	stopCh, ok := cm.obShardStops[shard]
	if !ok {
		return
	}
	delete(cm.obShardStops, shard)
	delete(cm.obShardLoad, shard)
	for idx, candidate := range cm.obShards {
		if candidate == shard {
			cm.obShards = append(cm.obShards[:idx], cm.obShards[idx+1:]...)
			break
		}
	}
	close(stopCh)
}

func (cm *ConnectionManager) stopAllTradeShardsLocked() {
	for _, shard := range append([]IShardWorker(nil), cm.tradeShards...) {
		cm.retireTradeShardLocked(shard)
	}
}

func (cm *ConnectionManager) stopAllOrderBookShardsLocked() {
	for _, shard := range append([]IShardWorker(nil), cm.obShards...) {
		cm.retireOrderBookShardLocked(shard)
	}
}
