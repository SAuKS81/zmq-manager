//go:build ccxt
// +build ccxt

package ccxt

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"time"

	"bybit-watcher/internal/shared_types"
	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
)

// BatchShardWorker verwaltet eine einzelne WebSocket-Verbindung für `watchTradesForSymbols`.
type BatchShardWorker struct {
	exchangeName  string
	marketType    string
	config        ExchangeConfig
	commandCh     chan ShardCommand
	stopCh        chan struct{}
	dataCh        chan<- *shared_types.TradeUpdate
	wg            *sync.WaitGroup
	mu            sync.Mutex
	activeSymbols map[string]bool
	exchange      ccxtpro.IExchange
	cancelWorkers context.CancelFunc
}

func NewBatchShardWorker(exchangeName, marketType string, config ExchangeConfig, stopCh chan struct{}, dataCh chan<- *shared_types.TradeUpdate, wg *sync.WaitGroup) *BatchShardWorker {
	options := makeExchangeOptions(exchangeName, marketType)
	exchange := ccxtpro.CreateExchange(exchangeName, options)
	if exchange == nil {
		log.Printf("[CCXT-BATCH-SHARD-FATAL] Konnte Exchange-Instanz für %s nicht erstellen", exchangeName)
		return nil
	}
	return &BatchShardWorker{
		exchangeName:  exchangeName,
		marketType:    marketType,
		config:        config,
		commandCh:     make(chan ShardCommand, 500),
		stopCh:        stopCh,
		dataCh:        dataCh,
		wg:            wg,
		activeSymbols: make(map[string]bool),
		exchange:      exchange,
	}
}

func (sw *BatchShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[CCXT-BATCH-SHARD] Starte Worker für %s (%s)", sw.exchangeName, sw.marketType)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sw.processCommandQueue()
		case <-sw.stopCh:
			log.Printf("[CCXT-BATCH-SHARD] Stoppe Worker für %s (%s)", sw.exchangeName, sw.marketType)
			if sw.cancelWorkers != nil {
				sw.cancelWorkers()
			}
			return
		}
	}
}

func (sw *BatchShardWorker) getCommandChannel() chan<- ShardCommand {
	return sw.commandCh
}

func (sw *BatchShardWorker) processCommandQueue() {
	var commands []ShardCommand
drain:
	for {
		select {
		case cmd := <-sw.commandCh:
			commands = append(commands, cmd)
		default:
			break drain
		}
	}
	if len(commands) == 0 {
		return
	}

	sw.mu.Lock()
	listChanged := false
	for _, cmd := range commands {
		// KORREKTUR HIER: Wir iterieren über die Schlüssel der Map.
		for s := range cmd.Symbols {
			if cmd.Action == "subscribe" && !sw.activeSymbols[s] {
				sw.activeSymbols[s] = true
				listChanged = true
			} else if cmd.Action == "unsubscribe" && sw.activeSymbols[s] {
				delete(sw.activeSymbols, s)
				listChanged = true
			}
		}
	}

	if !listChanged {
		sw.mu.Unlock()
		return
	}

	if sw.cancelWorkers != nil {
		sw.cancelWorkers()
	}

	var symbolsToWatch []string
	for s := range sw.activeSymbols {
		symbolsToWatch = append(symbolsToWatch, s)
	}
	sw.mu.Unlock()

	symbolsToWatch = sw.filterSupportedSymbols(symbolsToWatch)
	if len(symbolsToWatch) > 0 {
		var ctx context.Context
		ctx, sw.cancelWorkers = context.WithCancel(context.Background())
		for i := 0; i < len(symbolsToWatch); i += sw.config.BatchSize {
			end := i + sw.config.BatchSize
			if end > len(symbolsToWatch) {
				end = len(symbolsToWatch)
			}
			batch := symbolsToWatch[i:end]
			go sw.runWorkerBatch(ctx, batch)
			time.Sleep(sw.config.SubscribePause)
		}
	}
}

func (sw *BatchShardWorker) runWorkerBatch(ctx context.Context, symbolsBatch []string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			trades, err := sw.safeWatchTradesForSymbols(symbolsBatch)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[CCXT-BATCH-SHARD-ERROR] `watchTradesForSymbols` fehlgeschlagen: %v. Warte 5s.", err)
				time.Sleep(5 * time.Second)
				continue
			}
			if len(trades) == 0 {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			ingestNow := time.Now()
			goTimestamp := ingestNow.UnixMilli()
			for _, trade := range trades {
				normalized, _ := NormalizeTrade(trade, sw.exchangeName, sw.marketType, goTimestamp, ingestNow.UnixNano())
				if normalized != nil {
					sw.dataCh <- normalized
				}
			}
		}
	}
}

func (sw *BatchShardWorker) safeWatchTradesForSymbols(symbolsBatch []string) (trades []ccxtpro.Trade, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in WatchTradesForSymbols: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return sw.exchange.WatchTradesForSymbols(symbolsBatch)
}

func (sw *BatchShardWorker) filterSupportedSymbols(symbols []string) []string {
	markets, err := sw.exchange.LoadMarkets()
	if err != nil {
		log.Printf("[CCXT-BATCH-SHARD-WARN] LoadMarkets failed for %s/%s: %v", sw.exchangeName, sw.marketType, err)
		return symbols
	}
	if len(markets) == 0 {
		return symbols
	}

	supported := make(map[string]struct{}, len(markets))
	for symbol := range markets {
		supported[symbol] = struct{}{}
	}

	filtered := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		if _, ok := supported[symbol]; ok {
			filtered = append(filtered, symbol)
			continue
		}
		sw.mu.Lock()
		delete(sw.activeSymbols, symbol)
		sw.mu.Unlock()
	}

	return filtered
}
