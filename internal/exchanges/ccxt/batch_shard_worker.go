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

// BatchShardWorker verwaltet eine einzelne WebSocket-Verbindung fuer watchTradesForSymbols.
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
	exchange := createCCXTExchange(exchangeName, marketType)
	if exchange == nil {
		log.Printf("[CCXT-BATCH-SHARD-FATAL] Konnte Exchange-Instanz fuer %s nicht erstellen", exchangeName)
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
	log.Printf("[CCXT-BATCH-SHARD] Starte Worker fuer %s (%s)", sw.exchangeName, sw.marketType)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sw.processCommandQueue()
		case <-sw.stopCh:
			log.Printf("[CCXT-BATCH-SHARD] Stoppe Worker fuer %s (%s)", sw.exchangeName, sw.marketType)
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
	unsubscribeSymbols := make(map[string]bool)
	for _, cmd := range commands {
		for s := range cmd.Symbols {
			if cmd.Action == "subscribe" && !sw.activeSymbols[s] {
				sw.activeSymbols[s] = true
				listChanged = true
			} else if cmd.Action == "unsubscribe" && sw.activeSymbols[s] {
				delete(sw.activeSymbols, s)
				listChanged = true
				unsubscribeSymbols[s] = true
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

	if len(unsubscribeSymbols) > 0 {
		symbolsToUnwatch := make([]string, 0, len(unsubscribeSymbols))
		for s := range unsubscribeSymbols {
			symbolsToUnwatch = append(symbolsToUnwatch, s)
		}
		if sw.config.SupportsTradeBatchUnwatch || exchangeHasFeature(sw.exchange, "unWatchTradesForSymbols") {
			if _, err := sw.safeUnWatchTradesForSymbols(symbolsToUnwatch); err != nil {
				log.Printf("[CCXT-BATCH-SHARD-WARN] UnWatchTradesForSymbols(%d) fehlgeschlagen (%s/%s): %v. Fallback auf Shard-Recycle.", len(symbolsToUnwatch), sw.exchangeName, sw.marketType, err)
				sw.recycleExchange()
			}
		} else {
			log.Printf("[CCXT-BATCH-SHARD-INFO] Batch trade unwatch nicht freigegeben (%s/%s). Nutze harten Shard-Recycle fuer %d Symbole.", sw.exchangeName, sw.marketType, len(symbolsToUnwatch))
			sw.recycleExchange()
		}
	}

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
	} else {
		sw.cancelWorkers = nil
	}
}

func (sw *BatchShardWorker) recycleExchange() {
	closeCCXTExchange(sw.exchangeName, sw.marketType, sw.exchange)
	sw.exchange = createCCXTExchange(sw.exchangeName, sw.marketType)
	if sw.exchange == nil {
		log.Printf("[CCXT-BATCH-SHARD-FATAL] Konnte Exchange nach Recycle fuer %s/%s nicht neu erstellen", sw.exchangeName, sw.marketType)
	}
}

func (sw *BatchShardWorker) runWorkerBatch(ctx context.Context, symbolsBatch []string) {
	currentBatch := append([]string(nil), symbolsBatch...)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			trades, err := sw.safeWatchTradesForSymbols(currentBatch)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if missingSymbol, ok := extractMissingMarketSymbol(err); ok {
					log.Printf("[CCXT-BATCH-SHARD-WARN] Entferne ungueltiges Symbol '%s' aus Batch (%s/%s).", missingSymbol, sw.exchangeName, sw.marketType)
					currentBatch = removeSymbolFromBatch(currentBatch, missingSymbol)
					sw.mu.Lock()
					delete(sw.activeSymbols, missingSymbol)
					sw.mu.Unlock()
					if len(currentBatch) == 0 {
						log.Printf("[CCXT-BATCH-SHARD-WARN] Batch leer nach Symbol-Filter (%s/%s), Worker beendet.", sw.exchangeName, sw.marketType)
						return
					}
					continue
				}
				log.Printf("[CCXT-BATCH-SHARD-ERROR] watchTradesForSymbols fehlgeschlagen: %v. Warte 5s.", err)
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
				normalized, normErr := NormalizeTrade(trade, sw.exchangeName, sw.marketType, goTimestamp, ingestNow.UnixNano())
				if normErr != nil {
					log.Printf("[CCXT-BATCH-SHARD-WARN] normalize trade failed (%s/%s): %v", sw.exchangeName, sw.marketType, normErr)
					continue
				}
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

func (sw *BatchShardWorker) safeUnWatchTradesForSymbols(symbolsBatch []string) (_ interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in UnWatchTradesForSymbols: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return sw.exchange.UnWatchTradesForSymbols(symbolsBatch)
}

func (sw *BatchShardWorker) filterSupportedSymbols(symbols []string) []string {
	supported, err := getSupportedSymbols(sw.exchangeName, sw.marketType)
	if err != nil {
		log.Printf("[CCXT-BATCH-SHARD-WARN] supported symbol cache failed for %s/%s: %v", sw.exchangeName, sw.marketType, err)
		return symbols
	}
	if len(supported) == 0 {
		return symbols
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
