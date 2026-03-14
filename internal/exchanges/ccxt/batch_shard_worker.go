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
	ccxt "github.com/ccxt/ccxt/go/v4"
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
	statusCh      chan<- *shared_types.StreamStatusEvent
	wg            *sync.WaitGroup
	mu            sync.Mutex
	activeSymbols map[string]int
	exchange      ccxtpro.IExchange
	cancelWorkers context.CancelFunc
}

func NewBatchShardWorker(exchangeName, marketType string, config ExchangeConfig, stopCh chan struct{}, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *BatchShardWorker {
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
		statusCh:      statusCh,
		wg:            wg,
		activeSymbols: make(map[string]int),
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
			closeCCXTExchange(sw.exchangeName, sw.marketType, sw.exchange)
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
		for s, cacheN := range cmd.Symbols {
			currentCacheN, exists := sw.activeSymbols[s]
			if cmd.Action == "subscribe" && (!exists || currentCacheN != cacheN) {
				sw.activeSymbols[s] = cacheN
				listChanged = true
			} else if cmd.Action == "unsubscribe" && exists {
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

	hadActiveWorkers := sw.cancelWorkers != nil
	if sw.cancelWorkers != nil {
		sw.cancelWorkers()
		sw.cancelWorkers = nil
	}

	var symbolsToWatch []string
	for s := range sw.activeSymbols {
		symbolsToWatch = append(symbolsToWatch, s)
	}
	sw.mu.Unlock()

	recycledForListChange := false
	if hadActiveWorkers && sw.config.RecycleExchangeOnTradeChange {
		log.Printf("[CCXT-BATCH-SHARD-INFO] Recycle Exchange nach Listenwechsel (%s/%s), um parallele Trade-Batches auf derselben Instanz zu vermeiden.", sw.exchangeName, sw.marketType)
		sw.recycleExchange()
		recycledForListChange = true
	}

	if len(unsubscribeSymbols) > 0 && !recycledForListChange {
		symbolsToUnwatch := make([]string, 0, len(unsubscribeSymbols))
		for s := range unsubscribeSymbols {
			symbolsToUnwatch = append(symbolsToUnwatch, s)
		}
		if sw.config.SupportsTradeBatchUnwatch || exchangeHasFeature(sw.exchangeName, sw.exchange, "unWatchTradesForSymbols") {
			if _, err := sw.safeUnWatchTradesForSymbols(symbolsToUnwatch); err != nil {
				log.Printf("[CCXT-BATCH-SHARD-WARN] UnWatchTradesForSymbols(%d) fehlgeschlagen (%s/%s): %v. Fallback auf Shard-Recycle.", len(symbolsToUnwatch), sw.exchangeName, sw.marketType, err)
				emitStatus(sw.statusCh, &shared_types.StreamStatusEvent{
					Type:       "stream_update_failed",
					Exchange:   sw.exchangeName,
					MarketType: sw.marketType,
					DataType:   "trades",
					Symbols:    append([]string(nil), symbolsToUnwatch...),
					Status:     "failed",
					Reason:     "unwatch_trades_for_symbols_failed",
					Message:    err.Error(),
				})
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
	attempt := 0
	reconnecting := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
			trades, err := sw.safeWatchTradesForSymbols(currentBatch, sw.cacheNForBatch(currentBatch))
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				attempt++
				if missingSymbol, ok := extractMissingMarketSymbol(err); ok {
					log.Printf("[CCXT-BATCH-SHARD-WARN] Entferne ungueltiges Symbol '%s' aus Batch (%s/%s).", missingSymbol, sw.exchangeName, sw.marketType)
					emitStatus(sw.statusCh, &shared_types.StreamStatusEvent{
						Type:       "stream_update_failed",
						Exchange:   sw.exchangeName,
						MarketType: sw.marketType,
						DataType:   "trades",
						Symbol:     missingSymbol,
						Status:     "failed",
						Reason:     "missing_market_symbol",
						Message:    err.Error(),
						Attempt:    attempt,
					})
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
				log.Printf("[CCXT-BATCH-SHARD-ERROR] exchange=%s market_type=%s data_type=trades batch_size=%d symbols=%s attempt=%d err=%v. Warte 5s.", sw.exchangeName, sw.marketType, len(currentBatch), summarizeSymbols(currentBatch, 5), attempt, err)
				emitStatus(sw.statusCh, &shared_types.StreamStatusEvent{
					Type:       "stream_reconnecting",
					Exchange:   sw.exchangeName,
					MarketType: sw.marketType,
					DataType:   "trades",
					Symbols:    append([]string(nil), currentBatch...),
					Status:     "reconnecting",
					Reason:     "watch_trades_for_symbols_failed",
					Message:    err.Error(),
					Attempt:    attempt,
				})
				reconnecting = true
				time.Sleep(5 * time.Second)
				continue
			}
			if reconnecting {
				emitStatus(sw.statusCh, &shared_types.StreamStatusEvent{
					Type:       "stream_restored",
					Exchange:   sw.exchangeName,
					MarketType: sw.marketType,
					DataType:   "trades",
					Symbols:    append([]string(nil), currentBatch...),
					Status:     "running",
					Attempt:    attempt,
				})
				attempt = 0
				reconnecting = false
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

func (sw *BatchShardWorker) safeWatchTradesForSymbols(symbolsBatch []string, cacheN int) (trades []ccxtpro.Trade, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in WatchTradesForSymbols: %v\n%s", r, string(debug.Stack()))
		}
	}()
	if cacheN > 0 {
		return sw.exchange.WatchTradesForSymbols(symbolsBatch, ccxt.WithWatchTradesForSymbolsLimit(int64(cacheN)))
	}
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

func (sw *BatchShardWorker) cacheNForBatch(symbols []string) int {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	maxCacheN := 0
	for _, symbol := range symbols {
		if cacheN := sw.activeSymbols[symbol]; cacheN > maxCacheN {
			maxCacheN = cacheN
		}
	}
	return maxCacheN
}
