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
	statusCh      chan<- *shared_types.StreamStatusEvent
	wg            *sync.WaitGroup
	mu            sync.Mutex
	activeSymbols map[string]int
	exchange      ccxtpro.IExchange
	tradeLimit    int
	cancelWorkers context.CancelFunc
	cacheLogged   map[string]bool
}

func NewBatchShardWorker(exchangeName, marketType string, config ExchangeConfig, stopCh chan struct{}, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *BatchShardWorker {
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
		cacheLogged:   make(map[string]bool),
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

	if sw.cancelWorkers != nil {
		sw.cancelWorkers()
		sw.cancelWorkers = nil
	}

	var symbolsToWatch []string
	for s := range sw.activeSymbols {
		symbolsToWatch = append(symbolsToWatch, s)
	}
	desiredTradeLimit := sw.maxActiveCacheNLocked()
	sw.mu.Unlock()

	if !sw.ensureTradeExchange(desiredTradeLimit) {
		return
	}
	if len(unsubscribeSymbols) > 0 {
		symbolsToUnwatch := make([]string, 0, len(unsubscribeSymbols))
		for s := range unsubscribeSymbols {
			symbolsToUnwatch = append(symbolsToUnwatch, s)
		}
		if sw.config.SupportsTradeBatchUnwatch || exchangeHasFeature(sw.exchangeName, sw.exchange, "unWatchTradesForSymbols") {
			if _, err := sw.safeUnWatchTradesForSymbols(symbolsToUnwatch); err != nil {
				log.Printf("[CCXT-BATCH-SHARD-WARN] UnWatchTradesForSymbols(%d) fehlgeschlagen (%s/%s): %v", len(symbolsToUnwatch), sw.exchangeName, sw.marketType, err)
			}
		}
	}

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
	sw.recycleExchangeWithTradeLimit(sw.tradeLimit)
}

func (sw *BatchShardWorker) recycleExchangeWithTradeLimit(tradeLimit int) {
	closeCCXTExchange(sw.exchangeName, sw.marketType, sw.exchange)
	sw.exchange = createCCXTExchange(sw.exchangeName, sw.marketType, tradeLimit)
	sw.tradeLimit = tradeLimit
	if sw.exchange == nil {
		log.Printf("[CCXT-BATCH-SHARD-FATAL] Konnte Exchange nach Recycle fuer %s/%s mit tradesLimit=%d nicht neu erstellen", sw.exchangeName, sw.marketType, tradeLimit)
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
			trades, err := sw.safeWatchTradesForSymbols(currentBatch)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				attempt++
				delay := reconnectDelay(sw.config, attempt)
				log.Printf("[CCXT-BATCH-SHARD-ERROR] exchange=%s market_type=%s data_type=trades batch_size=%d symbols=%s attempt=%d err=%v. Warte %s.", sw.exchangeName, sw.marketType, len(currentBatch), summarizeSymbols(currentBatch, 5), attempt, err, delay)
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
				if !sleepWithContext(ctx, delay) {
					return
				}
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
					sw.logTradeCacheProofOnce(normalized.Symbol)
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

func (sw *BatchShardWorker) maxActiveCacheNLocked() int {
	maxCacheN := 0
	for _, cacheN := range sw.activeSymbols {
		if cacheN > maxCacheN {
			maxCacheN = cacheN
		}
	}
	return maxCacheN
}

func (sw *BatchShardWorker) ensureTradeExchange(desiredTradeLimit int) bool {
	if desiredTradeLimit <= 0 {
		desiredTradeLimit = 1
	}
	if sw.exchange != nil {
		return true
	}
	sw.recycleExchangeWithTradeLimit(desiredTradeLimit)
	return sw.exchange != nil
}

func (sw *BatchShardWorker) logTradeCacheProofOnce(symbol string) {
	sw.mu.Lock()
	if sw.cacheLogged[symbol] {
		sw.mu.Unlock()
		return
	}
	sw.mu.Unlock()

	snapshot, err := inspectTradeCache(sw.exchange, symbol)
	if err != nil {
		log.Printf("[CCXT-TRADE-CACHE] exchange=%s market_type=%s symbol=%s inspect_failed=%v", sw.exchangeName, sw.marketType, symbol, err)
		return
	}

	sw.mu.Lock()
	sw.cacheLogged[symbol] = true
	sw.mu.Unlock()

	log.Printf(
		"[CCXT-TRADE-CACHE] exchange=%s market_type=%s symbol=%s max_size=%d current_len=%d cache_type=%s",
		sw.exchangeName,
		sw.marketType,
		symbol,
		snapshot.MaxSize,
		snapshot.CurrentLen,
		snapshot.CacheType,
	)
}
