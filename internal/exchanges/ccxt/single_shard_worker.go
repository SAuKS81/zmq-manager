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

// SingleWatchShardWorker verwaltet eine einzelne Verbindung fuer mehrere Einzel-Symbol-Streams.
type SingleWatchShardWorker struct {
	exchangeName   string
	marketType     string
	config         ExchangeConfig
	commandCh      chan ShardCommand
	stopCh         chan struct{}
	dataCh         chan<- *shared_types.TradeUpdate
	statusCh       chan<- *shared_types.StreamStatusEvent
	wg             *sync.WaitGroup
	mu             sync.Mutex
	exchange       ccxtpro.IExchange
	tradeLimit     int
	activeWatchers map[string]context.CancelFunc
	activeCacheN   map[string]int
	cacheLogged    map[string]bool
}

func NewSingleWatchShardWorker(exchangeName, marketType string, config ExchangeConfig, stopCh chan struct{}, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *SingleWatchShardWorker {
	return &SingleWatchShardWorker{
		exchangeName:   exchangeName,
		marketType:     marketType,
		config:         config,
		commandCh:      make(chan ShardCommand, 100),
		stopCh:         stopCh,
		dataCh:         dataCh,
		statusCh:       statusCh,
		wg:             wg,
		activeWatchers: make(map[string]context.CancelFunc),
		activeCacheN:   make(map[string]int),
		cacheLogged:    make(map[string]bool),
	}
}

func (sw *SingleWatchShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[CCXT-SINGLE-SHARD] Starte Worker fuer %s", sw.exchangeName)

	for {
		select {
		case cmd := <-sw.commandCh:
			sw.handleCommand(cmd)
		case <-sw.stopCh:
			log.Printf("[CCXT-SINGLE-SHARD] Stoppe Worker fuer %s. Beende alle %d Watcher...", sw.exchangeName, len(sw.activeWatchers))
			sw.mu.Lock()
			for _, cancel := range sw.activeWatchers {
				cancel()
			}
			sw.mu.Unlock()
			closeCCXTExchange(sw.exchangeName, sw.marketType, sw.exchange)
			return
		}
	}
}

func (sw *SingleWatchShardWorker) getCommandChannel() chan<- ShardCommand {
	return sw.commandCh
}

func (sw *SingleWatchShardWorker) handleCommand(cmd ShardCommand) {
	startedWatchers := 0
	for symbol, cacheN := range cmd.Symbols {
		switch cmd.Action {
		case "subscribe":
			sw.mu.Lock()
			sw.activeCacheN[symbol] = cacheN
			if _, exists := sw.activeWatchers[symbol]; exists {
				sw.mu.Unlock()
				continue
			}
			desiredTradeLimit := sw.maxActiveCacheNLocked()
			sw.mu.Unlock()
			if !sw.ensureTradeExchange(desiredTradeLimit) {
				sw.mu.Lock()
				delete(sw.activeCacheN, symbol)
				sw.mu.Unlock()
				continue
			}
			ctx, cancel := context.WithCancel(context.Background())
			sw.mu.Lock()
			sw.activeWatchers[symbol] = cancel
			sw.mu.Unlock()
			go sw.runSingleWatch(ctx, symbol)
			startedWatchers++
			time.Sleep(sw.config.SubscribePause)
		case "unsubscribe":
			if !sw.stopWatcher(symbol) {
				continue
			}
		}
	}
	if startedWatchers > 0 {
		log.Printf(
			"[CCXT-SINGLE-SHARD] %s/%s: %d Watcher auf existierender Verbindung gestartet.",
			sw.exchangeName,
			sw.marketType,
			startedWatchers,
		)
	}
}

func (sw *SingleWatchShardWorker) stopWatcher(symbol string) bool {
	sw.mu.Lock()
	cancel, exists := sw.activeWatchers[symbol]
	if exists {
		delete(sw.activeWatchers, symbol)
		delete(sw.activeCacheN, symbol)
	}
	sw.mu.Unlock()
	if !exists {
		return false
	}

	log.Printf("[CCXT-SINGLE-SHARD] Stoppe Watcher fuer %s.", symbol)
	cancel()
	sw.tryUnwatchTrade(symbol)
	return true
}

func (sw *SingleWatchShardWorker) runSingleWatch(ctx context.Context, symbol string) {
	attempt := 0
	reconnecting := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
			trades, err := sw.safeWatchTrades(symbol)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				attempt++
				delay := reconnectDelay(sw.config, attempt)
				log.Printf("[CCXT-SINGLE-SHARD-ERROR] exchange=%s market_type=%s data_type=trades symbol=%s attempt=%d err=%v. Warte %s.", sw.exchangeName, sw.marketType, symbol, attempt, err, delay)
				emitStatus(sw.statusCh, &shared_types.StreamStatusEvent{
					Type:       "stream_reconnecting",
					Exchange:   sw.exchangeName,
					MarketType: sw.marketType,
					DataType:   "trades",
					Symbol:     symbol,
					Status:     "reconnecting",
					Reason:     "watch_trades_failed",
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
					Symbol:     symbol,
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
				normalized, _ := NormalizeTrade(trade, sw.exchangeName, sw.marketType, goTimestamp, ingestNow.UnixNano())
				if normalized != nil {
					sw.logTradeCacheProofOnce(normalized.Symbol)
					select {
					case sw.dataCh <- normalized:
					default:
					}
				}
			}
		}
	}
}

func (sw *SingleWatchShardWorker) safeWatchTrades(symbol string) (trades []ccxtpro.Trade, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in WatchTrades: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return sw.exchange.WatchTrades(symbol)
}

func (sw *SingleWatchShardWorker) tryUnwatchTrade(symbol string) {
	if !(sw.config.SupportsTradeUnwatch || exchangeHasFeature(sw.exchangeName, sw.exchange, "unWatchTrades")) {
		return
	}
	if _, err := sw.safeUnWatchTrades(symbol); err != nil {
		log.Printf("[CCXT-SINGLE-SHARD-WARN] UnWatchTrades('%s') fehlgeschlagen (%s/%s): %v", symbol, sw.exchangeName, sw.marketType, err)
	}
}

func (sw *SingleWatchShardWorker) safeUnWatchTrades(symbol string) (_ interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in UnWatchTrades: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return sw.exchange.UnWatchTrades(symbol)
}

func (sw *SingleWatchShardWorker) maxActiveCacheNLocked() int {
	maxCacheN := 0
	for _, cacheN := range sw.activeCacheN {
		if cacheN > maxCacheN {
			maxCacheN = cacheN
		}
	}
	return maxCacheN
}

func (sw *SingleWatchShardWorker) ensureTradeExchange(desiredTradeLimit int) bool {
	if desiredTradeLimit <= 0 {
		desiredTradeLimit = 1
	}
	if sw.exchange != nil {
		return true
	}
	sw.exchange = createCCXTExchange(sw.exchangeName, sw.marketType, desiredTradeLimit)
	sw.tradeLimit = desiredTradeLimit
	if sw.exchange == nil {
		log.Printf("[CCXT-SINGLE-SHARD-FATAL] Konnte Exchange fuer %s/%s mit tradesLimit=%d nicht erstellen", sw.exchangeName, sw.marketType, desiredTradeLimit)
		return false
	}
	return true
}

func (sw *SingleWatchShardWorker) logTradeCacheProofOnce(symbol string) {
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
