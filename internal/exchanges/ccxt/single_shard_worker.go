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

type tradeWatcher struct {
	cancel context.CancelFunc
	done   chan struct{}
}

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
	activeWatchers map[string]*tradeWatcher
	activeCacheN   map[string]int
	stopping       map[string]chan struct{}
	pendingStarts  map[string]int
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
		activeWatchers: make(map[string]*tradeWatcher),
		activeCacheN:   make(map[string]int),
		stopping:       make(map[string]chan struct{}),
		pendingStarts:  make(map[string]int),
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
			for _, watcher := range sw.activeWatchers {
				watcher.cancel()
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
	for symbol := range cmd.Symbols {
		cacheN := cmd.Symbols[symbol]
		switch cmd.Action {
		case "subscribe":
			sw.mu.Lock()
			sw.activeCacheN[symbol] = cacheN
			if _, exists := sw.activeWatchers[symbol]; exists {
				sw.mu.Unlock()
				continue
			}
			if _, stopping := sw.stopping[symbol]; stopping {
				sw.pendingStarts[symbol] = cacheN
				sw.mu.Unlock()
				continue
			}
			desiredTradeLimit := sw.maxActiveCacheNLocked()
			ctx, cancel := context.WithCancel(context.Background())
			watcher := &tradeWatcher{
				cancel: cancel,
				done:   make(chan struct{}),
			}
			sw.activeWatchers[symbol] = watcher
			sw.mu.Unlock()
			if !sw.ensureTradeExchange(desiredTradeLimit) {
				sw.mu.Lock()
				delete(sw.activeWatchers, symbol)
				delete(sw.activeCacheN, symbol)
				sw.mu.Unlock()
				cancel()
				continue
			}
			go sw.runSingleWatch(ctx, watcher.done, symbol)
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
	watcher, exists := sw.activeWatchers[symbol]
	if exists {
		delete(sw.activeWatchers, symbol)
		delete(sw.activeCacheN, symbol)
		sw.stopping[symbol] = watcher.done
		delete(sw.pendingStarts, symbol)
	}
	sw.mu.Unlock()
	if !exists {
		return false
	}

	log.Printf("[CCXT-SINGLE-SHARD] Stoppe Watcher fuer %s.", symbol)
	watcher.cancel()
	go sw.finalizeStoppedWatcher(symbol, watcher.done)
	return true
}

func (sw *SingleWatchShardWorker) finalizeStoppedWatcher(symbol string, done <-chan struct{}) {
	<-done

	sw.mu.Lock()
	if currentDone, exists := sw.stopping[symbol]; exists && currentDone == done {
		delete(sw.stopping, symbol)
	}
	cacheN, shouldRestart := sw.pendingStarts[symbol]
	if shouldRestart {
		delete(sw.pendingStarts, symbol)
	}
	sw.mu.Unlock()

	if shouldRestart {
		sw.commandCh <- ShardCommand{
			Action:  "subscribe",
			Symbols: map[string]int{symbol: cacheN},
		}
	}
}

func (sw *SingleWatchShardWorker) runSingleWatch(ctx context.Context, done chan struct{}, symbol string) {
	defer close(done)
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

func (sw *SingleWatchShardWorker) safeUnWatchTrades(symbol string) (_ interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in UnWatchTrades: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return sw.exchange.UnWatchTrades(symbol)
}

func (sw *SingleWatchShardWorker) recycleExchangeAndRestart() {
	sw.mu.Lock()
	symbols := make([]string, 0, len(sw.activeWatchers))
	oldWatchers := make([]*tradeWatcher, 0, len(sw.activeWatchers))
	for symbol, watcher := range sw.activeWatchers {
		symbols = append(symbols, symbol)
		oldWatchers = append(oldWatchers, watcher)
	}
	desiredTradeLimit := sw.maxActiveCacheNLocked()
	sw.activeWatchers = make(map[string]*tradeWatcher)
	sw.stopping = make(map[string]chan struct{})
	sw.pendingStarts = make(map[string]int)
	sw.mu.Unlock()

	for _, watcher := range oldWatchers {
		watcher.cancel()
	}
	closeCCXTExchange(sw.exchangeName, sw.marketType, sw.exchange)
	sw.exchange = createCCXTExchange(sw.exchangeName, sw.marketType, desiredTradeLimit)
	sw.tradeLimit = desiredTradeLimit
	if sw.exchange == nil {
		log.Printf("[CCXT-SINGLE-SHARD-FATAL] Konnte Exchange nach Recycle fuer %s/%s nicht neu erstellen", sw.exchangeName, sw.marketType)
		return
	}

	for _, symbol := range symbols {
		ctx, cancel := context.WithCancel(context.Background())
		watcher := &tradeWatcher{
			cancel: cancel,
			done:   make(chan struct{}),
		}
		sw.mu.Lock()
		sw.activeWatchers[symbol] = watcher
		sw.mu.Unlock()
		go sw.runSingleWatch(ctx, watcher.done, symbol)
		time.Sleep(sw.config.SubscribePause)
	}
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
