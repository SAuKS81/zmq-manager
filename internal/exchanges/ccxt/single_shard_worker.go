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
	activeWatchers map[string]context.CancelFunc
	activeCacheN   map[string]int
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
	}
}

func (sw *SingleWatchShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[CCXT-SINGLE-SHARD] Starte Worker fuer %s", sw.exchangeName)

	sw.exchange = createCCXTExchange(sw.exchangeName, sw.marketType)
	if sw.exchange == nil {
		log.Printf("[CCXT-SINGLE-SHARD] Instanz fuer %s konnte nicht erstellt werden.", sw.exchangeName)
		return
	}

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
	recycleNeeded := false
	startedWatchers := 0
	for symbol := range cmd.Symbols {
		cacheN := cmd.Symbols[symbol]
		switch cmd.Action {
		case "subscribe":
			sw.mu.Lock()
			if _, exists := sw.activeWatchers[symbol]; exists && sw.activeCacheN[symbol] == cacheN {
				sw.mu.Unlock()
				continue
			}
			if cancel, exists := sw.activeWatchers[symbol]; exists {
				cancel()
			}
			ctx, cancel := context.WithCancel(context.Background())
			sw.activeWatchers[symbol] = cancel
			sw.activeCacheN[symbol] = cacheN
			sw.mu.Unlock()
			go sw.runSingleWatch(ctx, symbol, cacheN)
			startedWatchers++
			time.Sleep(sw.config.SubscribePause)
		case "unsubscribe":
			sw.mu.Lock()
			cancel, exists := sw.activeWatchers[symbol]
			if exists {
				delete(sw.activeWatchers, symbol)
				delete(sw.activeCacheN, symbol)
			}
			sw.mu.Unlock()
			if !exists {
				continue
			}
			log.Printf("[CCXT-SINGLE-SHARD] Stoppe Watcher fuer %s.", symbol)
			if sw.config.SupportsTradeUnwatch || exchangeHasFeature(sw.exchangeName, sw.exchange, "unWatchTrades") {
				if _, err := sw.safeUnWatchTrades(symbol); err != nil {
					log.Printf("[CCXT-SINGLE-SHARD-WARN] UnWatchTrades('%s') fehlgeschlagen: %v. Fallback auf Shard-Recycle.", symbol, err)
					emitStatus(sw.statusCh, &shared_types.StreamStatusEvent{
						Type:       "stream_update_failed",
						Exchange:   sw.exchangeName,
						MarketType: sw.marketType,
						DataType:   "trades",
						Symbol:     symbol,
						Status:     "failed",
						Reason:     "unwatch_trades_failed",
						Message:    err.Error(),
					})
					recycleNeeded = true
				}
			} else {
				log.Printf("[CCXT-SINGLE-SHARD-INFO] Trade unwatch nicht freigegeben (%s/%s). Nutze harten Shard-Recycle.", sw.exchangeName, sw.marketType)
				recycleNeeded = true
			}
			cancel()
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
	if recycleNeeded {
		sw.recycleExchangeAndRestart()
	}
}

func (sw *SingleWatchShardWorker) runSingleWatch(ctx context.Context, symbol string, cacheN int) {
	attempt := 0
	reconnecting := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
			trades, err := sw.safeWatchTrades(symbol, cacheN)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				attempt++
				log.Printf("[CCXT-SINGLE-SHARD-ERROR] exchange=%s market_type=%s data_type=trades symbol=%s attempt=%d err=%v. Warte 5s.", sw.exchangeName, sw.marketType, symbol, attempt, err)
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
				time.Sleep(5 * time.Second)
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

func (sw *SingleWatchShardWorker) safeWatchTrades(symbol string, cacheN int) (trades []ccxtpro.Trade, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in WatchTrades: %v\n%s", r, string(debug.Stack()))
		}
	}()
	if cacheN > 0 {
		return sw.exchange.WatchTrades(symbol, ccxt.WithWatchTradesLimit(int64(cacheN)))
	}
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
	oldWatchers := make([]context.CancelFunc, 0, len(sw.activeWatchers))
	for symbol, cancel := range sw.activeWatchers {
		symbols = append(symbols, symbol)
		oldWatchers = append(oldWatchers, cancel)
	}
	sw.activeWatchers = make(map[string]context.CancelFunc)
	sw.mu.Unlock()

	for _, cancel := range oldWatchers {
		cancel()
	}
	closeCCXTExchange(sw.exchangeName, sw.marketType, sw.exchange)
	sw.exchange = createCCXTExchange(sw.exchangeName, sw.marketType)
	if sw.exchange == nil {
		log.Printf("[CCXT-SINGLE-SHARD-FATAL] Konnte Exchange nach Recycle fuer %s/%s nicht neu erstellen", sw.exchangeName, sw.marketType)
		return
	}

	for _, symbol := range symbols {
		ctx, cancel := context.WithCancel(context.Background())
		sw.mu.Lock()
		sw.activeWatchers[symbol] = cancel
		cacheN := sw.activeCacheN[symbol]
		sw.mu.Unlock()
		go sw.runSingleWatch(ctx, symbol, cacheN)
		time.Sleep(sw.config.SubscribePause)
	}
}
