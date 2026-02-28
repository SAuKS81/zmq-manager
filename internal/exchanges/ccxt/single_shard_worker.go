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
	wg             *sync.WaitGroup
	mu             sync.Mutex
	exchange       ccxtpro.IExchange
	activeWatchers map[string]context.CancelFunc
}

func NewSingleWatchShardWorker(exchangeName, marketType string, config ExchangeConfig, stopCh chan struct{}, dataCh chan<- *shared_types.TradeUpdate, wg *sync.WaitGroup) *SingleWatchShardWorker {
	return &SingleWatchShardWorker{
		exchangeName:   exchangeName,
		marketType:     marketType,
		config:         config,
		commandCh:      make(chan ShardCommand, 100),
		stopCh:         stopCh,
		dataCh:         dataCh,
		wg:             wg,
		activeWatchers: make(map[string]context.CancelFunc),
	}
}

func (sw *SingleWatchShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[CCXT-SINGLE-SHARD] Starte Worker fuer %s", sw.exchangeName)

	sw.exchange = ccxtpro.CreateExchange(sw.exchangeName, nil)
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
			return
		}
	}
}

func (sw *SingleWatchShardWorker) getCommandChannel() chan<- ShardCommand {
	return sw.commandCh
}

func (sw *SingleWatchShardWorker) handleCommand(cmd ShardCommand) {
	for symbol := range cmd.Symbols {
		switch cmd.Action {
		case "subscribe":
			sw.mu.Lock()
			if _, exists := sw.activeWatchers[symbol]; exists {
				sw.mu.Unlock()
				continue
			}
			log.Printf("[CCXT-SINGLE-SHARD] Starte Watcher fuer %s auf existierender Verbindung.", symbol)
			ctx, cancel := context.WithCancel(context.Background())
			sw.activeWatchers[symbol] = cancel
			sw.mu.Unlock()
			go sw.runSingleWatch(ctx, symbol)
			time.Sleep(sw.config.SubscribePause)
		case "unsubscribe":
			sw.mu.Lock()
			cancel, exists := sw.activeWatchers[symbol]
			if exists {
				delete(sw.activeWatchers, symbol)
			}
			sw.mu.Unlock()
			if !exists {
				continue
			}
			log.Printf("[CCXT-SINGLE-SHARD] Stoppe Watcher fuer %s.", symbol)
			if _, err := sw.safeUnWatchTrades(symbol); err != nil {
				log.Printf("[CCXT-SINGLE-SHARD-WARN] UnWatchTrades('%s') fehlgeschlagen: %v", symbol, err)
			}
			cancel()
		}
	}
}

func (sw *SingleWatchShardWorker) runSingleWatch(ctx context.Context, symbol string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			trades, err := sw.exchange.WatchTrades(symbol)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[CCXT-SINGLE-SHARD-ERROR] WatchTrades('%s'): %v. Warte 5s.", symbol, err)
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
					select {
					case sw.dataCh <- normalized:
					default:
					}
				}
			}
		}
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
