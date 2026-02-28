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

type OrderBookShardWorker struct {
	exchangeName   string
	marketType     string
	config         ExchangeConfig
	commandCh      chan ShardCommand
	stopCh         chan struct{}
	dataCh         chan<- *shared_types.OrderBookUpdate
	wg             *sync.WaitGroup
	mu             sync.Mutex
	exchange       ccxtpro.IExchange
	activeWatchers map[string]context.CancelFunc
}

func NewOrderBookShardWorker(exchangeName, marketType string, config ExchangeConfig, stopCh chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, wg *sync.WaitGroup) *OrderBookShardWorker {
	return &OrderBookShardWorker{
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

func (sw *OrderBookShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[CCXT-OB-SHARD] Starte Worker fuer %s", sw.exchangeName)
	options := makeExchangeOptions(sw.exchangeName, sw.marketType)
	sw.exchange = ccxtpro.CreateExchange(sw.exchangeName, options)
	if sw.exchange == nil {
		log.Printf("[CCXT-OB-SHARD] Instanz fuer %s konnte nicht erstellt werden.", sw.exchangeName)
		return
	}
	for {
		select {
		case cmd := <-sw.commandCh:
			sw.handleCommand(cmd)
		case <-sw.stopCh:
			log.Printf("[CCXT-OB-SHARD] Stoppe Worker fuer %s. Beende alle %d Watcher...", sw.exchangeName, len(sw.activeWatchers))
			sw.mu.Lock()
			for _, cancel := range sw.activeWatchers {
				cancel()
			}
			sw.mu.Unlock()
			return
		}
	}
}

func (sw *OrderBookShardWorker) getCommandChannel() chan<- ShardCommand {
	return sw.commandCh
}

func (sw *OrderBookShardWorker) handleCommand(cmd ShardCommand) {
	for symbol, depth := range cmd.Symbols {
		switch cmd.Action {
		case "subscribe":
			sw.mu.Lock()
			if _, exists := sw.activeWatchers[symbol]; exists {
				sw.mu.Unlock()
				continue
			}
			ctx, cancel := context.WithCancel(context.Background())
			sw.activeWatchers[symbol] = cancel
			sw.mu.Unlock()
			go sw.runSingleWatch(ctx, symbol, depth)
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
			if _, err := sw.safeUnWatchOrderBook(symbol); err != nil {
				log.Printf("[CCXT-OB-SHARD-WARN] UnWatchOrderBook('%s') fehlgeschlagen: %v", symbol, err)
			}
			cancel()
		}
	}
}

func (sw *OrderBookShardWorker) runSingleWatch(ctx context.Context, symbol string, depth int) {
	_ = depth
	for {
		select {
		case <-ctx.Done():
			return
		default:
			orderbook, err := sw.exchange.WatchOrderBook(symbol)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[CCXT-OB-SHARD-ERROR] WatchOrderBook('%s'): %v. Warte 5s.", symbol, err)
				time.Sleep(5 * time.Second)
				continue
			}
			ingestNow := time.Now()
			goTimestamp := ingestNow.UnixMilli()
			normalized, _ := NormalizeOrderBook(orderbook, sw.exchangeName, sw.marketType, goTimestamp, ingestNow.UnixNano())
			if normalized != nil {
				select {
				case sw.dataCh <- normalized:
				default:
				}
			}
		}
	}
}

func (sw *OrderBookShardWorker) safeUnWatchOrderBook(symbol string) (_ interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in UnWatchOrderBook: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return sw.exchange.UnWatchOrderBook(symbol)
}
