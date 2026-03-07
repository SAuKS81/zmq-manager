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
	activeDepths   map[string]int
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
		activeDepths:   make(map[string]int),
	}
}

func (sw *OrderBookShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[CCXT-OB-SHARD] Starte Worker fuer %s", sw.exchangeName)
	sw.exchange = createCCXTExchange(sw.exchangeName, sw.marketType)
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
	recycleNeeded := false
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
			sw.activeDepths[symbol] = depth
			sw.mu.Unlock()
			go sw.runSingleWatch(ctx, symbol, depth)
			time.Sleep(sw.config.SubscribePause)
		case "unsubscribe":
			sw.mu.Lock()
			cancel, exists := sw.activeWatchers[symbol]
			if exists {
				delete(sw.activeWatchers, symbol)
				delete(sw.activeDepths, symbol)
			}
			sw.mu.Unlock()
			if !exists {
				continue
			}
			if sw.config.SupportsOrderBookUnwatch {
				if _, err := sw.safeUnWatchOrderBook(symbol); err != nil {
					log.Printf("[CCXT-OB-SHARD-WARN] UnWatchOrderBook('%s') fehlgeschlagen: %v. Fallback auf Shard-Recycle.", symbol, err)
					recycleNeeded = true
				}
			} else {
				log.Printf("[CCXT-OB-SHARD-INFO] Orderbook unwatch nicht freigegeben (%s/%s). Nutze harten Shard-Recycle.", sw.exchangeName, sw.marketType)
				recycleNeeded = true
			}
			cancel()
		}
	}
	if recycleNeeded {
		sw.recycleExchangeAndRestart()
	}
}

func (sw *OrderBookShardWorker) runSingleWatch(ctx context.Context, symbol string, depth int) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			orderbook, err := sw.safeWatchOrderBook(symbol, depth)
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
			normalized, normErr := NormalizeOrderBook(orderbook, sw.exchangeName, sw.marketType, goTimestamp, ingestNow.UnixNano())
			if normErr != nil {
				log.Printf("[CCXT-OB-SHARD-WARN] normalize orderbook failed (%s/%s, symbol=%s): %v", sw.exchangeName, sw.marketType, symbol, normErr)
				continue
			}
			if normalized != nil {
				select {
				case sw.dataCh <- normalized:
				default:
				}
			}
		}
	}
}

func (sw *OrderBookShardWorker) safeWatchOrderBook(symbol string, depth int) (orderbook ccxtpro.OrderBook, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in WatchOrderBook: %v\n%s", r, string(debug.Stack()))
		}
	}()
	if depth > 0 {
		return sw.exchange.WatchOrderBook(symbol, ccxt.WithWatchOrderBookLimit(int64(depth)))
	}
	return sw.exchange.WatchOrderBook(symbol)
}

func (sw *OrderBookShardWorker) safeUnWatchOrderBook(symbol string) (_ interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in UnWatchOrderBook: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return sw.exchange.UnWatchOrderBook(symbol)
}

func (sw *OrderBookShardWorker) recycleExchangeAndRestart() {
	sw.mu.Lock()
	type watcherState struct {
		symbol string
		depth  int
		cancel context.CancelFunc
	}
	states := make([]watcherState, 0, len(sw.activeWatchers))
	for symbol, cancel := range sw.activeWatchers {
		states = append(states, watcherState{symbol: symbol, depth: sw.activeDepths[symbol], cancel: cancel})
	}
	sw.activeWatchers = make(map[string]context.CancelFunc)
	sw.activeDepths = make(map[string]int)
	sw.mu.Unlock()

	for _, state := range states {
		state.cancel()
	}
	closeCCXTExchange(sw.exchangeName, sw.marketType, sw.exchange)
	sw.exchange = createCCXTExchange(sw.exchangeName, sw.marketType)
	if sw.exchange == nil {
		log.Printf("[CCXT-OB-SHARD-FATAL] Konnte Exchange nach Recycle fuer %s/%s nicht neu erstellen", sw.exchangeName, sw.marketType)
		return
	}

	for _, state := range states {
		ctx, cancel := context.WithCancel(context.Background())
		sw.mu.Lock()
		sw.activeWatchers[state.symbol] = cancel
		sw.activeDepths[state.symbol] = state.depth
		sw.mu.Unlock()
		go sw.runSingleWatch(ctx, state.symbol, state.depth)
		time.Sleep(sw.config.SubscribePause)
	}
}
