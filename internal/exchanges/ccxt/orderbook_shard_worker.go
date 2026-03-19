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
	statusCh       chan<- *shared_types.StreamStatusEvent
	wg             *sync.WaitGroup
	mu             sync.Mutex
	exchange       ccxtpro.IExchange
	activeWatchers map[string]context.CancelFunc
	activeDepths   map[string]int
}

func NewOrderBookShardWorker(exchangeName, marketType string, config ExchangeConfig, stopCh chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *OrderBookShardWorker {
	return &OrderBookShardWorker{
		exchangeName:   exchangeName,
		marketType:     marketType,
		config:         config,
		commandCh:      make(chan ShardCommand, 100),
		stopCh:         stopCh,
		dataCh:         dataCh,
		statusCh:       statusCh,
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
			closeCCXTExchange(sw.exchangeName, sw.marketType, sw.exchange)
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
			if sw.config.SupportsOrderBookUnwatch || exchangeHasFeature(sw.exchangeName, sw.exchange, "unWatchOrderBook") {
				if _, err := sw.safeUnWatchOrderBook(symbol); err != nil {
					log.Printf("[CCXT-OB-SHARD-WARN] UnWatchOrderBook('%s') fehlgeschlagen (%s/%s): %v", symbol, sw.exchangeName, sw.marketType, err)
				}
			}
			cancel()
		}
	}
}

func (sw *OrderBookShardWorker) runSingleWatch(ctx context.Context, symbol string, depth int) {
	attempt := 0
	reconnecting := false
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
				attempt++
				delay := reconnectDelay(sw.config, attempt)
				log.Printf("[CCXT-OB-SHARD-ERROR] exchange=%s market_type=%s data_type=orderbooks symbol=%s depth=%d attempt=%d err=%v. Warte %s.", sw.exchangeName, sw.marketType, symbol, depth, attempt, err, delay)
				emitStatus(sw.statusCh, &shared_types.StreamStatusEvent{
					Type:       "stream_reconnecting",
					Exchange:   sw.exchangeName,
					MarketType: sw.marketType,
					DataType:   "orderbooks",
					Symbol:     symbol,
					Status:     "reconnecting",
					Reason:     "watch_orderbook_failed",
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
					DataType:   "orderbooks",
					Symbol:     symbol,
					Status:     "running",
					Attempt:    attempt,
				})
				attempt = 0
				reconnecting = false
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
