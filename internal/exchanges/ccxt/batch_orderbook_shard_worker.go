//go:build ccxt
// +build ccxt

package ccxt

import (
	"context"
	"fmt"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"bybit-watcher/internal/shared_types"
	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
)

// BatchOrderBookShardWorker verwaltet eine einzelne WebSocket-Verbindung fuer watchOrderBookForSymbols.
type BatchOrderBookShardWorker struct {
	exchangeName  string
	marketType    string
	config        ExchangeConfig
	commandCh     chan ShardCommand
	stopCh        chan struct{}
	dataCh        chan<- *shared_types.OrderBookUpdate
	statusCh      chan<- *shared_types.StreamStatusEvent
	wg            *sync.WaitGroup
	mu            sync.Mutex
	activeSymbols map[string]int
	exchange      ccxtpro.IExchange
	cancelWorkers context.CancelFunc
	activeWorkers atomic.Int64
}

func NewBatchOrderBookShardWorker(exchangeName, marketType string, config ExchangeConfig, stopCh chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *BatchOrderBookShardWorker {
	exchange := createCCXTExchange(exchangeName, marketType)
	if exchange == nil {
		log.Printf("[CCXT-BATCH-OB-FATAL] Konnte Exchange-Instanz fuer %s nicht erstellen", exchangeName)
		return nil
	}
	return &BatchOrderBookShardWorker{
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

func (sw *BatchOrderBookShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[CCXT-BATCH-OB] Starte Worker fuer %s (%s)", sw.exchangeName, sw.marketType)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sw.processCommandQueue()
		case <-sw.stopCh:
			log.Printf("[CCXT-BATCH-OB] Stoppe Worker fuer %s (%s)", sw.exchangeName, sw.marketType)
			if sw.cancelWorkers != nil {
				sw.cancelWorkers()
				sw.cancelWorkers = nil
			}
			closeCCXTExchange(sw.exchangeName, sw.marketType, sw.exchange)
			if !waitForActiveWorkers(&sw.activeWorkers, workerStopForceTimeout) {
				log.Printf("[CCXT-BATCH-OB-WARN] Worker fuer %s/%s beenden sich nach Stop nicht rechtzeitig (active=%d).", sw.exchangeName, sw.marketType, sw.activeWorkers.Load())
			}
			return
		}
	}
}

func (sw *BatchOrderBookShardWorker) getCommandChannel() chan<- ShardCommand {
	return sw.commandCh
}

func (sw *BatchOrderBookShardWorker) processCommandQueue() {
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
		for s, depth := range cmd.Symbols {
			if cmd.Action == "subscribe" && sw.activeSymbols[s] == 0 {
				sw.activeSymbols[s] = depth
				listChanged = true
			} else if cmd.Action == "unsubscribe" && sw.activeSymbols[s] != 0 {
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

	forceRecycle := false
	if sw.cancelWorkers != nil {
		sw.cancelWorkers()
		sw.cancelWorkers = nil
		if !waitForActiveWorkers(&sw.activeWorkers, workerStopGracePeriod) {
			log.Printf("[CCXT-BATCH-OB-WARN] Reconfigure timeout fuer %s/%s (active_workers=%d). Recycle Exchange, um haengende WatchOrderBookForSymbols-Goroutinen abzuraeumen.", sw.exchangeName, sw.marketType, sw.activeWorkers.Load())
			closeCCXTExchange(sw.exchangeName, sw.marketType, sw.exchange)
			sw.exchange = nil
			forceRecycle = true
			if !waitForActiveWorkers(&sw.activeWorkers, workerStopForceTimeout) {
				log.Printf("[CCXT-BATCH-OB-WARN] Alte Worker fuer %s/%s laufen nach Force-Recycle weiter (active=%d). Sie sollten nach dem Close auslaufen.", sw.exchangeName, sw.marketType, sw.activeWorkers.Load())
			}
		}
	}

	if sw.exchange == nil {
		sw.exchange = createCCXTExchange(sw.exchangeName, sw.marketType)
		if sw.exchange == nil {
			log.Printf("[CCXT-BATCH-OB-FATAL] Konnte Exchange fuer %s/%s nach Reconfigure nicht erstellen", sw.exchangeName, sw.marketType)
			return
		}
	}

	if len(unsubscribeSymbols) > 0 && !forceRecycle {
		symbolsToUnwatch := make([]string, 0, len(unsubscribeSymbols))
		for s := range unsubscribeSymbols {
			symbolsToUnwatch = append(symbolsToUnwatch, s)
		}
		if sw.config.SupportsOrderBookBatchUnwatch || exchangeHasFeature(sw.exchangeName, sw.exchange, "unWatchOrderBookForSymbols") {
			if _, err := sw.safeUnWatchOrderBookForSymbols(symbolsToUnwatch); err != nil {
				log.Printf("[CCXT-BATCH-OB-WARN] UnWatchOrderBookForSymbols(%d) fehlgeschlagen (%s/%s): %v", len(symbolsToUnwatch), sw.exchangeName, sw.marketType, err)
			}
		}
	}

	if len(symbolsToWatch) > 0 {
		var ctx context.Context
		ctx, sw.cancelWorkers = context.WithCancel(context.Background())
		exchange := sw.exchange
		log.Printf("[CCXT-BATCH-OB] Symbol-Liste geaendert. Starte Watcher fuer %d Symbole in Batches von %d...", len(symbolsToWatch), sw.config.BatchSize)
		for i := 0; i < len(symbolsToWatch); i += sw.config.BatchSize {
			end := i + sw.config.BatchSize
			if end > len(symbolsToWatch) {
				end = len(symbolsToWatch)
			}
			batch := symbolsToWatch[i:end]
			log.Printf("[CCXT-BATCH-OB] Starte Goroutine fuer Batch mit %d Symbolen.", len(batch))
			sw.activeWorkers.Add(1)
			go sw.runWorkerBatch(ctx, exchange, batch)
			time.Sleep(sw.config.SubscribePause)
		}
	} else {
		sw.cancelWorkers = nil
	}
}

func (sw *BatchOrderBookShardWorker) recycleExchange() {
	closeCCXTExchange(sw.exchangeName, sw.marketType, sw.exchange)
	sw.exchange = createCCXTExchange(sw.exchangeName, sw.marketType)
	if sw.exchange == nil {
		log.Printf("[CCXT-BATCH-OB-FATAL] Konnte Exchange nach Recycle fuer %s/%s nicht neu erstellen", sw.exchangeName, sw.marketType)
	}
}

func (sw *BatchOrderBookShardWorker) runWorkerBatch(ctx context.Context, exchange ccxtpro.IExchange, symbolsBatch []string) {
	defer sw.activeWorkers.Add(-1)
	currentBatch := append([]string(nil), symbolsBatch...)
	attempt := 0
	reconnecting := false
	for {
		select {
		case <-ctx.Done():
			return
		default:
			orderbook, err := sw.safeWatchOrderBookForSymbols(exchange, currentBatch)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				attempt++
				delay := reconnectDelay(sw.config, attempt)
				log.Printf("[CCXT-BATCH-OB-ERROR] exchange=%s market_type=%s data_type=orderbooks batch_size=%d symbols=%s attempt=%d err=%v. Warte %s.", sw.exchangeName, sw.marketType, len(currentBatch), summarizeSymbols(currentBatch, 5), attempt, err, delay)
				emitStatus(sw.statusCh, &shared_types.StreamStatusEvent{
					Type:       "stream_reconnecting",
					Exchange:   sw.exchangeName,
					MarketType: sw.marketType,
					DataType:   "orderbooks",
					Symbols:    append([]string(nil), currentBatch...),
					Status:     "reconnecting",
					Reason:     "watch_orderbook_for_symbols_failed",
					Message:    err.Error(),
					Attempt:    attempt,
				})
				reconnecting = true
				if !sleepWithContext(ctx, delay) {
					return
				}
				continue
			}
			if ctx.Err() != nil {
				return
			}
			if reconnecting {
				emitStatus(sw.statusCh, &shared_types.StreamStatusEvent{
					Type:       "stream_restored",
					Exchange:   sw.exchangeName,
					MarketType: sw.marketType,
					DataType:   "orderbooks",
					Symbols:    append([]string(nil), currentBatch...),
					Status:     "running",
					Attempt:    attempt,
				})
				attempt = 0
				reconnecting = false
			}

			ingestNow := time.Now()
			goTimestamp := ingestNow.UnixMilli()
			if orderbook.Symbol == nil {
				continue
			}

			sw.mu.Lock()
			requestedDepth := sw.activeSymbols[*orderbook.Symbol]
			sw.mu.Unlock()
			if requestedDepth > 0 {
				if len(orderbook.Bids) > requestedDepth {
					orderbook.Bids = orderbook.Bids[:requestedDepth]
				}
				if len(orderbook.Asks) > requestedDepth {
					orderbook.Asks = orderbook.Asks[:requestedDepth]
				}
			}

			normalized, normErr := NormalizeOrderBook(orderbook, sw.exchangeName, sw.marketType, goTimestamp, ingestNow.UnixNano())
			if normErr != nil {
				log.Printf("[CCXT-BATCH-OB-WARN] normalize orderbook failed (%s/%s): %v", sw.exchangeName, sw.marketType, normErr)
				continue
			}
			if normalized != nil {
				if !sw.sendOrderBookUpdate(ctx, normalized) {
					return
				}
			}
		}
	}
}

func (sw *BatchOrderBookShardWorker) safeWatchOrderBookForSymbols(exchange ccxtpro.IExchange, symbolsBatch []string) (orderbook ccxtpro.OrderBook, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in WatchOrderBookForSymbols: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return exchange.WatchOrderBookForSymbols(symbolsBatch)
}

func (sw *BatchOrderBookShardWorker) safeUnWatchOrderBookForSymbols(symbolsBatch []string) (_ interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in UnWatchOrderBookForSymbols: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return sw.exchange.UnWatchOrderBookForSymbols(symbolsBatch)
}

func (sw *BatchOrderBookShardWorker) sendOrderBookUpdate(ctx context.Context, normalized *shared_types.OrderBookUpdate) bool {
	select {
	case sw.dataCh <- normalized:
		return true
	case <-ctx.Done():
		return false
	}
}
