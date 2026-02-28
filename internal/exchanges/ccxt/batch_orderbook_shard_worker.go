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

// BatchOrderBookShardWorker verwaltet eine einzelne WebSocket-Verbindung fuer watchOrderBookForSymbols.
type BatchOrderBookShardWorker struct {
	exchangeName  string
	marketType    string
	config        ExchangeConfig
	commandCh     chan ShardCommand
	stopCh        chan struct{}
	dataCh        chan<- *shared_types.OrderBookUpdate
	wg            *sync.WaitGroup
	mu            sync.Mutex
	activeSymbols map[string]int
	exchange      ccxtpro.IExchange
	cancelWorkers context.CancelFunc
}

func NewBatchOrderBookShardWorker(exchangeName, marketType string, config ExchangeConfig, stopCh chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, wg *sync.WaitGroup) *BatchOrderBookShardWorker {
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

	if len(unsubscribeSymbols) > 0 {
		symbolsToUnwatch := make([]string, 0, len(unsubscribeSymbols))
		for s := range unsubscribeSymbols {
			symbolsToUnwatch = append(symbolsToUnwatch, s)
		}
		if sw.config.SupportsOrderBookBatchUnwatch {
			if _, err := sw.safeUnWatchOrderBookForSymbols(symbolsToUnwatch); err != nil {
				log.Printf("[CCXT-BATCH-OB-WARN] UnWatchOrderBookForSymbols(%d) fehlgeschlagen (%s/%s): %v. Fallback auf Shard-Recycle.", len(symbolsToUnwatch), sw.exchangeName, sw.marketType, err)
				sw.recycleExchange()
			}
		} else {
			log.Printf("[CCXT-BATCH-OB-INFO] Batch orderbook unwatch nicht freigegeben (%s/%s). Nutze harten Shard-Recycle fuer %d Symbole.", sw.exchangeName, sw.marketType, len(symbolsToUnwatch))
			sw.recycleExchange()
		}
	}

	symbolsToWatch = sw.filterSupportedSymbols(symbolsToWatch)
	if len(symbolsToWatch) > 0 {
		var ctx context.Context
		ctx, sw.cancelWorkers = context.WithCancel(context.Background())
		log.Printf("[CCXT-BATCH-OB] Symbol-Liste geaendert. Starte Watcher fuer %d Symbole in Batches von %d...", len(symbolsToWatch), sw.config.BatchSize)
		for i := 0; i < len(symbolsToWatch); i += sw.config.BatchSize {
			end := i + sw.config.BatchSize
			if end > len(symbolsToWatch) {
				end = len(symbolsToWatch)
			}
			batch := symbolsToWatch[i:end]
			log.Printf("[CCXT-BATCH-OB] Starte Goroutine fuer Batch mit %d Symbolen.", len(batch))
			go sw.runWorkerBatch(ctx, batch)
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

func (sw *BatchOrderBookShardWorker) runWorkerBatch(ctx context.Context, symbolsBatch []string) {
	currentBatch := append([]string(nil), symbolsBatch...)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			orderbook, err := sw.safeWatchOrderBookForSymbols(currentBatch)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				if missingSymbol, ok := extractMissingMarketSymbol(err); ok {
					log.Printf("[CCXT-BATCH-OB-WARN] Entferne ungueltiges Symbol '%s' aus Batch (%s/%s).", missingSymbol, sw.exchangeName, sw.marketType)
					currentBatch = removeSymbolFromBatch(currentBatch, missingSymbol)
					sw.mu.Lock()
					delete(sw.activeSymbols, missingSymbol)
					sw.mu.Unlock()
					if len(currentBatch) == 0 {
						log.Printf("[CCXT-BATCH-OB-WARN] Batch leer nach Symbol-Filter (%s/%s), Worker beendet.", sw.exchangeName, sw.marketType)
						return
					}
					continue
				}
				log.Printf("[CCXT-BATCH-OB-ERROR] watchOrderBookForSymbols fuer Batch (%d Symbole) fehlgeschlagen: %v. Warte 5s.", len(currentBatch), err)
				time.Sleep(5 * time.Second)
				continue
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
				sw.dataCh <- normalized
			}
		}
	}
}

func (sw *BatchOrderBookShardWorker) safeWatchOrderBookForSymbols(symbolsBatch []string) (orderbook ccxtpro.OrderBook, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in WatchOrderBookForSymbols: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return sw.exchange.WatchOrderBookForSymbols(symbolsBatch)
}

func (sw *BatchOrderBookShardWorker) safeUnWatchOrderBookForSymbols(symbolsBatch []string) (_ interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic in UnWatchOrderBookForSymbols: %v\n%s", r, string(debug.Stack()))
		}
	}()
	return sw.exchange.UnWatchOrderBookForSymbols(symbolsBatch)
}

func (sw *BatchOrderBookShardWorker) filterSupportedSymbols(symbols []string) []string {
	supported, err := getSupportedSymbols(sw.exchangeName, sw.marketType)
	if err != nil {
		log.Printf("[CCXT-BATCH-OB-WARN] supported symbol cache failed for %s/%s: %v", sw.exchangeName, sw.marketType, err)
		return symbols
	}
	if len(supported) == 0 {
		return symbols
	}

	filtered := make([]string, 0, len(symbols))
	removed := 0
	for _, symbol := range symbols {
		if _, ok := supported[symbol]; ok {
			filtered = append(filtered, symbol)
			continue
		}
		removed++
		sw.mu.Lock()
		delete(sw.activeSymbols, symbol)
		sw.mu.Unlock()
	}

	if removed > 0 {
		log.Printf("[CCXT-BATCH-OB-WARN] dropped %d unsupported symbols for %s/%s", removed, sw.exchangeName, sw.marketType)
	}

	return filtered
}
