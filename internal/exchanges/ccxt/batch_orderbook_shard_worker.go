package ccxt

import (
	"context"
	"log"
	"sync"
	"time"

	"bybit-watcher/internal/shared_types"
	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
)

// BatchOrderBookShardWorker verwaltet eine einzelne WebSocket-Verbindung für `watchOrderBookForSymbols`.
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
	options := map[string]interface{}{"options": map[string]interface{}{"defaultType": marketType}}
	exchange := ccxtpro.CreateExchange(exchangeName, options)
	if exchange == nil {
		log.Printf("[CCXT-BATCH-OB-FATAL] Konnte Exchange-Instanz für %s nicht erstellen", exchangeName)
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
	log.Printf("[CCXT-BATCH-OB] Starte Worker für %s (%s)", sw.exchangeName, sw.marketType)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			sw.processCommandQueue()
		case <-sw.stopCh:
			log.Printf("[CCXT-BATCH-OB] Stoppe Worker für %s (%s)", sw.exchangeName, sw.marketType)
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
	for _, cmd := range commands {
		for s, depth := range cmd.Symbols {
			if cmd.Action == "subscribe" && sw.activeSymbols[s] == 0 {
				sw.activeSymbols[s] = depth
				listChanged = true
			} else if cmd.Action == "unsubscribe" && sw.activeSymbols[s] != 0 {
				delete(sw.activeSymbols, s)
				listChanged = true
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

	if len(symbolsToWatch) > 0 {
		var ctx context.Context
		ctx, sw.cancelWorkers = context.WithCancel(context.Background())

		// ======================================================================
		// KORREKTUR HIER: Die Logik zum Aufteilen in Batches wurde hinzugefügt.
		// ======================================================================
		log.Printf("[CCXT-BATCH-OB] Symbol-Liste geändert. Starte Watcher für %d Symbole in Batches von %d...", len(symbolsToWatch), sw.config.BatchSize)
		for i := 0; i < len(symbolsToWatch); i += sw.config.BatchSize {
			end := i + sw.config.BatchSize
			if end > len(symbolsToWatch) {
				end = len(symbolsToWatch)
			}
			batch := symbolsToWatch[i:end]
			log.Printf("[CCXT-BATCH-OB] Starte Goroutine für Batch mit %d Symbolen.", len(batch))
			go sw.runWorkerBatch(ctx, batch)
			// Kurze Pause zwischen den Batches, um Rate-Limits beim Verbindungsaufbau zu vermeiden
			time.Sleep(sw.config.SubscribePause)
		}
		// ======================================================================
	}
}

// Umbenannt zu runWorkerBatch, da die Funktion jetzt einen Batch verarbeitet.
func (sw *BatchOrderBookShardWorker) runWorkerBatch(ctx context.Context, symbolsBatch []string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Die Funktion verarbeitet jetzt den übergebenen Batch, nicht mehr die ganze Liste.
			orderbook, err := sw.exchange.WatchOrderBookForSymbols(symbolsBatch)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				log.Printf("[CCXT-BATCH-OB-ERROR] `watchOrderBookForSymbols` für Batch (%d Symbole) fehlgeschlagen: %v. Warte 5s.", len(symbolsBatch), err)
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

			normalized, _ := NormalizeOrderBook(orderbook, sw.exchangeName, sw.marketType, goTimestamp, ingestNow.UnixNano())
			if normalized != nil {
				sw.dataCh <- normalized
			}
		}
	}
}
