//go:build ccxt
// +build ccxt

// --- START OF FILE internal/exchanges/ccxt/single_shard_worker.go.txt ---

package ccxt

import (
	"context"
	"log"
	"sync"
	"time"

	"bybit-watcher/internal/shared_types"
	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
)

// SingleWatchShardWorker verwaltet eine einzelne Verbindung für mehrere Einzel-Symbol-Streams.
type SingleWatchShardWorker struct {
	exchangeName string
	marketType   string
	// NEU: Der Worker benötigt die Konfiguration für die Pausen.
	config         ExchangeConfig
	commandCh      chan ShardCommand
	stopCh         chan struct{}
	dataCh         chan<- *shared_types.TradeUpdate
	wg             *sync.WaitGroup
	mu             sync.Mutex
	exchange       ccxtpro.IExchange
	activeWatchers map[string]context.CancelFunc
}

// ÄNDERUNG: Die Signatur des Konstruktors wird um 'config' erweitert.
func NewSingleWatchShardWorker(exchangeName, marketType string, config ExchangeConfig, stopCh chan struct{}, dataCh chan<- *shared_types.TradeUpdate, wg *sync.WaitGroup) *SingleWatchShardWorker {
	return &SingleWatchShardWorker{
		exchangeName:   exchangeName,
		marketType:     marketType,
		config:         config, // NEU: Konfiguration speichern.
		commandCh:      make(chan ShardCommand, 100),
		stopCh:         stopCh,
		dataCh:         dataCh,
		wg:             wg,
		activeWatchers: make(map[string]context.CancelFunc),
	}
}

func (sw *SingleWatchShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[CCXT-SINGLE-SHARD] Starte Worker für %s", sw.exchangeName)

	sw.exchange = ccxtpro.CreateExchange(sw.exchangeName, nil)
	if sw.exchange == nil {
		log.Printf("[CCXT-SINGLE-SHARD] Instanz für %s konnte nicht erstellt werden.", sw.exchangeName)
		return
	}

	for {
		select {
		case cmd := <-sw.commandCh:
			sw.handleCommand(cmd)
		case <-sw.stopCh:
			log.Printf("[CCXT-SINGLE-SHARD] Stoppe Worker für %s. Beende alle %d Watcher...", sw.exchangeName, len(sw.activeWatchers))
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
	// KORREKTUR HIER: Wir iterieren über die Schlüssel der Map.
	for symbol := range cmd.Symbols {
		sw.mu.Lock()
		if cmd.Action == "subscribe" {
			if _, exists := sw.activeWatchers[symbol]; exists {
				sw.mu.Unlock()
				continue
			}
			log.Printf("[CCXT-SINGLE-SHARD] Starte Watcher für %s auf existierender Verbindung.", symbol)
			ctx, cancel := context.WithCancel(context.Background())
			sw.activeWatchers[symbol] = cancel
			go sw.runSingleWatch(ctx, symbol)
		} else if cmd.Action == "unsubscribe" {
			if cancel, exists := sw.activeWatchers[symbol]; exists {
				log.Printf("[CCXT-SINGLE-SHARD] Stoppe Watcher für %s.", symbol)
				cancel()
				delete(sw.activeWatchers, symbol)
			}
		}
		sw.mu.Unlock()

		if cmd.Action == "subscribe" {
			time.Sleep(sw.config.SubscribePause)
		}
	}
}

// Die runSingleWatch Funktion muss ebenfalls das nicht-blockierende Senden verwenden,
// um gegen die fehlerhafte KuCoin-Bibliothek immun zu sein.
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
					// KORREKTUR: Auch hier muss nicht-blockierend gesendet werden,
					// um einen Rückstau zu verhindern, falls kein Logger läuft.
					select {
					case sw.dataCh <- normalized:
						// Erfolgreich
					default:
						// Verwerfen, um den Worker nicht zu blockieren.
						// Ein globaler Zähler wäre hier besser, aber für den Moment
						// ist das Verhindern des Absturzes wichtiger.
					}
				}
			}
		}
	}
}

