package main

import (
	"log"
	"sort"
	"sync"
	"time"

	ccxt "github.com/ccxt/ccxt/go/v4"
	pro "github.com/ccxt/ccxt/go/v4/pro"
)

const (
	EXCHANGE_NAME      = "kucoin"
	MAX_SYMBOLS_TO_SUB = 1000
	SYMBOLS_PER_SHARD  = 300
	SUBSCRIBE_PAUSE    = 200 * time.Millisecond
	NEW_SHARD_PAUSE    = 1100 * time.Millisecond

	// NEUER PARAMETER: Kapazität des zentralen Trade-Kanals.
	// 1 bedeutet, dass es quasi keinen Puffer gibt (maximale Backpressure).
	TRADE_CHANNEL_CAPACITY = 1

	STATS_INTERVAL = 10 * time.Second
)

// Die Stats-Struktur und ihre Methoden bleiben unverändert.
type Stats struct {
	mutex                sync.Mutex
	tradeCounter         int
	totalTradesProcessed int64
	e2eLatencies         []int64
}

func (s *Stats) calculateAndLogStats() {
	s.mutex.Lock()
	countInWindow := s.tradeCounter
	totalCount := s.totalTradesProcessed
	latencies := make([]int64, len(s.e2eLatencies))
	copy(latencies, s.e2eLatencies)
	s.tradeCounter = 0
	s.e2eLatencies = []int64{}
	s.mutex.Unlock()

	tradesPerSec := float64(countInWindow) / STATS_INTERVAL.Seconds()
	log.Printf("[STATS] Rate: %d in %v (%.2f/s) | Gesamt: %d",
		countInWindow, STATS_INTERVAL, tradesPerSec, totalCount)

	if len(latencies) > 0 {
		sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
		var sum int64
		for _, lat := range latencies { sum += lat }
		avg := float64(sum) / float64(len(latencies))
		min := latencies[0]
		max := latencies[len(latencies)-1]
		p95 := latencies[int(float64(len(latencies))*0.95)]
		p99 := latencies[int(float64(len(latencies))*0.99)]
		log.Printf("        └─ E2E Latenz (ms): min=%d, avg=%.2f, p95=%d, p99=%d, max=%d",
			min, avg, p95, p99, max)
	}
}

func logStatsPeriodically(stats *Stats, wg *sync.WaitGroup) {
	defer wg.Done()
	ticker := time.NewTicker(STATS_INTERVAL)
	defer ticker.Stop()
	for range ticker.C {
		stats.calculateAndLogStats()
	}
}

// watchShard bleibt unverändert.
func watchShard(shardID int, symbols []string, tradeCh chan<- pro.Trade, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("[Shard %d] Starte für %d Symbole...", shardID, len(symbols))
	exchange := pro.CreateExchange(EXCHANGE_NAME, nil)
	if exchange == nil { log.Printf("[Shard %d] FEHLER: Konnte Exchange-Instanz nicht erstellen.", shardID); return }
	var shardWg sync.WaitGroup
	for _, symbol := range symbols {
		shardWg.Add(1)
		go func(s string) {
			defer shardWg.Done()
			for {
				trades, err := exchange.WatchTrades(s)
				if err != nil {
					log.Printf("[Shard %d] FEHLER bei WatchTrades für %s: %v. Warte 5s...", shardID, s, err)
					time.Sleep(5 * time.Second)
					continue
				}
				for _, trade := range trades {
					tradeCh <- trade // Diese Zeile wird nun blockieren, wenn der Kanal voll ist.
				}
			}
		}(symbol)
		log.Printf("[Shard %d] Symbol %s abonniert. Warte %v...", shardID, symbol, SUBSCRIBE_PAUSE)
		time.Sleep(SUBSCRIBE_PAUSE)
	}
	log.Printf("[Shard %d] Alle %d Abo-Routinen wurden gestartet.", shardID, len(symbols))
	shardWg.Wait()
}


func main() {
	// ... (Markt-Lade-Code bleibt unverändert) ...
	log.Printf("Starte KuCoin Trade Watcher für bis zu %d Symbole...", MAX_SYMBOLS_TO_SUB)
	log.Println("Erstelle temporäre REST-Instanz zum Laden der Märkte...")
	tempExchange := ccxt.NewKucoin(nil)
	log.Println("Lade Märkte von KuCoin...")
	marketsMap, err := tempExchange.LoadMarkets(true)
	if err != nil { log.Fatalf("Märkte konnten nicht geladen werden: %v", err) }
	log.Println("Märkte geladen.")
	var activeSpotSymbols []string
	for _, market := range marketsMap {
		if market.Spot != nil && *market.Spot &&
			market.Active != nil && *market.Active &&
			market.QuoteCurrency != nil && *market.QuoteCurrency == "USDT" &&
			market.Symbol != nil {
			activeSpotSymbols = append(activeSpotSymbols, *market.Symbol)
		}
	}
	if len(activeSpotSymbols) == 0 { log.Fatal("Keine aktiven Spot-Märkte mit Quote USDT gefunden.") }
	log.Printf("%d aktive Spot-Märkte mit Quote USDT gefunden.", len(activeSpotSymbols))
	if len(activeSpotSymbols) > MAX_SYMBOLS_TO_SUB { activeSpotSymbols = activeSpotSymbols[:MAX_SYMBOLS_TO_SUB] }
	log.Printf("Werde Trades für %d Symbole abonnieren.", len(activeSpotSymbols))

	// KORREKTUR: Der Kanal wird jetzt mit der neuen Kapazität erstellt.
	tradeChannel := make(chan pro.Trade, TRADE_CHANNEL_CAPACITY)

	var wg sync.WaitGroup
	stats := &Stats{}

	var statsWg sync.WaitGroup
	statsWg.Add(1)
	go logStatsPeriodically(stats, &statsWg)

	shardCounter := 1
	for i := 0; i < len(activeSpotSymbols); i += SYMBOLS_PER_SHARD {
		end := i + SYMBOLS_PER_SHARD
		if end > len(activeSpotSymbols) { end = len(activeSpotSymbols) }
		shardSymbols := activeSpotSymbols[i:end]
		wg.Add(1)
		go watchShard(shardCounter, shardSymbols, tradeChannel, &wg)
		shardCounter++
		if i+SYMBOLS_PER_SHARD < len(activeSpotSymbols) {
			log.Printf("Warte %v bevor der nächste Shard gestartet wird...", NEW_SHARD_PAUSE)
			time.Sleep(NEW_SHARD_PAUSE)
		}
	}

	log.Println("Alle Shards wurden gestartet. Sammle Trades für die Statistik...")

	for trade := range tradeChannel {
		goRecvTimestamp := time.Now().UnixMilli()
		if trade.Timestamp != nil {
			e2eLatency := goRecvTimestamp - *trade.Timestamp
			if e2eLatency >= 0 { // Latenz kann auch 0 sein
				stats.mutex.Lock()
				stats.tradeCounter++
				stats.totalTradesProcessed++
				stats.e2eLatencies = append(stats.e2eLatencies, e2eLatency)
				stats.mutex.Unlock()
			}
		} else {
			stats.mutex.Lock()
			stats.tradeCounter++
			stats.totalTradesProcessed++
			stats.mutex.Unlock()
		}
	}

	wg.Wait()
	log.Println("Alle Shard-Manager wurden beendet.")
}