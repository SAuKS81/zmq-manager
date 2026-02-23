package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ccxt/ccxt/go/v4"
	json "github.com/goccy/go-json"
	"github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

// ======================================================================
// --- 1. SHARED TYPES ---
// ======================================================================

type OrderBookLevel struct {
	Price  float64 `json:"price" msgpack:"p"`
	Amount float64 `json:"amount" msgpack:"a"`
}

type OrderBookUpdate struct {
	Exchange    string           `json:"exchange" msgpack:"e"`
	Symbol      string           `json:"symbol" msgpack:"s"`
	MarketType  string           `json:"market_type" msgpack:"m"`
	Timestamp   int64            `json:"timestamp" msgpack:"t"`  // Exchange Time
	GoTimestamp int64            `json:"go_timestamp" msgpack:"gt"` // Broker Time
	Bids        []OrderBookLevel `json:"bids" msgpack:"b"`
	Asks        []OrderBookLevel `json:"asks" msgpack:"a"`
	DataType    string           `json:"data_type" msgpack:"dt"`
}

type TradeUpdate struct {
	Exchange    string  `json:"exchange" msgpack:"e"`
	Symbol      string  `json:"symbol" msgpack:"s"`
	MarketType  string  `json:"market_type" msgpack:"m"`
	Timestamp   int64   `json:"timestamp" msgpack:"t"`  // Exchange Time
	GoTimestamp int64   `json:"go_timestamp" msgpack:"gt"` // Broker Time
	Price       float64 `json:"price" msgpack:"p"`
	Amount      float64 `json:"amount" msgpack:"a"`
	Side        string  `json:"side" msgpack:"Si"`
	TradeID     string  `json:"trade_id" msgpack:"ti"`
	DataType    string  `json:"data_type" msgpack:"dt"`
}

// ======================================================================
// --- 2. KONFIGURATION ---
// ======================================================================

type JobConfig struct {
	Handler     string
	MarketTypes []string
	DataTypes   []string
	SymbolLimit int
	Depth       int
}

var StressTestConfig = []JobConfig{
	{
		Handler:     "binance_native",
		MarketTypes: []string{"spot", "swap"},
		DataTypes:   []string{"trades", "orderbooks"},
		SymbolLimit: 1000, 
		Depth:       1, 
	},
	{
		Handler:     "bybit_native",
		MarketTypes: []string{"spot", "swap"},
		DataTypes:   []string{"trades", "orderbooks"},
		SymbolLimit: 1000,
		Depth:       1,
	},
}

// ======================================================================
// --- 3. CCXT LOGIK ---
// ======================================================================

func getCCXTSymbols(handler string, marketType string, limit int) []string {
	exchangeID := strings.Split(handler, "_")[0]
	log.Printf("Lade Märkte für %s (%s) via CCXT...", strings.ToUpper(exchangeID), strings.ToUpper(marketType))

	var exc ccxt.IExchange
	options := map[string]interface{}{
		"defaultType": marketType,
	}
	config := map[string]interface{}{"options": options}

	switch exchangeID {
	case "binance":
		exc = ccxt.NewBinance(config)
	case "bybit":
		exc = ccxt.NewBybit(config)
	case "mexc":
		exc = ccxt.NewMexc(config)
	case "kucoin":
		exc = ccxt.NewKucoin(config)
	case "bitget":
		exc = ccxt.NewBitget(config)
	default:
		log.Printf("[WARN] Unbekannte Exchange ID für CCXT: %s", exchangeID)
		return []string{}
	}

	markets, err := exc.LoadMarkets()
	if err != nil {
		log.Printf("[ERROR] CCXT LoadMarkets fehlgeschlagen für %s: %v", exchangeID, err)
		return []string{}
	}

	var symbols []string
	validQuotes := []string{"/USDT", "/USDC", "/FDUSD", "/TUSD", "/DAI"}

	for _, m := range markets {
		if m.Active != nil && !*m.Active {
			continue
		}
		if m.Symbol == nil {
			continue
		}
		symbolStr := *m.Symbol

		if marketType == "spot" {
			if m.Spot != nil && *m.Spot {
				for _, quote := range validQuotes {
					if strings.HasSuffix(symbolStr, quote) {
						symbols = append(symbols, symbolStr)
						break
					}
				}
			}
		}

		if marketType == "swap" {
			isSwap := (m.Swap != nil && *m.Swap) || (m.Future != nil && *m.Future) || (m.Linear != nil && *m.Linear)
			isStableSettle := false
			if m.Settle != nil {
				if *m.Settle == "USDT" || *m.Settle == "USDC" {
					isStableSettle = true
				}
			} else {
				if strings.HasSuffix(symbolStr, ":USDT") || strings.HasSuffix(symbolStr, ":USDC") {
					isStableSettle = true
				}
			}
			if isSwap && isStableSettle {
				symbols = append(symbols, symbolStr)
			}
		}
	}

	if len(symbols) == 0 {
		log.Printf("[WARN] Keine passenden Märkte gefunden für %s %s", exchangeID, marketType)
		return []string{}
	}

	rand.Seed(time.Now().UnixNano())
	rand.Shuffle(len(symbols), func(i, j int) { symbols[i], symbols[j] = symbols[j], symbols[i] })

	if len(symbols) > limit {
		return symbols[:limit]
	}
	return symbols
}

// ======================================================================
// --- 4. STATISTIK & CLIENT ---
// ======================================================================

type Statistics struct {
	mu sync.Mutex
	
	TradeCount          int
	TradeInternalLats   []int64
	TradeE2ELats        []int64

	ObCount             int
	ObInternalLats      []int64
	ObE2ELats           []int64
}

func (s *Statistics) AddTrades(count int, internalLats []int64, e2eLats []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.TradeCount += count
	s.TradeInternalLats = append(s.TradeInternalLats, internalLats...)
	s.TradeE2ELats = append(s.TradeE2ELats, e2eLats...)
}

func (s *Statistics) AddOrderBooks(count int, internalLats []int64, e2eLats []int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ObCount += count
	s.ObInternalLats = append(s.ObInternalLats, internalLats...)
	s.ObE2ELats = append(s.ObE2ELats, e2eLats...)
}

func (s *Statistics) ResetAndPrint() {
	s.mu.Lock()
	defer s.mu.Unlock()

	fmt.Println("\n==================================================")
	printStatGroup("Trades", s.TradeCount, s.TradeInternalLats, s.TradeE2ELats)
	fmt.Println("--------------------------------------------------")
	printStatGroup("OrderBooks", s.ObCount, s.ObInternalLats, s.ObE2ELats)
	fmt.Println("==================================================")

	s.TradeCount = 0
	s.TradeInternalLats = s.TradeInternalLats[:0]
	s.TradeE2ELats = s.TradeE2ELats[:0]
	
	s.ObCount = 0
	s.ObInternalLats = s.ObInternalLats[:0]
	s.ObE2ELats = s.ObE2ELats[:0]
}

func printStatGroup(label string, count int, internalLats []int64, e2eLats []int64) {
	if count == 0 {
		return
	}
	rate := float64(count) / 10.0
	fmt.Printf("[STATS|%-10s] Rate: %d in 10s (%.2f/s)\n", label, count, rate)

	printLatencyLine("Internal Latency", internalLats)
	printLatencyLine("E2E Latency     ", e2eLats)
}

func printLatencyLine(name string, lats []int64) {
	if len(lats) == 0 {
		return
	}
	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	var sum int64
	for _, l := range lats {
		sum += l
	}
	avg := float64(sum) / float64(len(lats))
	max := float64(lats[len(lats)-1])
	p95Index := int(math.Floor(float64(len(lats)) * 0.95))
	p95 := float64(lats[p95Index])

	fmt.Printf("         └─ %s (ms): avg=%.2f, p95=%.2f, max=%.0f\n", name, avg, p95, max)
}

type BrokerClient struct {
	socket  zmq4.Socket
	stats   *Statistics
	stopCh  chan struct{}
	zmqAddr string
}

func NewBrokerClient(addr string) *BrokerClient {
	return &BrokerClient{
		zmqAddr: addr,
		stats:   &Statistics{},
		stopCh:  make(chan struct{}),
	}
}

func (bc *BrokerClient) Connect() {
	clientID := fmt.Sprintf("go-stress-%d", time.Now().UnixNano())
	bc.socket = zmq4.NewDealer(context.Background(), zmq4.WithID(zmq4.SocketIdentity(clientID)))
	if err := bc.socket.Dial(bc.zmqAddr); err != nil {
		log.Fatalf("ZMQ Connect Fehler: %v", err)
	}
	log.Printf("Client mit ID '%s' verbunden mit %s", clientID, bc.zmqAddr)
}

func (bc *BrokerClient) Start() {
	go bc.listenLoop()
	go bc.statsLoop()
}

func (bc *BrokerClient) listenLoop() {
	for {
		select {
		case <-bc.stopCh:
			return
		default:
			msg, err := bc.socket.Recv()
			if err != nil {
				continue
			}

			if len(msg.Frames) < 2 {
				continue
			}

			headerFrame := msg.Frames[len(msg.Frames)-2]
			payloadFrame := msg.Frames[len(msg.Frames)-1]
			
			msgType := string(headerFrame)
			recvTs := time.Now().UnixMilli()

			if len(headerFrame) == 0 {
				if len(payloadFrame) > 0 && payloadFrame[0] == '{' {
					if bytes.Contains(payloadFrame, []byte("ping")) {
						bc.sendPong()
					}
				}
				continue
			}

			switch msgType {
			case "T": // Trades
				var trades []*TradeUpdate
				if err := msgpack.Unmarshal(payloadFrame, &trades); err != nil {
					continue
				}
				
				internal := make([]int64, 0, len(trades))
				e2e := make([]int64, 0, len(trades))
				
				for _, t := range trades {
					// Internal: Recv - BrokerParseTime
					if t.GoTimestamp > 0 {
						internal = append(internal, recvTs-t.GoTimestamp)
					}
					// E2E: Recv - ExchangeMatchTime
					if t.Timestamp > 0 {
						e2e = append(e2e, recvTs-t.Timestamp)
					}
				}
				bc.stats.AddTrades(len(trades), internal, e2e)

			case "O": // OrderBooks
				var obs []*OrderBookUpdate
				if err := msgpack.Unmarshal(payloadFrame, &obs); err != nil {
					continue
				}
				
				internal := make([]int64, 0, len(obs))
				e2e := make([]int64, 0, len(obs))
				
				for _, ob := range obs {
					if ob.GoTimestamp > 0 {
						internal = append(internal, recvTs-ob.GoTimestamp)
					}
					if ob.Timestamp > 0 {
						e2e = append(e2e, recvTs-ob.Timestamp)
					}
				}
				bc.stats.AddOrderBooks(len(obs), internal, e2e)
			}
		}
	}
}

func (bc *BrokerClient) statsLoop() {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-bc.stopCh:
			return
		case <-ticker.C:
			bc.stats.ResetAndPrint()
		}
	}
}

func (bc *BrokerClient) SubscribeBulk(handler string, symbols []string, marketType, dataType string, depth int) {
	req := map[string]interface{}{
		"action":      "subscribe_bulk",
		"exchange":    handler,
		"symbols":     symbols,
		"market_type": marketType,
		"data_type":   dataType,
		"encoding":    "msgpack",
	}
	
	if dataType == "orderbooks" && depth > 0 {
		req["depth"] = depth
	}

	reqBytes, _ := json.Marshal(req)
	bc.socket.Send(zmq4.NewMsg(reqBytes))
}

func (bc *BrokerClient) sendPong() {
	pong, _ := json.Marshal(map[string]string{"message": "pong"})
	bc.socket.Send(zmq4.NewMsg(pong))
}

func (bc *BrokerClient) Stop() {
	close(bc.stopCh)
	bc.socket.Close()
}

func getBrokerAddress() string {
	if runtime.GOOS == "windows" {
		return "tcp://localhost:5555"
	}
	return "ipc:///tmp/feed_broker.ipc"
}

// ======================================================================
// --- 5. MAIN ---
// ======================================================================

func main() {
	log.Println("Starte Go Stresstest-Client (E2E Metrics)...")
	
	addr := getBrokerAddress()
	log.Printf("Verbinde zu Broker via: %s", addr)

	client := NewBrokerClient(addr)
	client.Connect()
	client.Start()

	totalSubs := 0

	for _, job := range StressTestConfig {
		for _, mType := range job.MarketTypes {
			log.Printf("\n--- Job: %s | %s ---", job.Handler, mType)
			
			symbols := getCCXTSymbols(job.Handler, mType, job.SymbolLimit)
			
			if len(symbols) > 0 {
				for _, dType := range job.DataTypes {
					client.SubscribeBulk(job.Handler, symbols, mType, dType, job.Depth)
					log.Printf(" -> %s abonniert.", dType)
					time.Sleep(200 * time.Millisecond)
				}
				totalSubs += len(symbols) * len(job.DataTypes)
			} else {
				log.Printf("Keine Symbole gefunden.")
			}
		}
	}

	log.Printf("\nFertig. %d Streams abonniert.", totalSubs)
	log.Println("Lausche auf Daten... Drücke Ctrl+C zum Beenden.")

	select {}
}