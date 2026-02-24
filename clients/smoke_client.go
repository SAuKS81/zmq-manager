package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"

	"bybit-watcher/internal/shared_types"
	"github.com/go-zeromq/zmq4"
	json "github.com/goccy/go-json"
	"github.com/vmihailenco/msgpack/v5"
)

type subscribeRequest struct {
	Action     string `json:"action"`
	Exchange   string `json:"exchange"`
	Symbol     string `json:"symbol"`
	MarketType string `json:"market_type"`
	DataType   string `json:"data_type"`
	Depth      int    `json:"depth,omitempty"`
	Encoding   string `json:"encoding,omitempty"`
}

type smokeConfig struct {
	exchangesCSV   string
	symbolsFile    string
	symbolsLimit   int
	trades         bool
	orderbooks     bool
	obDepth        int
	encoding       string
	duration       time.Duration
	rateLog        time.Duration
	randomize      bool
	marketType     string
	subscribePause time.Duration
	brokerAddress  string
}

func main() {
	cfg := parseFlags()
	if err := validateConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "[SMOKE] config error: %v\n", err)
		os.Exit(1)
	}

	symbols, err := loadSymbols(cfg.symbolsFile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[SMOKE] symbols load failed: %v\n", err)
		os.Exit(1)
	}
	if len(symbols) == 0 {
		fmt.Fprintf(os.Stderr, "[SMOKE] symbols file has no symbols: %s\n", cfg.symbolsFile)
		os.Exit(1)
	}

	symbols = selectSymbols(symbols, cfg.symbolsLimit, cfg.randomize)
	exchanges := parseExchanges(cfg.exchangesCSV)
	if len(exchanges) == 0 {
		fmt.Fprintf(os.Stderr, "[SMOKE] no exchanges provided\n")
		os.Exit(1)
	}

	socket := zmq4.NewDealer(context.Background(), zmq4.WithID(zmq4.SocketIdentity(fmt.Sprintf("smoke-%d", time.Now().UnixNano()))))
	defer socket.Close()

	if err := socket.Dial(cfg.brokerAddress); err != nil {
		fmt.Fprintf(os.Stderr, "[SMOKE] broker connect failed (%s): %v\n", cfg.brokerAddress, err)
		os.Exit(1)
	}

	plan := buildPlan(exchanges, symbols, cfg)
	if len(plan) == 0 {
		fmt.Fprintf(os.Stderr, "[SMOKE] subscribe plan is empty\n")
		os.Exit(1)
	}

	fmt.Printf("[SMOKE] broker=%s exchanges=%d symbols=%d requests=%d duration=%s encoding=%s\n",
		cfg.brokerAddress, len(exchanges), len(symbols), len(plan), cfg.duration, cfg.encoding)

	for i, req := range plan {
		payload, err := json.Marshal(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[SMOKE] marshal request %d failed: %v\n", i+1, err)
			os.Exit(1)
		}
		if err := socket.Send(zmq4.NewMsg(payload)); err != nil {
			fmt.Fprintf(os.Stderr, "[SMOKE] send request %d failed: %v\n", i+1, err)
			os.Exit(1)
		}
		if cfg.subscribePause > 0 {
			time.Sleep(cfg.subscribePause)
		}
	}

	consumeLoop(socket, cfg)
}

func consumeLoop(socket zmq4.Socket, cfg smokeConfig) {
	msgCh := make(chan zmq4.Msg, 1024)
	errCh := make(chan error, 1)

	go func() {
		for {
			msg, err := socket.Recv()
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- msg
		}
	}()

	deadline := time.NewTimer(cfg.duration)
	defer deadline.Stop()

	rateTicker := time.NewTicker(cfg.rateLog)
	defer rateTicker.Stop()

	var totalTrades int
	var totalOB int
	var windowTrades int
	var windowOB int
	var totalDecodeErrors int
	var windowDecodeErrors int
	reconnects := 0

	for {
		select {
		case <-deadline.C:
			fmt.Printf("[SMOKE] done total_trades=%d total_ob=%d decode_errors=%d reconnects=%d\n", totalTrades, totalOB, totalDecodeErrors, reconnects)
			return
		case <-rateTicker.C:
			secs := cfg.rateLog.Seconds()
			fmt.Printf("[SMOKE] rate trades/s=%.2f ob/s=%.2f decode_errors=%d reconnects=%d\n",
				float64(windowTrades)/secs,
				float64(windowOB)/secs,
				windowDecodeErrors,
				reconnects,
			)
			windowTrades = 0
			windowOB = 0
			windowDecodeErrors = 0
		case err := <-errCh:
			fmt.Fprintf(os.Stderr, "[SMOKE] recv failed: %v\n", err)
			os.Exit(1)
		case msg := <-msgCh:
			trades, obs, decodeErr := decodeMessage(socket, msg)
			totalTrades += trades
			windowTrades += trades
			totalOB += obs
			windowOB += obs
			totalDecodeErrors += decodeErr
			windowDecodeErrors += decodeErr
		}
	}
}

func decodeMessage(socket zmq4.Socket, msg zmq4.Msg) (int, int, int) {
	if len(msg.Frames) == 0 {
		return 0, 0, 0
	}

	payload := msg.Frames[len(msg.Frames)-1]
	header := []byte{}
	if len(msg.Frames) >= 2 {
		header = msg.Frames[len(msg.Frames)-2]
	}

	if len(header) == 1 {
		switch header[0] {
		case 'T':
			var trades []*shared_types.TradeUpdate
			if err := msgpack.Unmarshal(payload, &trades); err != nil {
				return 0, 0, 1
			}
			if len(trades) == 0 {
				return 1, 0, 0
			}
			return len(trades), 0, 0
		case 'O':
			var obs []*shared_types.OrderBookUpdate
			if err := msgpack.Unmarshal(payload, &obs); err != nil {
				return 0, 0, 1
			}
			if len(obs) == 0 {
				return 0, 1, 0
			}
			return 0, len(obs), 0
		}
	}

	var ping map[string]string
	if err := json.Unmarshal(payload, &ping); err == nil {
		if ping["type"] == "ping" {
			pong, _ := json.Marshal(map[string]string{"message": "pong"})
			_ = socket.Send(zmq4.NewMsg(pong))
			return 0, 0, 0
		}
	}

	var arr []map[string]any
	if err := json.Unmarshal(payload, &arr); err == nil {
		trades := 0
		obs := 0
		for _, item := range arr {
			if dt, ok := item["data_type"].(string); ok {
				switch dt {
				case "trades":
					trades++
				case "orderbooks":
					obs++
				}
			}
		}
		return trades, obs, 0
	}

	return 0, 0, 1
}

func parseFlags() smokeConfig {
	cfg := smokeConfig{}

	flag.StringVar(&cfg.exchangesCSV, "exchanges", "binance_native,bybit_native", "comma-separated exchanges")
	flag.StringVar(&cfg.symbolsFile, "symbols-file", "", "path to symbol file (one symbol per line)")
	flag.IntVar(&cfg.symbolsLimit, "symbols-limit", 200, "max symbols to use (0 = all)")
	flag.BoolVar(&cfg.trades, "trades", true, "subscribe trades")
	flag.BoolVar(&cfg.orderbooks, "orderbooks", true, "subscribe orderbooks")
	flag.IntVar(&cfg.obDepth, "ob-depth", 5, "orderbook depth")
	flag.StringVar(&cfg.encoding, "encoding", "msgpack", "msgpack|json")
	flag.DurationVar(&cfg.duration, "duration", 60*time.Second, "consume duration")
	flag.DurationVar(&cfg.rateLog, "rate-log", 10*time.Second, "stats interval")
	flag.BoolVar(&cfg.randomize, "randomize-symbols", false, "deterministically shuffle symbols with fixed seed")
	flag.StringVar(&cfg.marketType, "market-type", "spot", "market type for subscriptions")
	flag.DurationVar(&cfg.subscribePause, "subscribe-pause", 50*time.Millisecond, "pause between subscribe requests")
	flag.StringVar(&cfg.brokerAddress, "broker", defaultBrokerAddress(), "broker endpoint")

	flag.Parse()
	return cfg
}

func validateConfig(cfg smokeConfig) error {
	if cfg.symbolsFile == "" {
		return fmt.Errorf("--symbols-file is required")
	}
	if cfg.encoding != "msgpack" && cfg.encoding != "json" {
		return fmt.Errorf("--encoding must be msgpack or json")
	}
	if !cfg.trades && !cfg.orderbooks {
		return fmt.Errorf("at least one of --trades or --orderbooks must be true")
	}
	if cfg.duration <= 0 {
		return fmt.Errorf("--duration must be > 0")
	}
	if cfg.rateLog <= 0 {
		return fmt.Errorf("--rate-log must be > 0")
	}
	if cfg.obDepth < 0 {
		return fmt.Errorf("--ob-depth must be >= 0")
	}
	return nil
}

func parseExchanges(csv string) []string {
	parts := strings.Split(csv, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		s := strings.TrimSpace(part)
		if s != "" {
			out = append(out, s)
		}
	}
	return out
}

func loadSymbols(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	symbols := make([]string, 0, 1024)
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		symbols = append(symbols, line)
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return symbols, nil
}

func selectSymbols(input []string, limit int, randomize bool) []string {
	selected := make([]string, len(input))
	copy(selected, input)

	if randomize {
		r := rand.New(rand.NewSource(42))
		r.Shuffle(len(selected), func(i, j int) {
			selected[i], selected[j] = selected[j], selected[i]
		})
	}

	if limit > 0 && limit < len(selected) {
		return selected[:limit]
	}
	return selected
}

func buildPlan(exchanges []string, symbols []string, cfg smokeConfig) []subscribeRequest {
	plan := make([]subscribeRequest, 0, len(exchanges)*len(symbols)*2)
	for _, ex := range exchanges {
		for _, symbol := range symbols {
			if cfg.trades {
				plan = append(plan, subscribeRequest{
					Action:     "subscribe",
					Exchange:   ex,
					Symbol:     symbol,
					MarketType: cfg.marketType,
					DataType:   "trades",
					Encoding:   cfg.encoding,
				})
			}
			if cfg.orderbooks {
				plan = append(plan, subscribeRequest{
					Action:     "subscribe",
					Exchange:   ex,
					Symbol:     symbol,
					MarketType: cfg.marketType,
					DataType:   "orderbooks",
					Depth:      cfg.obDepth,
					Encoding:   cfg.encoding,
				})
			}
		}
	}
	return plan
}

func defaultBrokerAddress() string {
	if runtime.GOOS == "linux" {
		return "ipc:///tmp/feed_broker.ipc"
	}
	return "tcp://127.0.0.1:5555"
}
