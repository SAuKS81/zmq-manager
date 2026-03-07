package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"bybit-watcher/internal/shared_types"
	"github.com/go-zeromq/zmq4"
	"github.com/vmihailenco/msgpack/v5"
)

type loggerConfig struct {
	brokerAddress string
	exchange      string
	marketType    string
	symbols       []string
	encoding      string
	outDir        string
}

type subscribeBulkRequest struct {
	Action     string   `json:"action"`
	Exchange   string   `json:"exchange"`
	Symbols    []string `json:"symbols"`
	MarketType string   `json:"market_type"`
	DataType   string   `json:"data_type"`
	Encoding   string   `json:"encoding,omitempty"`
}

type controlMessage struct {
	Type    string `json:"type"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type fileManager struct {
	mu      sync.Mutex
	baseDir string
	buffers map[string][][]string
}

func newFileManager(baseDir string) *fileManager {
	return &fileManager{
		baseDir: baseDir,
		buffers: make(map[string][][]string),
	}
}

func (fm *fileManager) addRecord(symbol string, record []string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if _, ok := fm.buffers[symbol]; !ok {
		fm.buffers[symbol] = make([][]string, 0, 200)
	}

	fm.buffers[symbol] = append(fm.buffers[symbol], record)
	if len(fm.buffers[symbol]) < 200 {
		return
	}

	bufferToFlush := fm.buffers[symbol]
	fm.buffers[symbol] = make([][]string, 0, 200)
	go fm.flushBuffer(symbol, bufferToFlush)
}

func (fm *fileManager) flushBuffer(symbol string, buffer [][]string) {
	if len(buffer) == 0 {
		return
	}

	dateStr := time.Now().UTC().Format("2006-01-02")
	fileName := fmt.Sprintf("%s_%s.csv.gz", strings.ReplaceAll(symbol, "/", ""), dateStr)
	filePath := filepath.Join(fm.baseDir, fileName)

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		log.Printf("[LOGGER] open failed symbol=%s err=%v", symbol, err)
		return
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		log.Printf("[LOGGER] stat failed symbol=%s err=%v", symbol, err)
		return
	}

	gz := gzip.NewWriter(file)
	defer gz.Close()

	csvWriter := csv.NewWriter(gz)
	if stat.Size() == 0 {
		header := []string{
			"timestamp",
			"symbol",
			"side",
			"size",
			"price",
			"tickDirection",
			"trdMatchID",
			"grossValue",
			"homeNotional",
			"foreignNotional",
			"e2e_latency_ms",
			"internal_latency_ms",
		}
		if err := csvWriter.Write(header); err != nil {
			log.Printf("[LOGGER] header write failed symbol=%s err=%v", symbol, err)
			return
		}
	}

	if err := csvWriter.WriteAll(buffer); err != nil {
		log.Printf("[LOGGER] batch write failed symbol=%s err=%v", symbol, err)
		return
	}
	csvWriter.Flush()
}

func (fm *fileManager) flushAllOnShutdown() {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	log.Println("[LOGGER] flushing remaining buffers")
	var wg sync.WaitGroup
	for symbol, buffer := range fm.buffers {
		if len(buffer) == 0 {
			continue
		}
		wg.Add(1)
		go func(symbol string, buffer [][]string) {
			defer wg.Done()
			fm.flushBuffer(symbol, buffer)
		}(symbol, buffer)
	}
	wg.Wait()
}

func cleanupOldFiles(dir string) {
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("[LOGGER] cleanup read dir failed: %v", err)
		return
	}

	todayStr := time.Now().UTC().Format("2006-01-02")
	for _, file := range files {
		fileName := file.Name()
		if !strings.HasSuffix(fileName, ".csv.gz") || strings.Contains(fileName, todayStr) {
			continue
		}
		filePath := filepath.Join(dir, fileName)
		if err := os.Remove(filePath); err != nil {
			log.Printf("[LOGGER] cleanup remove failed file=%s err=%v", filePath, err)
		}
	}
}

func dailyCleanup(dir string) {
	cleanupOldFiles(dir)
	for {
		now := time.Now().UTC()
		nextRun := time.Date(now.Year(), now.Month(), now.Day(), 0, 1, 0, 0, time.UTC).Add(24 * time.Hour)
		time.Sleep(time.Until(nextRun))
		cleanupOldFiles(dir)
	}
}

func parseFlags() loggerConfig {
	symbolsCSV := ""
	cfg := loggerConfig{}

	flag.StringVar(&cfg.brokerAddress, "broker", defaultBrokerAddress(), "broker endpoint")
	flag.StringVar(&cfg.exchange, "exchange", "bybit_native", "exchange route")
	flag.StringVar(&cfg.marketType, "market-type", "swap", "market type")
	flag.StringVar(&symbolsCSV, "symbols", "BTCUSDT,ETHUSDT", "comma-separated native symbols")
	flag.StringVar(&cfg.encoding, "encoding", "msgpack", "json|msgpack|binary")
	flag.StringVar(&cfg.outDir, "out-dir", "trades", "output directory")
	flag.Parse()

	cfg.symbols = parseCSV(symbolsCSV)
	return cfg
}

func validateConfig(cfg loggerConfig) error {
	if cfg.exchange == "" {
		return fmt.Errorf("exchange must not be empty")
	}
	if cfg.marketType != "spot" && cfg.marketType != "swap" {
		return fmt.Errorf("market-type must be spot or swap")
	}
	if len(cfg.symbols) == 0 {
		return fmt.Errorf("at least one symbol is required")
	}
	switch cfg.encoding {
	case "json", "msgpack", "binary":
	default:
		return fmt.Errorf("encoding must be json, msgpack or binary")
	}
	if cfg.outDir == "" {
		return fmt.Errorf("out-dir must not be empty")
	}
	return nil
}

func parseCSV(input string) []string {
	parts := strings.Split(input, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func defaultBrokerAddress() string {
	if runtime.GOOS == "linux" {
		return "ipc:///tmp/feed_broker.ipc"
	}
	return "tcp://127.0.0.1:5555"
}

func main() {
	cfg := parseFlags()
	if err := validateConfig(cfg); err != nil {
		log.Fatalf("[LOGGER] config error: %v", err)
	}

	log.Printf(
		"[LOGGER] starting broker=%s exchange=%s market_type=%s symbols=%s encoding=%s",
		cfg.brokerAddress,
		cfg.exchange,
		cfg.marketType,
		strings.Join(cfg.symbols, ","),
		cfg.encoding,
	)

	fm := newFileManager(cfg.outDir)
	if err := os.MkdirAll(cfg.outDir, 0o755); err != nil {
		log.Fatalf("[LOGGER] could not create output dir: %v", err)
	}
	go dailyCleanup(cfg.outDir)

	socket := zmq4.NewDealer(context.Background())
	defer socket.Close()

	if err := socket.Dial(cfg.brokerAddress); err != nil {
		log.Fatalf("[LOGGER] broker connection failed: %v", err)
	}

	subscribePayload, err := json.Marshal(subscribeBulkRequest{
		Action:     "subscribe_bulk",
		Exchange:   cfg.exchange,
		Symbols:    cfg.symbols,
		MarketType: cfg.marketType,
		DataType:   "trades",
		Encoding:   cfg.encoding,
	})
	if err != nil {
		log.Fatalf("[LOGGER] subscribe marshal failed: %v", err)
	}
	if err := socket.Send(zmq4.NewMsg(subscribePayload)); err != nil {
		log.Fatalf("[LOGGER] subscribe send failed: %v", err)
	}

	ctx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()

	msgCh := make(chan zmq4.Msg, 256)
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

	for {
		select {
		case <-ctx.Done():
			sendDisconnect(socket)
			fm.flushAllOnShutdown()
			return
		case err := <-errCh:
			log.Printf("[LOGGER] recv failed: %v", err)
			sendDisconnect(socket)
			fm.flushAllOnShutdown()
			return
		case msg := <-msgCh:
			processMessage(socket, fm, msg, time.Now())
		}
	}
}

func sendDisconnect(socket zmq4.Socket) {
	payload, err := json.Marshal(map[string]string{"action": "disconnect"})
	if err != nil {
		return
	}
	_ = socket.Send(zmq4.NewMsg(payload))
}

func processMessage(socket zmq4.Socket, fm *fileManager, msg zmq4.Msg, recvTime time.Time) {
	header, payload := detectHeaderAndPayload(msg.Frames)
	if len(payload) == 0 {
		return
	}

	if handleControlPayload(socket, payload) {
		return
	}

	trades, ok := decodeTrades(header, payload)
	if !ok {
		return
	}
	for _, trade := range trades {
		if trade == nil {
			continue
		}
		writeTradeRecord(fm, recvTime, trade)
	}
}

func detectHeaderAndPayload(frames [][]byte) (byte, []byte) {
	if len(frames) == 0 {
		return 0, nil
	}

	payload := frames[len(frames)-1]
	for i := len(frames) - 2; i >= 0; i-- {
		frame := frames[i]
		if len(frame) != 1 {
			continue
		}
		switch frame[0] {
		case 'T', 'O', 'J':
			return frame[0], payload
		}
	}
	return 0, payload
}

func handleControlPayload(socket zmq4.Socket, payload []byte) bool {
	var msg controlMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return false
	}

	switch msg.Type {
	case "ping":
		pong, err := json.Marshal(map[string]string{"message": "pong"})
		if err == nil {
			_ = socket.Send(zmq4.NewMsg(pong))
		}
		return true
	case "error":
		log.Printf("[LOGGER] broker error code=%s message=%s", msg.Code, msg.Message)
		return true
	}

	if strings.HasPrefix(msg.Type, "stream_") || msg.Type == "deploy_batch_summary" || msg.Type == "runtime_totals_tick" {
		log.Printf("[LOGGER] control type=%s message=%s", msg.Type, msg.Message)
		return true
	}
	return false
}

func decodeTrades(header byte, payload []byte) ([]*shared_types.TradeUpdate, bool) {
	switch header {
	case 'O':
		return nil, false
	case 'T':
		var trades []*shared_types.TradeUpdate
		if err := msgpack.Unmarshal(payload, &trades); err != nil {
			log.Printf("[LOGGER] msgpack decode failed: %v", err)
			return nil, false
		}
		return trades, true
	default:
		var trades []*shared_types.TradeUpdate
		if err := json.Unmarshal(payload, &trades); err == nil {
			return trades, true
		}

		var trade shared_types.TradeUpdate
		if err := json.Unmarshal(payload, &trade); err == nil && trade.DataType == "trades" {
			return []*shared_types.TradeUpdate{&trade}, true
		}
	}
	return nil, false
}

func writeTradeRecord(fm *fileManager, recvTime time.Time, trade *shared_types.TradeUpdate) {
	cleanSymbol := strings.Split(trade.Symbol, ":")[0]
	e2eLatencyMS := float64(recvTime.UnixMilli() - trade.Timestamp)
	internalLatencyMS := float64(0)
	if trade.GoTimestamp > 0 {
		internalLatencyMS = float64(recvTime.UnixMilli() - trade.GoTimestamp)
	}

	homeNotional := trade.Amount
	foreignNotional := trade.Amount * trade.Price

	record := []string{
		fmt.Sprintf("%.4f", float64(trade.Timestamp)/1000.0),
		cleanSymbol,
		titleCaseASCII(trade.Side),
		fmt.Sprintf("%.8f", trade.Amount),
		fmt.Sprintf("%.8f", trade.Price),
		"Unknown",
		trade.TradeID,
		"0.0",
		fmt.Sprintf("%.8f", homeNotional),
		fmt.Sprintf("%.8f", foreignNotional),
		fmt.Sprintf("%.3f", e2eLatencyMS),
		fmt.Sprintf("%.3f", internalLatencyMS),
	}
	fm.addRecord(cleanSymbol, record)
}

func titleCaseASCII(input string) string {
	if input == "" {
		return ""
	}
	lower := strings.ToLower(input)
	return strings.ToUpper(lower[:1]) + lower[1:]
}
