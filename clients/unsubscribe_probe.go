package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"bybit-watcher/internal/shared_types"
	"github.com/go-zeromq/zmq4"
	json "github.com/goccy/go-json"
	"github.com/vmihailenco/msgpack/v5"
)

type probeBulkRequest struct {
	Action     string   `json:"action"`
	RequestID  string   `json:"request_id,omitempty"`
	Exchange   string   `json:"exchange"`
	Symbols    []string `json:"symbols"`
	MarketType string   `json:"market_type"`
	DataType   string   `json:"data_type"`
	Encoding   string   `json:"encoding,omitempty"`
}

type probeStatusEvent struct {
	Type       string   `json:"type"`
	Exchange   string   `json:"exchange,omitempty"`
	MarketType string   `json:"market_type,omitempty"`
	DataType   string   `json:"data_type,omitempty"`
	Symbol     string   `json:"symbol,omitempty"`
	Symbols    []string `json:"symbols,omitempty"`
	Reason     string   `json:"reason,omitempty"`
	Message    string   `json:"message,omitempty"`
	Attempt    int      `json:"attempt,omitempty"`
	Timestamp  int64    `json:"ts,omitempty"`
}

type probeConfig struct {
	broker      string
	exchange    string
	marketType  string
	dataType    string
	symbolsFile string
	limit       int
	encoding    string
	cycles      int
	hold        time.Duration
	cooldown    time.Duration
}

func main() {
	cfg := parseProbeFlags()
	symbols, err := loadProbeSymbols(cfg.symbolsFile, cfg.limit)
	if err != nil {
		fmt.Fprintf(os.Stderr, "[PROBE] symbols load failed: %v\n", err)
		os.Exit(1)
	}
	if len(symbols) == 0 {
		fmt.Fprintln(os.Stderr, "[PROBE] no symbols selected")
		os.Exit(1)
	}

	socket := zmq4.NewDealer(context.Background(), zmq4.WithID(zmq4.SocketIdentity(fmt.Sprintf("unsub-probe-%d", time.Now().UnixNano()))))
	defer socket.Close()

	if err := socket.Dial(cfg.broker); err != nil {
		fmt.Fprintf(os.Stderr, "[PROBE] connect failed (%s): %v\n", cfg.broker, err)
		os.Exit(1)
	}

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

	fmt.Printf("[PROBE] broker=%s exchange=%s market_type=%s data_type=%s symbols=%d cycles=%d hold=%s cooldown=%s\n",
		cfg.broker, cfg.exchange, cfg.marketType, cfg.dataType, len(symbols), cfg.cycles, cfg.hold, cfg.cooldown,
	)

	for cycle := 1; cycle <= cfg.cycles; cycle++ {
		subReq := probeBulkRequest{
			Action:     "subscribe_bulk",
			RequestID:  fmt.Sprintf("probe-sub-%d", cycle),
			Exchange:   cfg.exchange,
			Symbols:    symbols,
			MarketType: cfg.marketType,
			DataType:   cfg.dataType,
			Encoding:   cfg.encoding,
		}
		if err := sendProbe(socket, subReq); err != nil {
			fmt.Fprintf(os.Stderr, "[PROBE] subscribe cycle=%d failed: %v\n", cycle, err)
			os.Exit(1)
		}
		fmt.Printf("[PROBE] SUBSCRIBE_SENT cycle=%d request_id=%s\n", cycle, subReq.RequestID)
		drainProbe(socket, msgCh, errCh, time.Now().Add(cfg.hold))

		unsubReq := probeBulkRequest{
			Action:     "unsubscribe_bulk",
			RequestID:  fmt.Sprintf("probe-unsub-%d", cycle),
			Exchange:   cfg.exchange,
			Symbols:    symbols,
			MarketType: cfg.marketType,
			DataType:   cfg.dataType,
		}
		if err := sendProbe(socket, unsubReq); err != nil {
			fmt.Fprintf(os.Stderr, "[PROBE] unsubscribe cycle=%d failed: %v\n", cycle, err)
			os.Exit(1)
		}
		fmt.Printf("[PROBE] UNSUBSCRIBE_SENT cycle=%d request_id=%s\n", cycle, unsubReq.RequestID)
		drainProbe(socket, msgCh, errCh, time.Now().Add(cfg.cooldown))
	}

	_ = sendProbe(socket, map[string]string{"action": "disconnect"})
	fmt.Println("[PROBE] DISCONNECT_SENT")
}

func parseProbeFlags() probeConfig {
	cfg := probeConfig{}
	flag.StringVar(&cfg.broker, "broker", defaultProbeBroker(), "broker endpoint")
	flag.StringVar(&cfg.exchange, "exchange", "kucoin", "exchange route")
	flag.StringVar(&cfg.marketType, "market-type", "spot", "market type")
	flag.StringVar(&cfg.dataType, "data-type", "trades", "trades|orderbooks")
	flag.StringVar(&cfg.symbolsFile, "symbols-file", "scripts/symbols_example_50.txt", "symbols file")
	flag.IntVar(&cfg.limit, "limit", 50, "max number of symbols (0 = all)")
	flag.StringVar(&cfg.encoding, "encoding", "msgpack", "json|msgpack")
	flag.IntVar(&cfg.cycles, "cycles", 3, "subscribe/unsubscribe cycles")
	flag.DurationVar(&cfg.hold, "hold", 8*time.Second, "hold time after subscribe")
	flag.DurationVar(&cfg.cooldown, "cooldown", 10*time.Second, "observe time after unsubscribe")
	flag.Parse()
	return cfg
}

func defaultProbeBroker() string {
	if runtime.GOOS == "linux" {
		return "ipc:///tmp/feed_broker.ipc"
	}
	return "tcp://127.0.0.1:5555"
}

func loadProbeSymbols(path string, limit int) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	out := make([]string, 0, 256)
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

func sendProbe(socket zmq4.Socket, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return socket.Send(zmq4.NewMsg(raw))
}

func drainProbe(socket zmq4.Socket, msgCh <-chan zmq4.Msg, errCh <-chan error, until time.Time) {
	for time.Now().Before(until) {
		timeout := time.Until(until)
		if timeout > 500*time.Millisecond {
			timeout = 500 * time.Millisecond
		}
		select {
		case err := <-errCh:
			fmt.Fprintf(os.Stderr, "[PROBE] recv failed: %v\n", err)
			return
		case msg := <-msgCh:
			handleProbeMessage(socket, msg)
		case <-time.After(timeout):
		}
	}
}

func handleProbeMessage(socket zmq4.Socket, msg zmq4.Msg) {
	if len(msg.Frames) == 0 {
		return
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
			if err := msgpack.Unmarshal(payload, &trades); err == nil {
				fmt.Printf("[PROBE] DATA trades=%d\n", len(trades))
				return
			}
		case 'O':
			fmt.Println("[PROBE] DATA orderbooks")
			return
		}
	}

	var ping map[string]string
	if err := json.Unmarshal(payload, &ping); err == nil && ping["type"] == "ping" {
		_ = sendProbe(socket, map[string]string{"message": "pong"})
		fmt.Println("[PROBE] PONG_SENT")
		return
	}

	var status probeStatusEvent
	if err := json.Unmarshal(payload, &status); err == nil && strings.HasPrefix(status.Type, "stream_") {
		fmt.Printf("[PROBE] STATUS type=%s exchange=%s market_type=%s data_type=%s symbol=%s symbols=%d attempt=%d reason=%s message=%s\n",
			status.Type,
			status.Exchange,
			status.MarketType,
			status.DataType,
			status.Symbol,
			len(status.Symbols),
			status.Attempt,
			status.Reason,
			status.Message,
		)
		return
	}

	var errResp map[string]any
	if err := json.Unmarshal(payload, &errResp); err == nil {
		if typ, _ := errResp["type"].(string); typ != "" {
			fmt.Printf("[PROBE] CONTROL type=%s payload=%s\n", typ, string(payload))
			return
		}
	}

	fmt.Printf("[PROBE] RAW %s\n", string(payload))
}
