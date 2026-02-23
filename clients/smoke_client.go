package main

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/go-zeromq/zmq4"
	json "github.com/goccy/go-json"
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

func main() {
	address := defaultBrokerAddress()

	socket := zmq4.NewDealer(context.Background(), zmq4.WithID(zmq4.SocketIdentity(fmt.Sprintf("smoke-%d", time.Now().UnixNano()))))
	defer socket.Close()

	if err := socket.Dial(address); err != nil {
		fmt.Fprintf(os.Stderr, "[SMOKE] broker connect failed (%s): %v\n", address, err)
		os.Exit(1)
	}

	requests := []subscribeRequest{
		{Action: "subscribe", Exchange: "bybit_native", Symbol: "BTC/USDT", MarketType: "spot", DataType: "trades", Encoding: "msgpack"},
		{Action: "subscribe", Exchange: "binance_native", Symbol: "BTC/USDT", MarketType: "spot", DataType: "trades", Encoding: "msgpack"},
		{Action: "subscribe", Exchange: "binance_native", Symbol: "BTC/USDT", MarketType: "spot", DataType: "orderbooks", Depth: 5, Encoding: "msgpack"},
	}

	for i, req := range requests {
		payload, err := json.Marshal(req)
		if err != nil {
			fmt.Fprintf(os.Stderr, "[SMOKE] marshal request %d failed: %v\n", i+1, err)
			os.Exit(1)
		}
		if err := socket.Send(zmq4.NewMsg(payload)); err != nil {
			fmt.Fprintf(os.Stderr, "[SMOKE] send request %d failed: %v\n", i+1, err)
			os.Exit(1)
		}
		time.Sleep(250 * time.Millisecond)
	}

	end := time.Now().Add(10 * time.Second)
	tradeMsgs := 0
	obMsgs := 0

	for time.Now().Before(end) {
		msg, err := socket.Recv()
		if err != nil {
			fmt.Fprintf(os.Stderr, "[SMOKE] recv failed: %v\n", err)
			os.Exit(1)
		}
		if len(msg.Frames) == 0 {
			continue
		}

		header := []byte{}
		payload := msg.Frames[len(msg.Frames)-1]
		if len(msg.Frames) >= 2 {
			header = msg.Frames[len(msg.Frames)-2]
		}

		if len(header) == 1 {
			switch header[0] {
			case 'T':
				tradeMsgs++
			case 'O':
				obMsgs++
			}
			continue
		}

		var ping map[string]string
		if err := json.Unmarshal(payload, &ping); err == nil && ping["type"] == "ping" {
			pong, _ := json.Marshal(map[string]string{"message": "pong"})
			_ = socket.Send(zmq4.NewMsg(pong))
		}
	}

	fmt.Printf("[SMOKE] finished in 10s\n")
	fmt.Printf("[SMOKE] trade msgs: %d\n", tradeMsgs)
	fmt.Printf("[SMOKE] ob msgs: %d\n", obMsgs)
}

func defaultBrokerAddress() string {
	if runtime.GOOS == "linux" {
		return "ipc:///tmp/feed_broker.ipc"
	}
	return "tcp://127.0.0.1:5555"
}
