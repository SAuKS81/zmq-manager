package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"
	"github.com/gorilla/websocket"
)

type outbound struct {
	json any
	text *string
}

type marketState struct {
	mu       sync.Mutex
	base     map[string]float64
	tradeSeq map[string]int64
	obSeq    map[string]int64
}

func newMarketState() *marketState {
	return &marketState{
		base:     make(map[string]float64),
		tradeSeq: make(map[string]int64),
		obSeq:    make(map[string]int64),
	}
}

func (m *marketState) nextPrice(symbol string) float64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.base[symbol]
	if !ok {
		v = 50 + float64(len(symbol))*7
	}
	seq := m.tradeSeq[symbol]
	wave := math.Sin(float64(seq) / 8.0)
	v = v + (wave * 0.2)
	if v < 0.0001 {
		v = 0.0001
	}
	m.base[symbol] = v
	return v
}

func (m *marketState) nextTradeID(symbol string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tradeSeq[symbol]++
	return m.tradeSeq[symbol]
}

func (m *marketState) nextOBSeq(topic string) int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.obSeq[topic]++
	return m.obSeq[topic]
}

func main() {
	listen := flag.String("listen", "127.0.0.1:18080", "listen addr")
	tick := flag.Duration("tick", 100*time.Millisecond, "event tick interval")
	flag.Parse()

	upgrader := websocket.Upgrader{CheckOrigin: func(r *http.Request) bool { return true }}
	state := newMarketState()

	http.HandleFunc("/v5/public/spot", func(w http.ResponseWriter, r *http.Request) {
		handleBybit(upgrader, state, *tick, w, r)
	})
	http.HandleFunc("/v5/public/linear", func(w http.ResponseWriter, r *http.Request) {
		handleBybit(upgrader, state, *tick, w, r)
	})

	http.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		handleBinance(upgrader, state, *tick, "spot", w, r)
	})
	http.HandleFunc("/stream/spot", func(w http.ResponseWriter, r *http.Request) {
		handleBinance(upgrader, state, *tick, "spot", w, r)
	})
	http.HandleFunc("/stream/futures", func(w http.ResponseWriter, r *http.Request) {
		handleBinance(upgrader, state, *tick, "futures", w, r)
	})

	http.HandleFunc("/v2/ws/public", func(w http.ResponseWriter, r *http.Request) {
		handleBitget(upgrader, state, *tick, w, r)
	})

	log.Printf("[WS-REPLAY] listening on ws://%s", *listen)
	log.Printf("[WS-REPLAY] bybit:   ws://%s/v5/public/{spot|linear}", *listen)
	log.Printf("[WS-REPLAY] binance: ws://%s/stream or /stream/{spot|futures}", *listen)
	log.Printf("[WS-REPLAY] bitget:  ws://%s/v2/ws/public", *listen)
	log.Fatal(http.ListenAndServe(*listen, nil))
}

func handleBybit(upgrader websocket.Upgrader, state *marketState, tick time.Duration, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	writeCh := make(chan outbound, 1024)
	done := make(chan struct{})

	go wsWriter(conn, writeCh, done)

	var mu sync.Mutex
	tradeSyms := make(map[string]bool)
	obTopics := make(map[string]string)
	snapshotDone := make(map[string]bool)

	go func() {
		defer close(done)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req map[string]any
			if err := json.Unmarshal(msg, &req); err != nil {
				continue
			}
			op, _ := req["op"].(string)
			if op == "ping" {
				writeCh <- outbound{json: map[string]any{"op": "pong"}}
				continue
			}
			if op != "subscribe" && op != "unsubscribe" {
				continue
			}
			rawArgs, _ := req["args"].([]any)
			mu.Lock()
			for _, a := range rawArgs {
				s, ok := a.(string)
				if !ok {
					continue
				}
				if strings.HasPrefix(s, "publicTrade.") {
					sym := strings.TrimPrefix(s, "publicTrade.")
					if op == "subscribe" {
						tradeSyms[sym] = true
					} else {
						delete(tradeSyms, sym)
					}
				}
				if strings.HasPrefix(s, "orderbook.") {
					parts := strings.Split(s, ".")
					if len(parts) >= 3 {
						sym := parts[len(parts)-1]
						if op == "subscribe" {
							obTopics[s] = sym
						} else {
							delete(obTopics, s)
							delete(snapshotDone, s)
						}
					}
				}
			}
			mu.Unlock()
		}
	}()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			ts := now.UnixMilli()

			mu.Lock()
			localTrades := make([]string, 0, len(tradeSyms))
			for s := range tradeSyms {
				localTrades = append(localTrades, s)
			}
			localOB := make(map[string]string, len(obTopics))
			for t, s := range obTopics {
				localOB[t] = s
			}
			mu.Unlock()

			for _, sym := range localTrades {
				p := state.nextPrice(sym)
				tradeID := strconv.FormatInt(state.nextTradeID("bybit:"+sym), 10)
				msg := map[string]any{
					"topic": "publicTrade." + sym,
					"type":  "snapshot",
					"data": []map[string]any{{
						"T": ts,
						"s": sym,
						"S": "Buy",
						"v": "0.10",
						"p": fmt.Sprintf("%.6f", p),
						"i": tradeID,
					}},
				}
				writeCh <- outbound{json: msg}
			}

			for topic, sym := range localOB {
				p := state.nextPrice(sym)
				seq := state.nextOBSeq("bybit:" + topic)
				if seq == 0 {
					seq = 1
				}

				mu.Lock()
				hadSnapshot := snapshotDone[topic]
				if !hadSnapshot {
					snapshotDone[topic] = true
				}
				mu.Unlock()

				if !hadSnapshot {
					snap := map[string]any{
						"topic": topic,
						"type":  "snapshot",
						"ts":    ts,
						"data": map[string]any{
							"s": sym,
							"u": seq,
							"b": [][2]string{{fmt.Sprintf("%.6f", p-0.10), "2.0"}, {fmt.Sprintf("%.6f", p-0.20), "1.5"}},
							"a": [][2]string{{fmt.Sprintf("%.6f", p+0.10), "2.2"}, {fmt.Sprintf("%.6f", p+0.20), "1.7"}},
						},
					}
					writeCh <- outbound{json: snap}
					continue
				}

				delta := map[string]any{
					"topic": topic,
					"type":  "delta",
					"ts":    ts,
					"data": map[string]any{
						"s": sym,
						"u": seq,
						"b": [][2]string{{fmt.Sprintf("%.6f", p-0.10), "2.1"}},
						"a": [][2]string{{fmt.Sprintf("%.6f", p+0.10), "2.3"}},
					},
				}
				writeCh <- outbound{json: delta}
			}
		}
	}
}

func handleBinance(upgrader websocket.Upgrader, state *marketState, tick time.Duration, mode string, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	writeCh := make(chan outbound, 1024)
	done := make(chan struct{})
	go wsWriter(conn, writeCh, done)

	var mu sync.Mutex
	streams := make(map[string]bool)

	go func() {
		defer close(done)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			var req struct {
				Method string   `json:"method"`
				Params []string `json:"params"`
				ID     uint64   `json:"id"`
			}
			if err := json.Unmarshal(msg, &req); err != nil {
				continue
			}
			m := strings.ToUpper(req.Method)
			if m == "SUBSCRIBE" || m == "UNSUBSCRIBE" {
				mu.Lock()
				for _, s := range req.Params {
					if m == "SUBSCRIBE" {
						streams[s] = true
					} else {
						delete(streams, s)
					}
				}
				mu.Unlock()
				writeCh <- outbound{json: map[string]any{"result": nil, "id": req.ID}}
			}
		}
	}()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			ts := now.UnixMilli()
			mu.Lock()
			local := make([]string, 0, len(streams))
			for s := range streams {
				local = append(local, s)
			}
			mu.Unlock()

			for _, stream := range local {
				parts := strings.Split(stream, "@")
				if len(parts) < 2 {
					continue
				}
				symLower := parts[0]
				symUpper := strings.ToUpper(symLower)
				p := state.nextPrice("binance:" + symUpper)

				if strings.Contains(stream, "@trade") {
					tradeID := state.nextTradeID("binance-trade:" + symUpper)
					msg := map[string]any{
						"stream": stream,
						"data": map[string]any{
							"e": "trade",
							"E": ts,
							"s": symUpper,
							"t": tradeID,
							"p": fmt.Sprintf("%.6f", p),
							"q": "0.05",
							"T": ts,
							"m": false,
						},
					}
					writeCh <- outbound{json: msg}
					continue
				}

				if strings.Contains(stream, "@depth") {
					obData := map[string]any{
						"e": "depthUpdate",
						"E": ts,
						"s": symUpper,
					}
					if mode == "futures" {
						obData["b"] = [][2]string{{fmt.Sprintf("%.6f", p-0.10), "1.0"}}
						obData["a"] = [][2]string{{fmt.Sprintf("%.6f", p+0.10), "1.1"}}
					} else {
						obData["bids"] = [][2]string{{fmt.Sprintf("%.6f", p-0.10), "1.0"}}
						obData["asks"] = [][2]string{{fmt.Sprintf("%.6f", p+0.10), "1.1"}}
					}
					msg := map[string]any{"stream": stream, "data": obData}
					writeCh <- outbound{json: msg}
				}
			}
		}
	}
}

func handleBitget(upgrader websocket.Upgrader, state *marketState, tick time.Duration, w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	writeCh := make(chan outbound, 1024)
	done := make(chan struct{})
	go wsWriter(conn, writeCh, done)

	type arg struct {
		InstType string `json:"instType"`
		Channel  string `json:"channel"`
		InstID   string `json:"instId"`
	}
	var mu sync.Mutex
	subs := make(map[string]arg)

	go func() {
		defer close(done)
		for {
			mt, msg, err := conn.ReadMessage()
			if err != nil {
				return
			}
			if mt == websocket.TextMessage && string(msg) == "ping" {
				pong := "pong"
				writeCh <- outbound{text: &pong}
				continue
			}
			var req struct {
				Op   string `json:"op"`
				Args []arg  `json:"args"`
			}
			if err := json.Unmarshal(msg, &req); err != nil {
				continue
			}
			op := strings.ToLower(req.Op)
			if op != "subscribe" && op != "unsubscribe" {
				continue
			}
			mu.Lock()
			for _, a := range req.Args {
				k := a.InstType + "|" + a.Channel + "|" + a.InstID
				if op == "subscribe" {
					subs[k] = a
				} else {
					delete(subs, k)
				}
				writeCh <- outbound{json: map[string]any{"event": op, "arg": a}}
			}
			mu.Unlock()
		}
	}()

	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-done:
			return
		case now := <-ticker.C:
			ts := now.UnixMilli()
			mu.Lock()
			local := make([]arg, 0, len(subs))
			for _, a := range subs {
				local = append(local, a)
			}
			mu.Unlock()

			for _, a := range local {
				if strings.ToLower(a.Channel) != "trade" {
					continue
				}
				p := state.nextPrice("bitget:" + a.InstID)
				id := strconv.FormatInt(state.nextTradeID("bitget:"+a.InstID), 10)
				msg := map[string]any{
					"action": "update",
					"arg":    a,
					"data": []map[string]string{{
						"ts":      strconv.FormatInt(ts, 10),
						"price":   fmt.Sprintf("%.6f", p),
						"size":    "0.02",
						"side":    "buy",
						"tradeId": id,
					}},
				}
				writeCh <- outbound{json: msg}
			}
		}
	}
}

func wsWriter(conn *websocket.Conn, writeCh <-chan outbound, done <-chan struct{}) {
	for {
		select {
		case <-done:
			return
		case out := <-writeCh:
			if out.text != nil {
				if err := conn.WriteMessage(websocket.TextMessage, []byte(*out.text)); err != nil {
					return
				}
				continue
			}
			if out.json != nil {
				if err := conn.WriteJSON(out.json); err != nil {
					return
				}
			}
		}
	}
}
