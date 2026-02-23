package broker

import (
	"sync"
	"testing"
	"time"

	"bybit-watcher/internal/shared_types"
)

func newTestClientManager(buffer int) *ClientManager {
	return &ClientManager{
		clients:   make(map[string]*Client),
		requestCh: make(chan *shared_types.ClientRequest, buffer),
	}
}

func drainRequests(ch <-chan *shared_types.ClientRequest) []*shared_types.ClientRequest {
	out := make([]*shared_types.ClientRequest, 0)
	for {
		select {
		case req := <-ch:
			out = append(out, req)
		default:
			return out
		}
	}
}

func TestHandleMessageSubscribeBulk(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients:   map[string]*Client{"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json"}},
		requestCh: reqCh,
	}

	payload := []byte(`{"action":"subscribe_bulk","exchange":"binance_native","symbols":["BTC/USDT","ETH/USDT"],"market_type":"swap","depth":1}`)
	cm.handleMessage([]byte("client-1"), payload)

	reqs := drainRequests(reqCh)
	if len(reqs) != 2 {
		t.Fatalf("expected 2 requests, got %d", len(reqs))
	}
	for _, req := range reqs {
		if req.Action != "subscribe" {
			t.Fatalf("expected action subscribe, got %q", req.Action)
		}
		if req.DataType != "trades" {
			t.Fatalf("expected default datatype trades, got %q", req.DataType)
		}
		if req.MarketType != "swap" || req.Exchange != "binance_native" {
			t.Fatalf("unexpected routing fields: %+v", req)
		}
	}
}

func TestHandleMessageInvalidJSONNoPanic(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients:   map[string]*Client{"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json"}},
		requestCh: reqCh,
	}

	cm.handleMessage([]byte("client-1"), []byte(`{"action":`))

	if got := len(drainRequests(reqCh)); got != 0 {
		t.Fatalf("expected 0 requests after invalid json, got %d", got)
	}
}

func TestHandleMessageUnknownActionNoForward(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients:   map[string]*Client{"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json"}},
		requestCh: reqCh,
	}

	cm.handleMessage([]byte("client-1"), []byte(`{"action":"do_magic","exchange":"bybit_native","symbol":"BTC/USDT","market_type":"spot"}`))
	if got := len(drainRequests(reqCh)); got != 0 {
		t.Fatalf("expected unknown action to be dropped, got %d forwarded requests", got)
	}
}

func TestHandleMessageEncodingUpdate(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients:   map[string]*Client{"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json"}},
		requestCh: reqCh,
	}

	cm.handleMessage([]byte("client-1"), []byte(`{"encoding":"binary"}`))

	cm.clientsMu.RLock()
	encoding := cm.clients["client-1"].Encoding
	cm.clientsMu.RUnlock()

	if encoding != "msgpack" {
		t.Fatalf("expected encoding msgpack, got %q", encoding)
	}
}

func TestHandleMessageConcurrentClientStateAccess(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients:   map[string]*Client{"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json"}},
		requestCh: reqCh,
	}

	pongPayload := []byte(`{"message":"pong"}`)

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 500; j++ {
				cm.handleMessage([]byte("client-1"), pongPayload)
			}
		}(i)
	}
	wg.Wait()

	cm.clientsMu.RLock()
	client := cm.clients["client-1"]
	cm.clientsMu.RUnlock()

	if client == nil {
		t.Fatalf("client entry disappeared")
	}
	if client.LastPong.IsZero() {
		t.Fatalf("expected last pong timestamp to be set")
	}
}
