package broker

import (
	"sync"
	"testing"
	"time"

	"bybit-watcher/internal/shared_types"
	"github.com/go-zeromq/zmq4"
)

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
		sendCh:    make(chan outboundEnvelope, 16),
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
		sendCh:    make(chan outboundEnvelope, 16),
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
		sendCh:    make(chan outboundEnvelope, 16),
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
		sendCh:    make(chan outboundEnvelope, 16),
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
		sendCh:    make(chan outboundEnvelope, 16),
	}

	pongPayload := []byte(`{"message":"pong"}`)

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				cm.handleMessage([]byte("client-1"), pongPayload)
			}
		}(i)
	}
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("concurrent handleMessage test timed out")
	}

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

func TestEnqueueSocketSendDoesNotBlockWhenChannelFull(t *testing.T) {
	cm := &ClientManager{
		sendCh: make(chan outboundEnvelope, 1),
	}
	cm.sendCh <- outboundEnvelope{msg: zmq4.NewMsg([]byte("filled")), metricType: "trade"}

	done := make(chan struct{})
	go func() {
		cm.enqueueSocketSend(outboundEnvelope{msg: zmq4.NewMsg([]byte("drop-if-full")), metricType: "trade"})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("enqueueSocketSend blocked on full channel")
	}
}
