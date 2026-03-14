package broker

import (
	"bytes"
	"log"
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
		sendChP1:  make(chan outboundEnvelope, 16),
		p2Latest:  make(map[p2LatestKey]outboundEnvelope),
	}

	payload := []byte(`{"action":"subscribe_bulk","request_id":"deploy-123","exchange":"binance_native","symbols":["BTC/USDT","ETH/USDT"],"market_type":"swap","depth":1}`)
	cm.handleMessage([]byte("client-1"), payload)

	reqs := drainRequests(reqCh)
	if len(reqs) != 3 {
		t.Fatalf("expected 3 requests, got %d", len(reqs))
	}
	if reqs[0].Action != "deploy_batch_register" || reqs[0].RequestID != "deploy-123" || reqs[0].BatchSent != 2 {
		t.Fatalf("expected deploy batch register request, got %+v", reqs[0])
	}
	for _, req := range reqs[1:] {
		if req.Action != "subscribe" {
			t.Fatalf("expected action subscribe, got %q", req.Action)
		}
		if req.DataType != "trades" {
			t.Fatalf("expected default datatype trades, got %q", req.DataType)
		}
		if req.MarketType != "swap" || req.Exchange != "binance_native" {
			t.Fatalf("unexpected routing fields: %+v", req)
		}
		if req.RequestID != "deploy-123" {
			t.Fatalf("expected request_id to propagate, got %+v", req)
		}
	}
}

func TestHandleMessageSubscribeBulkPropagatesCacheN(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients:   map[string]*Client{"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json"}},
		requestCh: reqCh,
		sendChP1:  make(chan outboundEnvelope, 16),
		p2Latest:  make(map[p2LatestKey]outboundEnvelope),
	}

	payload := []byte(`{"action":"subscribe_bulk","request_id":"deploy-123","exchange":"kucoin","symbols":["BTC/USDT","ETH/USDT"],"market_type":"spot","data_type":"trades","cache_n":1}`)
	cm.handleMessage([]byte("client-1"), payload)

	reqs := drainRequests(reqCh)
	if len(reqs) != 3 {
		t.Fatalf("expected 3 requests, got %d", len(reqs))
	}
	for _, req := range reqs[1:] {
		if req.CacheN != 1 {
			t.Fatalf("expected cache_n=1 to propagate, got %+v", req)
		}
	}
}

func TestHandleMessageInvalidJSONNoPanic(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients:   map[string]*Client{"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json"}},
		requestCh: reqCh,
		sendChP1:  make(chan outboundEnvelope, 16),
		p2Latest:  make(map[p2LatestKey]outboundEnvelope),
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
		sendChP1:  make(chan outboundEnvelope, 16),
		p2Latest:  make(map[p2LatestKey]outboundEnvelope),
	}

	cm.handleMessage([]byte("client-1"), []byte(`{"action":"do_magic","exchange":"bybit_native","symbol":"BTC/USDT","market_type":"spot"}`))
	if got := len(drainRequests(reqCh)); got != 0 {
		t.Fatalf("expected unknown action to be dropped, got %d forwarded requests", got)
	}
}

func TestHandleMessageSubscribeAllRejected(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients:   map[string]*Client{"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json"}},
		requestCh: reqCh,
		sendChP1:  make(chan outboundEnvelope, 16),
		p2Latest:  make(map[p2LatestKey]outboundEnvelope),
	}

	cm.handleMessage([]byte("client-1"), []byte(`{"action":"subscribe_all","exchange":"binance_native","market_type":"spot"}`))

	if got := len(drainRequests(reqCh)); got != 0 {
		t.Fatalf("expected subscribe_all to be rejected locally, got %d forwarded requests", got)
	}
	if got := len(cm.sendChP1); got != 1 {
		t.Fatalf("expected one error response, got %d", got)
	}
}

func TestHandleMessageEncodingUpdate(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients:   map[string]*Client{"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json", Role: clientRoleFeed}},
		requestCh: reqCh,
		sendChP1:  make(chan outboundEnvelope, 16),
		p2Latest:  make(map[p2LatestKey]outboundEnvelope),
	}

	cm.handleMessage([]byte("client-1"), []byte(`{"encoding":"binary"}`))

	cm.clientsMu.RLock()
	encoding := cm.clients["client-1"].Encoding
	cm.clientsMu.RUnlock()

	if encoding != "msgpack" {
		t.Fatalf("expected encoding msgpack, got %q", encoding)
	}
}

func TestHandleMessageControlRolePersistsAndCleansUpSubscriptions(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients: map[string]*Client{
			"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json", Role: clientRoleFeed},
		},
		requestCh: reqCh,
		sendChP1:  make(chan outboundEnvelope, 16),
		p2Latest:  make(map[p2LatestKey]outboundEnvelope),
	}

	cm.handleMessage([]byte("client-1"), []byte(`{"client_role":"control","action":"get_runtime_snapshot","request_id":"snap-1"}`))

	reqs := drainRequests(reqCh)
	if len(reqs) != 2 {
		t.Fatalf("expected cleanup disconnect plus runtime snapshot, got %+v", reqs)
	}
	if reqs[0].Action != "disconnect" {
		t.Fatalf("expected first request to cleanup subscriptions, got %+v", reqs[0])
	}
	if reqs[1].Action != "get_runtime_snapshot" || reqs[1].RequestID != "snap-1" {
		t.Fatalf("expected runtime snapshot request, got %+v", reqs[1])
	}
	if role := cm.clients["client-1"].Role; role != clientRoleControl {
		t.Fatalf("expected persisted control role, got %q", role)
	}
}

func TestHandleMessageControlClientRejectsSubscribeBulk(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients: map[string]*Client{
			"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json", Role: clientRoleControl},
		},
		requestCh: reqCh,
		sendChP1:  make(chan outboundEnvelope, 16),
		p2Latest:  make(map[p2LatestKey]outboundEnvelope),
	}

	payload := []byte(`{"action":"subscribe_bulk","request_id":"deploy-123","exchange":"bybit_native","symbols":["BTCUSDT"],"market_type":"spot","data_type":"orderbooks"}`)
	cm.handleMessage([]byte("client-1"), payload)

	if got := len(drainRequests(reqCh)); got != 0 {
		t.Fatalf("expected no forwarded requests for control client subscribe, got %d", got)
	}
	if got := len(cm.sendChP1); got != 1 {
		t.Fatalf("expected one error response, got %d", got)
	}
}

func TestHandleMessageControlClientAllowsStickySubscribeBulk(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients: map[string]*Client{
			"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json", Role: clientRoleControl},
		},
		requestCh: reqCh,
		sendChP1:  make(chan outboundEnvelope, 16),
		p2Latest:  make(map[p2LatestKey]outboundEnvelope),
	}

	payload := []byte(`{"client_role":"control","action":"subscribe_bulk","sticky":true,"request_id":"deploy-123","exchange":"bybit_native","symbols":["BTCUSDT"],"market_type":"spot","data_type":"orderbooks"}`)
	cm.handleMessage([]byte("client-1"), payload)

	reqs := drainRequests(reqCh)
	if len(reqs) != 2 {
		t.Fatalf("expected deploy register plus subscribe, got %+v", reqs)
	}
	if reqs[1].Action != "subscribe" || !reqs[1].Sticky {
		t.Fatalf("expected sticky subscribe request, got %+v", reqs[1])
	}
	if got := len(cm.sendChP1); got != 0 {
		t.Fatalf("expected no local error response, got %d", got)
	}
}

func TestHandleMessageConcurrentClientStateAccess(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 10)
	cm := &ClientManager{
		clients:   map[string]*Client{"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json", Role: clientRoleFeed}},
		requestCh: reqCh,
		sendChP1:  make(chan outboundEnvelope, 16),
		p2Latest:  make(map[p2LatestKey]outboundEnvelope),
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
		sendChP1: make(chan outboundEnvelope, 1),
		p2Latest: make(map[p2LatestKey]outboundEnvelope),
		clients: map[string]*Client{
			"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json"},
		},
	}
	cm.sendChP1 <- outboundEnvelope{msg: zmq4.NewMsgFrom([]byte("client-1"), []byte("filled")), metricType: "trade", priority: priorityP1}

	done := make(chan struct{})
	go func() {
		cm.enqueueSocketSend(outboundEnvelope{msg: zmq4.NewMsgFrom([]byte("client-1"), []byte("drop-if-full")), metricType: "trade", priority: priorityP1})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(200 * time.Millisecond):
		t.Fatalf("enqueueSocketSend blocked on full channel")
	}
}

func TestDistributeOrderBookBatchUsesUnifiedSendQueue(t *testing.T) {
	cm := &ClientManager{
		clients: map[string]*Client{
			"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json", Role: clientRoleFeed},
		},
		sendChP1: make(chan outboundEnvelope, 4),
		p2Latest: make(map[p2LatestKey]outboundEnvelope),
	}

	updates := []*shared_types.OrderBookUpdate{
		{Exchange: "bybit", Symbol: "BTCUSDT", MarketType: "spot", Bids: []shared_types.OrderBookLevel{{Price: 1, Amount: 1}}, Asks: []shared_types.OrderBookLevel{{Price: 2, Amount: 1}}},
		{Exchange: "bybit", Symbol: "ETHUSDT", MarketType: "spot", Bids: []shared_types.OrderBookLevel{{Price: 1, Amount: 1}, {Price: 0.9, Amount: 1}}, Asks: []shared_types.OrderBookLevel{{Price: 2, Amount: 1}}},
	}

	cm.distributeOrderBookBatch([][]byte{[]byte("client-1")}, updates)

	if got := len(cm.sendChP1); got != 1 {
		t.Fatalf("expected a single direct queue message, got %d", got)
	}
	if gotP2 := len(cm.p2Latest); gotP2 != 0 {
		t.Fatalf("expected no p2 backlog, got %d", gotP2)
	}
}

func TestDistributeTradeBatchSkipsControlClients(t *testing.T) {
	cm := &ClientManager{
		clients: map[string]*Client{
			"feed-1":    {ID: []byte("feed-1"), LastPong: time.Now(), Encoding: "json", Role: clientRoleFeed},
			"control-1": {ID: []byte("control-1"), LastPong: time.Now(), Encoding: "json", Role: clientRoleControl},
		},
		sendChP1: make(chan outboundEnvelope, 4),
		p2Latest: make(map[p2LatestKey]outboundEnvelope),
	}

	trades := []*shared_types.TradeUpdate{
		{Exchange: "bybit", Symbol: "BTCUSDT", MarketType: "spot", Price: 1, Amount: 1},
	}

	cm.distributeTradeBatch([][]byte{[]byte("feed-1"), []byte("control-1")}, trades)

	if got := len(cm.sendChP1); got != 1 {
		t.Fatalf("expected only feed client to receive trade batch, got %d queue items", got)
	}
}

func TestEnqueueSocketSendWarnsThenDisconnectsSlowClient(t *testing.T) {
	reqCh := make(chan *shared_types.ClientRequest, 2)
	cm := &ClientManager{
		requestCh: reqCh,
		sendChP1:  make(chan outboundEnvelope, 1),
		p2Latest:  make(map[p2LatestKey]outboundEnvelope),
		clients: map[string]*Client{
			"client-1": {ID: []byte("client-1"), LastPong: time.Now(), Encoding: "json"},
		},
	}
	cm.sendChP1 <- outboundEnvelope{msg: zmq4.NewMsgFrom([]byte("client-1"), []byte("filled")), metricType: "trade", priority: priorityP1}

	var logBuf bytes.Buffer
	prevWriter := log.Writer()
	prevFlags := log.Flags()
	log.SetOutput(&logBuf)
	log.SetFlags(0)
	defer func() {
		log.SetOutput(prevWriter)
		log.SetFlags(prevFlags)
	}()

	cm.enqueueSocketSend(outboundEnvelope{msg: zmq4.NewMsgFrom([]byte("client-1"), []byte("drop-1")), metricType: "trade", priority: priorityP1})
	cm.enqueueSocketSend(outboundEnvelope{msg: zmq4.NewMsgFrom([]byte("client-1"), []byte("drop-2")), metricType: "trade", priority: priorityP1})

	if _, exists := cm.clients["client-1"]; exists {
		t.Fatalf("expected slow client to be removed after repeated queue overflow")
	}

	reqs := drainRequests(reqCh)
	if len(reqs) != 1 || reqs[0].Action != "disconnect" {
		t.Fatalf("expected one disconnect request, got %+v", reqs)
	}

	got := logBuf.String()
	if !bytes.Contains([]byte(got), []byte("verursacht Sendestau")) {
		t.Fatalf("expected slow-client warning log, got %q", got)
	}
	if !bytes.Contains([]byte(got), []byte("wegen wiederholtem Sendestau getrennt")) {
		t.Fatalf("expected slow-client disconnect log, got %q", got)
	}
}
