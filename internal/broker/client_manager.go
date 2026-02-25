package broker

import (
	"bytes"
	"context"
	"log"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
	"github.com/go-zeromq/zmq4"
	json "github.com/goccy/go-json"
	"github.com/vmihailenco/msgpack/v5"
)

const (
	pingInterval   = 15 * time.Second
	clientTimeout  = 45 * time.Second
	HeaderJSON     = "J"
	HeaderTradeBin = "T"
	HeaderOBBin    = "O"
	p2LatestMax    = 200000
)

var (
	headerTradeBinFrame = []byte(HeaderTradeBin)
	headerOBBinFrame    = []byte(HeaderOBBin)
	msgpackCtxPool      = sync.Pool{
		New: func() any {
			buf := &bytes.Buffer{}
			buf.Grow(8 * 1024)
			return &msgpackEncoderCtx{
				buf: buf,
				enc: msgpack.NewEncoder(buf),
			}
		},
	}
)

type msgpackEncoderCtx struct {
	buf *bytes.Buffer
	enc *msgpack.Encoder
}

type Client struct {
	ID       []byte
	LastPong time.Time
	Encoding string
}

type outboundEnvelope struct {
	msg         zmq4.Msg
	metricType  string
	ingestNano  int64
	ingestNanos []int64
	priority    sendPriority
	p2Key       p2LatestKey
}

type sendPriority uint8

const (
	priorityP1 sendPriority = iota + 1
	priorityP2
)

type perEncodingOrderBookCache struct {
	jsonByOB    map[*shared_types.OrderBookUpdate][]byte
	msgpackByOB map[*shared_types.OrderBookUpdate][]byte
}

type p2LatestKey struct {
	clientID   string
	exchange   string
	marketType string
	symbol     string
}

type obBatchLatestKey struct {
	exchange   string
	marketType string
	symbol     string
}

type ClientManager struct {
	socket         zmq4.Socket
	clients        map[string]*Client
	clientsMu      sync.RWMutex
	requestCh      chan<- *shared_types.ClientRequest
	DistributionCh chan *DistributionMessage
	sendChP1       chan outboundEnvelope
	p2Mu           sync.Mutex
	p2Latest       map[p2LatestKey]outboundEnvelope
	bindAddress    string
}

type incomingClientMessage struct {
	Action         string   `json:"action"`
	Exchange       string   `json:"exchange"`
	Symbol         string   `json:"symbol"`
	Symbols        []string `json:"symbols"`
	MarketType     string   `json:"market_type"`
	DataType       string   `json:"data_type"`
	OrderBookDepth int      `json:"depth,omitempty"`
	Encoding       string   `json:"encoding"`
	Message        string   `json:"message"`
}

func NewClientManager(requestCh chan<- *shared_types.ClientRequest) *ClientManager {
	socket := zmq4.NewRouter(context.Background(), zmq4.WithID([]byte("broker-router")))

	var bindAddr string
	if runtime.GOOS == "linux" {
		bindAddr = "ipc:///tmp/feed_broker.ipc"
		log.Println("[NETWORK] Linux erkannt: Nutze High-Performance IPC (ipc:///tmp/feed_broker.ipc)")
	} else {
		bindAddr = "tcp://*:5555"
		log.Println("[NETWORK] Windows/Mac erkannt: Nutze Standard TCP (tcp://*:5555)")
	}

	if strings.HasPrefix(bindAddr, "ipc://") {
		path := strings.TrimPrefix(bindAddr, "ipc://")
		_ = os.Remove(path)
	}

	if err := socket.Listen(bindAddr); err != nil {
		log.Fatalf("[ZMQ-FATAL] Socket konnte nicht gebunden werden auf %s: %v", bindAddr, err)
	}

	return &ClientManager{
		socket:         socket,
		clients:        make(map[string]*Client),
		requestCh:      requestCh,
		DistributionCh: make(chan *DistributionMessage, 10000),
		sendChP1:       make(chan outboundEnvelope, 4096),
		p2Latest:       make(map[p2LatestKey]outboundEnvelope, 4096),
		bindAddress:    bindAddr,
	}
}

func (cm *ClientManager) Run() {
	log.Println("[CLIENT-MANAGER] Startet...")
	log.Printf("[CLIENT-MANAGER] Lauscht auf ZMQ-Anfragen unter %s", cm.bindAddress)

	go cm.readLoop()
	go cm.distributionLoop()
	go cm.writeLoop()
	go cm.healthCheckLoop()
}

func (cm *ClientManager) readLoop() {
	for {
		msg, err := cm.socket.Recv()
		if err != nil {
			log.Printf("[ZMQ-RECV-ERROR] %v", err)
			continue
		}

		if len(msg.Frames) < 2 {
			continue
		}
		clientID := msg.Frames[0]
		payload := msg.Frames[len(msg.Frames)-1]

		clientIDStr := string(clientID)
		cm.clientsMu.Lock()
		if _, ok := cm.clients[clientIDStr]; !ok {
			log.Printf("[CLIENT-MANAGER] Neuer Client verbunden: %s", clientIDStr)
			cm.clients[clientIDStr] = &Client{ID: append([]byte(nil), clientID...), LastPong: time.Now(), Encoding: "json"}
		}
		cm.clientsMu.Unlock()

		cm.handleMessage(clientID, payload)
	}
}

func (cm *ClientManager) distributionLoop() {
	for msg := range cm.DistributionCh {
		switch payload := msg.RawPayload.(type) {
		case []*shared_types.TradeUpdate:
			cm.distributeTradeBatch(msg.ClientIDs, payload)
		case []*shared_types.OrderBookUpdate:
			cm.distributeOrderBookBatch(msg.ClientIDs, payload)
		default:
			continue
		}

		if msg.OnComplete != nil {
			msg.OnComplete()
		}
	}
}

func (cm *ClientManager) distributeTradeBatch(clientIDs [][]byte, trades []*shared_types.TradeUpdate) {
	if len(trades) == 0 {
		return
	}

	ingestNanos := make([]int64, 0, len(trades))
	for _, t := range trades {
		if t != nil && t.IngestUnixNano > 0 {
			ingestNanos = append(ingestNanos, t.IngestUnixNano)
		}
	}

	var jsonCache []byte
	var msgpackCache []byte
	for _, clientID := range clientIDs {
		clientIDStr := string(clientID)
		encoding, exists := cm.clientEncoding(clientIDStr)
		if !exists {
			continue
		}

		msg, ok := cm.encodePayloadForClient(clientID, encoding, HeaderTradeBin, trades, &jsonCache, &msgpackCache, metrics.TypeTrade)
		if !ok {
			continue
		}
		cm.enqueueSocketSend(outboundEnvelope{
			msg:         msg,
			metricType:  metrics.TypeTrade,
			ingestNanos: ingestNanos,
			priority:    priorityP1,
		})
	}
}

func (cm *ClientManager) distributeOrderBookBatch(clientIDs [][]byte, updates []*shared_types.OrderBookUpdate) {
	if len(updates) == 0 {
		return
	}

	// P2 messages are latest-only by design. Collapse to latest per symbol in-batch
	// before doing any client-specific encoding work.
	p1Updates := make([]*shared_types.OrderBookUpdate, 0, len(updates))
	p2LatestBySymbol := make(map[obBatchLatestKey]*shared_types.OrderBookUpdate, len(updates))
	p2Order := make([]obBatchLatestKey, 0, len(updates))

	for _, ob := range updates {
		if ob == nil {
			continue
		}
		if classifyOrderBookPriority(ob) == priorityP1 {
			p1Updates = append(p1Updates, ob)
			continue
		}

		key := obBatchLatestKey{
			exchange:   ob.Exchange,
			marketType: ob.MarketType,
			symbol:     ob.Symbol,
		}
		if _, exists := p2LatestBySymbol[key]; !exists {
			p2Order = append(p2Order, key)
		}
		p2LatestBySymbol[key] = ob
	}

	if len(p1Updates) == 0 && len(p2Order) == 0 {
		return
	}

	// Reuse serialized single-update envelopes across clients in this batch.
	// This removes repeated msgpack/json marshal for identical OB pointers.
	cacheSize := len(p1Updates) + len(p2Order)
	encodedCache := perEncodingOrderBookCache{
		jsonByOB:    make(map[*shared_types.OrderBookUpdate][]byte, cacheSize),
		msgpackByOB: make(map[*shared_types.OrderBookUpdate][]byte, cacheSize),
	}

	for _, clientID := range clientIDs {
		clientIDStr := string(clientID)
		encoding, exists := cm.clientEncoding(clientIDStr)
		if !exists {
			continue
		}

		for _, ob := range p1Updates {
			metricType := orderBookEnvelopeType(ob)
			cm.enqueueOrderBookEnvelope(clientID, clientIDStr, encoding, ob, metricType, priorityP1, &encodedCache)
		}

		for _, key := range p2Order {
			ob := p2LatestBySymbol[key]
			if ob == nil {
				continue
			}
			metricType := orderBookEnvelopeType(ob)
			cm.enqueueOrderBookEnvelope(clientID, clientIDStr, encoding, ob, metricType, priorityP2, &encodedCache)
		}
	}
}

func (cm *ClientManager) enqueueOrderBookEnvelope(
	clientID []byte,
	clientIDStr string,
	encoding string,
	ob *shared_types.OrderBookUpdate,
	metricType string,
	priority sendPriority,
	encodedCache *perEncodingOrderBookCache,
) {
	if ob == nil {
		return
	}
	msg, ok := cm.encodeSingleOrderBookForClient(clientID, encoding, ob, metricType, encodedCache)
	if !ok {
		return
	}

	var p2Key p2LatestKey
	if priority == priorityP2 {
		p2Key = p2LatestKey{
			clientID:   clientIDStr,
			exchange:   ob.Exchange,
			marketType: ob.MarketType,
			symbol:     ob.Symbol,
		}
	}

	cm.enqueueSocketSend(outboundEnvelope{
		msg:        msg,
		metricType: metricType,
		ingestNano: ob.IngestUnixNano,
		priority:   priority,
		p2Key:      p2Key,
	})
}

func (cm *ClientManager) clientEncoding(clientID string) (string, bool) {
	cm.clientsMu.RLock()
	client, exists := cm.clients[clientID]
	encoding := ""
	if exists {
		encoding = client.Encoding
	}
	cm.clientsMu.RUnlock()
	return encoding, exists
}

func (cm *ClientManager) encodePayloadForClient(
	clientID []byte,
	encoding string,
	msgTypeHeader string,
	payload any,
	jsonCache *[]byte,
	msgpackCache *[]byte,
	metricType string,
) (zmq4.Msg, bool) {
	if encoding == "msgpack" || encoding == "binary" {
		if *msgpackCache == nil {
			binPayload, err := marshalMsgpack(payload)
			if err != nil {
				metrics.RecordDropped(metrics.ReasonInternalErr, metricType)
				log.Printf("[MSGPACK ERROR] %v", err)
				return zmq4.Msg{}, false
			}
			*msgpackCache = binPayload
		}
		if msgTypeHeader == HeaderTradeBin {
			return zmq4.NewMsgFrom(clientID, headerTradeBinFrame, *msgpackCache), true
		}
		return zmq4.NewMsgFrom(clientID, []byte(msgTypeHeader), *msgpackCache), true
	}

	if *jsonCache == nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			metrics.RecordDropped(metrics.ReasonInternalErr, metricType)
			return zmq4.Msg{}, false
		}
		*jsonCache = raw
	}
	return zmq4.NewMsgFrom(clientID, *jsonCache), true
}

func (cm *ClientManager) encodeSingleOrderBookForClient(
	clientID []byte,
	encoding string,
	ob *shared_types.OrderBookUpdate,
	metricType string,
	cache *perEncodingOrderBookCache,
) (zmq4.Msg, bool) {
	var payload []byte
	var ok bool

	if encoding == "msgpack" || encoding == "binary" {
		payload, ok = cache.msgpackByOB[ob]
		if !ok {
			binPayload, err := marshalSingleOBAsMsgpackArray(ob)
			if err != nil {
				metrics.RecordDropped(metrics.ReasonInternalErr, metricType)
				log.Printf("[MSGPACK ERROR] %v", err)
				return zmq4.Msg{}, false
			}
			cache.msgpackByOB[ob] = binPayload
			payload = binPayload
		}
		return zmq4.NewMsgFrom(clientID, headerOBBinFrame, payload), true
	}

	payload, ok = cache.jsonByOB[ob]
	if !ok {
		single := [1]*shared_types.OrderBookUpdate{ob}
		raw, err := json.Marshal(single[:])
		if err != nil {
			metrics.RecordDropped(metrics.ReasonInternalErr, metricType)
			return zmq4.Msg{}, false
		}
		cache.jsonByOB[ob] = raw
		payload = raw
	}
	return zmq4.NewMsgFrom(clientID, payload), true
}

func orderBookEnvelopeType(ob *shared_types.OrderBookUpdate) string {
	if ob != nil && ob.UpdateType == metrics.TypeOBSnapshot {
		return metrics.TypeOBSnapshot
	}
	return metrics.TypeOBUpdate
}

func marshalMsgpack(v any) ([]byte, error) {
	ctx := msgpackCtxPool.Get().(*msgpackEncoderCtx)
	ctx.buf.Reset()
	ctx.enc.ResetWriter(ctx.buf)
	if err := ctx.enc.Encode(v); err != nil {
		msgpackCtxPool.Put(ctx)
		return nil, err
	}
	out := append([]byte(nil), ctx.buf.Bytes()...)
	msgpackCtxPool.Put(ctx)
	return out, nil
}

func marshalSingleOBAsMsgpackArray(ob *shared_types.OrderBookUpdate) ([]byte, error) {
	ctx := msgpackCtxPool.Get().(*msgpackEncoderCtx)
	ctx.buf.Reset()
	ctx.enc.ResetWriter(ctx.buf)
	if err := ctx.enc.EncodeArrayLen(1); err != nil {
		msgpackCtxPool.Put(ctx)
		return nil, err
	}
	if err := ctx.enc.Encode(ob); err != nil {
		msgpackCtxPool.Put(ctx)
		return nil, err
	}
	out := append([]byte(nil), ctx.buf.Bytes()...)
	msgpackCtxPool.Put(ctx)
	return out, nil
}

func classifyOrderBookPriority(ob *shared_types.OrderBookUpdate) sendPriority {
	if ob == nil {
		return priorityP2
	}
	if ob.UpdateType == metrics.TypeOBSnapshot {
		return priorityP2
	}
	if len(ob.Bids) <= 1 && len(ob.Asks) <= 1 {
		return priorityP1
	}
	return priorityP2
}

func (cm *ClientManager) writeLoop() {
	flushTicker := time.NewTicker(25 * time.Millisecond)
	defer flushTicker.Stop()

	for {
		if cm.drainP1() {
			continue
		}

		select {
		case outbound := <-cm.sendChP1:
			cm.sendOutbound(outbound)
		case <-flushTicker.C:
			if len(cm.sendChP1) > 0 {
				continue
			}
			if outbound, ok := cm.popP2LatestDeterministic(); ok {
				cm.sendOutbound(outbound)
			}
		}
	}
}

func (cm *ClientManager) healthCheckLoop() {
	type clientSnapshot struct {
		id       string
		clientID []byte
		lastPong time.Time
	}

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		pingPayload, _ := json.Marshal(map[string]string{"type": "ping"})
		clientsToRemove := make([]string, 0)

		cm.clientsMu.RLock()
		snapshots := make([]clientSnapshot, 0, len(cm.clients))
		for id, client := range cm.clients {
			snapshots = append(snapshots, clientSnapshot{id: id, clientID: append([]byte(nil), client.ID...), lastPong: client.LastPong})
		}
		cm.clientsMu.RUnlock()

		for _, client := range snapshots {
			if now.Sub(client.lastPong) > clientTimeout {
				clientsToRemove = append(clientsToRemove, client.id)
				continue
			}
			cm.enqueueSocketSend(outboundEnvelope{msg: zmq4.NewMsgFrom(client.clientID, []byte{}, pingPayload), metricType: metrics.TypeTrade})
		}

		for _, id := range clientsToRemove {
			var removedClientID []byte
			cm.clientsMu.Lock()
			if client, ok := cm.clients[id]; ok {
				if now.Sub(client.LastPong) > clientTimeout {
					removedClientID = append([]byte(nil), client.ID...)
					delete(cm.clients, id)
				}
			}
			cm.clientsMu.Unlock()
			if len(removedClientID) == 0 {
				continue
			}
			log.Printf("[CLIENT-MANAGER] Client %s wegen Timeout entfernt.", id)
			cm.enqueueRequest(&shared_types.ClientRequest{ClientID: removedClientID, Action: "disconnect"})
		}
	}
}

func (cm *ClientManager) handleMessage(clientID []byte, payload []byte) {
	var req incomingClientMessage
	if err := json.Unmarshal(payload, &req); err != nil {
		metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
		cm.sendClientError(clientID, "invalid_json", "payload must be valid JSON")
		return
	}

	clientIDStr := string(clientID)
	if req.Message == "pong" {
		cm.clientsMu.Lock()
		if client, exists := cm.clients[clientIDStr]; exists {
			client.LastPong = time.Now()
		}
		cm.clientsMu.Unlock()
		return
	}

	if req.Encoding != "" {
		encoding, ok := normalizeClientEncoding(req.Encoding)
		if !ok {
			cm.sendClientError(clientID, "invalid_encoding", "encoding must be one of: json, msgpack, binary")
			return
		}
		cm.clientsMu.Lock()
		if client, exists := cm.clients[clientIDStr]; exists {
			client.Encoding = encoding
		}
		cm.clientsMu.Unlock()
		log.Printf("[CLIENT-MANAGER] Client %s setzt Encoding auf: %s", clientIDStr, encoding)
	}

	if req.Action == "" {
		if req.Encoding == "" {
			cm.sendClientError(clientID, "missing_action", "action field is required")
		}
		return
	}
	if req.DataType == "" {
		req.DataType = "trades"
	}

	switch req.Action {
	case "subscribe_bulk":
		if req.Exchange == "" || req.MarketType == "" || len(req.Symbols) == 0 {
			cm.sendClientError(clientID, "invalid_request", "subscribe_bulk requires exchange, market_type and symbols")
			return
		}
		log.Printf("[CLIENT-MANAGER] 'subscribe_bulk': %d Symbole von %s", len(req.Symbols), clientIDStr)
		for _, symbol := range req.Symbols {
			if symbol == "" {
				continue
			}
			cm.enqueueRequest(&shared_types.ClientRequest{ClientID: clientID, Action: "subscribe", Exchange: req.Exchange, Symbol: symbol, MarketType: req.MarketType, DataType: req.DataType, OrderBookDepth: req.OrderBookDepth})
		}
		return
	case "subscribe_all":
		if req.Exchange == "" || req.MarketType == "" {
			cm.sendClientError(clientID, "invalid_request", "subscribe_all requires exchange and market_type")
			return
		}
	case "subscribe", "unsubscribe":
		if req.Exchange == "" || req.MarketType == "" || req.Symbol == "" {
			cm.sendClientError(clientID, "invalid_request", "subscribe/unsubscribe requires exchange, symbol and market_type")
			return
		}
	case "disconnect":
		cm.enqueueRequest(&shared_types.ClientRequest{ClientID: clientID, Action: "disconnect"})
		return
	default:
		cm.sendClientError(clientID, "unknown_action", "unsupported action")
		return
	}

	cm.enqueueRequest(&shared_types.ClientRequest{ClientID: clientID, Action: req.Action, Exchange: req.Exchange, Symbol: req.Symbol, MarketType: req.MarketType, DataType: req.DataType, OrderBookDepth: req.OrderBookDepth})
}

func (cm *ClientManager) enqueueRequest(req *shared_types.ClientRequest) {
	if cm.requestCh == nil {
		return
	}
	cm.requestCh <- req
}

func (cm *ClientManager) sendClientError(clientID []byte, code, message string) {
	payload, err := json.Marshal(map[string]string{"type": "error", "code": code, "message": message})
	if err != nil {
		metrics.RecordDropped(metrics.ReasonInternalErr, metrics.TypeTrade)
		return
	}
	cm.enqueueSocketSend(outboundEnvelope{msg: zmq4.NewMsgFrom(clientID, payload), metricType: metrics.TypeTrade})
}

func normalizeClientEncoding(input string) (string, bool) {
	switch strings.ToLower(input) {
	case "json":
		return "json", true
	case "msgpack", "binary":
		return "msgpack", true
	default:
		return "", false
	}
}

func (cm *ClientManager) enqueueSocketSend(envelope outboundEnvelope) bool {
	if envelope.priority == priorityP2 {
		return cm.enqueueP2Latest(envelope)
	}

	select {
	case cm.sendChP1 <- envelope:
		return true
	default:
		metrics.RecordDropped(metrics.ReasonBufferFull, envelope.metricType)
		if caller, stack, ok := logDropCallsite(envelope.metricType, metrics.ReasonBufferFull); ok {
			log.Printf("DROP_CALLSITE type=%s reason=%s caller=%s stack=%s", envelope.metricType, metrics.ReasonBufferFull, caller, stack)
		}
		return false
	}
}

func (cm *ClientManager) enqueueP2Latest(envelope outboundEnvelope) bool {
	if envelope.p2Key.clientID == "" {
		metrics.RecordDropped(metrics.ReasonInternalErr, envelope.metricType)
		return false
	}

	cm.p2Mu.Lock()
	if _, exists := cm.p2Latest[envelope.p2Key]; !exists && len(cm.p2Latest) >= p2LatestMax {
		cm.p2Mu.Unlock()
		metrics.RecordDropped(metrics.ReasonBufferFull, envelope.metricType)
		if caller, stack, ok := logDropCallsite(envelope.metricType, metrics.ReasonBufferFull); ok {
			log.Printf("DROP_CALLSITE type=%s reason=%s caller=%s stack=%s", envelope.metricType, metrics.ReasonBufferFull, caller, stack)
		}
		return false
	}
	cm.p2Latest[envelope.p2Key] = envelope
	cm.p2Mu.Unlock()
	return true
}

func (cm *ClientManager) popP2LatestDeterministic() (outboundEnvelope, bool) {
	cm.p2Mu.Lock()
	defer cm.p2Mu.Unlock()

	if len(cm.p2Latest) == 0 {
		return outboundEnvelope{}, false
	}

	var (
		minKey p2LatestKey
		hasKey bool
	)
	for key := range cm.p2Latest {
		if !hasKey || lessP2Key(key, minKey) {
			minKey = key
			hasKey = true
		}
	}

	outbound := cm.p2Latest[minKey]
	delete(cm.p2Latest, minKey)
	return outbound, true
}

func lessP2Key(a, b p2LatestKey) bool {
	if a.clientID != b.clientID {
		return a.clientID < b.clientID
	}
	if a.exchange != b.exchange {
		return a.exchange < b.exchange
	}
	if a.marketType != b.marketType {
		return a.marketType < b.marketType
	}
	return a.symbol < b.symbol
}

func (cm *ClientManager) drainP1() bool {
	drained := false
	for {
		select {
		case outbound := <-cm.sendChP1:
			drained = true
			cm.sendOutbound(outbound)
		default:
			return drained
		}
	}
}

func (cm *ClientManager) sendOutbound(outbound outboundEnvelope) {
	now := time.Now()
	if err := cm.socket.Send(outbound.msg); err != nil {
		metrics.RecordDropped(metrics.ReasonInternalErr, outbound.metricType)
		return
	}
	metrics.RecordPublish(outbound.metricType)
	if outbound.ingestNano > 0 {
		metrics.ObserveProcessing(outbound.metricType, now.Sub(time.Unix(0, outbound.ingestNano)).Seconds())
	}
	for _, ingest := range outbound.ingestNanos {
		if ingest > 0 {
			metrics.ObserveProcessing(outbound.metricType, now.Sub(time.Unix(0, ingest)).Seconds())
		}
	}
}

func (cm *ClientManager) SendQueueStats() (p1Len int, p1Cap int, p2Len int, p2Cap int) {
	cm.p2Mu.Lock()
	p2Len = len(cm.p2Latest)
	cm.p2Mu.Unlock()
	return len(cm.sendChP1), cap(cm.sendChP1), p2Len, p2LatestMax
}
