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
	pingInterval    = 15 * time.Second
	clientTimeout   = 45 * time.Second
	slowWarnWindow  = 30 * time.Second
	HeaderJSON      = "J"
	HeaderTradeBin  = "T"
	HeaderOBBin     = "O"
	p2LatestMax     = 200000
	p1BurstBeforeP2 = 2
)

const (
	clientRoleFeed    = "feed"
	clientRoleControl = "control"
)

var (
	headerTradeBinFrame = []byte(HeaderTradeBin)
	headerOBBinFrame    = []byte(HeaderOBBin)
	emptyFrame          = []byte{}
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
	ID              []byte
	LastPong        time.Time
	Encoding        string
	Role            string
	SlowQueueDrops  int
	LastQueueDropAt time.Time
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
	StatusCh       chan *shared_types.StreamStatusEvent
	DistributionCh chan *DistributionMessage
	sendChP1       chan outboundEnvelope
	p2Mu           sync.Mutex
	p2Latest       map[p2LatestKey]outboundEnvelope
	bindAddress    string
}

type incomingClientMessage struct {
	Action         string   `json:"action"`
	Scope          string   `json:"scope"`
	RequestID      string   `json:"request_id"`
	ClientRole     string   `json:"client_role"`
	Sticky         bool     `json:"sticky"`
	Exchange       string   `json:"exchange"`
	Symbol         string   `json:"symbol"`
	Symbols        []string `json:"symbols"`
	MarketType     string   `json:"market_type"`
	DataType       string   `json:"data_type"`
	CacheN         int      `json:"cache_n,omitempty"`
	OrderBookDepth int      `json:"depth,omitempty"`
	OrderBookFreq  string   `json:"frequency,omitempty"`
	PushIntervalMS int      `json:"push_interval_ms,omitempty"`
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
		StatusCh:       make(chan *shared_types.StreamStatusEvent, 1024),
		DistributionCh: make(chan *DistributionMessage, 10000),
		sendChP1:       make(chan outboundEnvelope, 16384),
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
			cm.clients[clientIDStr] = &Client{ID: append([]byte(nil), clientID...), LastPong: time.Now(), Encoding: "json", Role: clientRoleFeed}
		}
		cm.clientsMu.Unlock()

		cm.handleMessage(clientID, payload)
	}
}

func (cm *ClientManager) distributionLoop() {
	for msg := range cm.DistributionCh {
		clientIDs := msg.ClientIDs
		if msg.Broadcast {
			clientIDs = cm.snapshotClientIDs()
			if len(clientIDs) == 0 {
				if msg.OnComplete != nil {
					msg.OnComplete()
				}
				continue
			}
		}

		switch payload := msg.RawPayload.(type) {
		case []*shared_types.TradeUpdate:
			cm.distributeTradeBatch(clientIDs, payload)
		case []*shared_types.OrderBookUpdate:
			cm.distributeOrderBookBatch(clientIDs, payload)
		case *shared_types.StreamStatusEvent:
			cm.distributeStatusEvent(clientIDs, payload)
		case *shared_types.SubscriptionsSnapshotResponse:
			cm.distributeJSONPayload(clientIDs, payload)
		case *shared_types.SubscriptionHealthSnapshotResponse:
			cm.distributeJSONPayload(clientIDs, payload)
		case *shared_types.RuntimeSnapshotResponse:
			cm.distributeJSONPayload(clientIDs, payload)
		case *shared_types.CapabilitiesSnapshotResponse:
			cm.distributeJSONPayload(clientIDs, payload)
		case *shared_types.DeployBatchSummaryEvent:
			cm.distributeJSONPayload(clientIDs, payload)
		case *shared_types.RuntimeTotalsTickEvent:
			cm.distributeJSONPayload(clientIDs, payload)
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
		if !exists || cm.isControlClient(clientIDStr) {
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
	for _, clientID := range clientIDs {
		clientIDStr := string(clientID)
		encoding, exists := cm.clientEncoding(clientIDStr)
		if !exists || cm.isControlClient(clientIDStr) {
			continue
		}
		encodedCache := &perEncodingOrderBookCache{
			jsonByOB:    make(map[*shared_types.OrderBookUpdate][]byte, len(updates)),
			msgpackByOB: make(map[*shared_types.OrderBookUpdate][]byte, len(updates)),
		}
		for _, ob := range updates {
			cm.enqueueOrderBookEnvelope(
				clientID,
				clientIDStr,
				encoding,
				ob,
				orderBookEnvelopeType(ob),
				priorityP1,
				encodedCache,
			)
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

	envelope := outboundEnvelope{
		msg:        msg,
		metricType: metricType,
		ingestNano: ob.IngestUnixNano,
		priority:   priority,
		p2Key:      p2Key,
	}
	if priority == priorityP2 && cm.enqueueP2Latest(envelope) {
		return
	}
	cm.enqueueSocketSend(envelope)
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

func (cm *ClientManager) snapshotClientIDs() [][]byte {
	cm.clientsMu.RLock()
	defer cm.clientsMu.RUnlock()

	clientIDs := make([][]byte, 0, len(cm.clients))
	for _, client := range cm.clients {
		clientIDs = append(clientIDs, append([]byte(nil), client.ID...))
	}
	return clientIDs
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
		switch msgTypeHeader {
		case HeaderTradeBin:
			return zmq4.NewMsgFrom(clientID, headerTradeBinFrame, *msgpackCache), true
		case HeaderOBBin:
			return zmq4.NewMsgFrom(clientID, headerOBBinFrame, *msgpackCache), true
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

func (cm *ClientManager) writeLoop() {
	p1BurstCount := 0
	for {
		if p1BurstCount >= p1BurstBeforeP2 {
			if outbound, ok := cm.popP2LatestDeterministic(); ok {
				cm.sendOutbound(outbound)
				p1BurstCount = 0
				continue
			}
			p1BurstCount = 0
		}

		select {
		case outbound := <-cm.sendChP1:
			cm.sendOutbound(outbound)
			p1BurstCount++
		default:
			if outbound, ok := cm.popP2LatestDeterministic(); ok {
				cm.sendOutbound(outbound)
				p1BurstCount = 0
				continue
			}
			outbound := <-cm.sendChP1
			cm.sendOutbound(outbound)
			p1BurstCount = 1
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
			cm.enqueueSocketSend(outboundEnvelope{msg: zmq4.NewMsgFrom(client.clientID, emptyFrame, pingPayload), metricType: metrics.TypeTrade})
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
		cm.sendClientError(clientID, "", "invalid_json", "payload must be valid JSON")
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

	roleChangedToControl := false
	if req.ClientRole != "" {
		role, ok := normalizeClientRole(req.ClientRole)
		if !ok {
			cm.sendClientError(clientID, req.RequestID, "invalid_client_role", "client_role must be one of: feed, control")
			return
		}
		cm.clientsMu.Lock()
		if client, exists := cm.clients[clientIDStr]; exists {
			prevRole := client.Role
			if prevRole == "" {
				prevRole = clientRoleFeed
			}
			client.Role = role
			roleChangedToControl = prevRole != role && role == clientRoleControl
		}
		cm.clientsMu.Unlock()
		log.Printf("[CLIENT-MANAGER] Client %s setzt Rolle auf: %s", clientIDStr, role)
		if roleChangedToControl {
			cm.enqueueRequest(&shared_types.ClientRequest{ClientID: clientID, Action: "disconnect"})
		}
	}

	if req.Encoding != "" {
		encoding, ok := normalizeClientEncoding(req.Encoding)
		if !ok {
			cm.sendClientError(clientID, req.RequestID, "invalid_encoding", "encoding must be one of: json, msgpack, binary")
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
			cm.sendClientError(clientID, req.RequestID, "missing_action", "action field is required")
		}
		return
	}
	if req.DataType == "" {
		req.DataType = "trades"
	}

	switch req.Action {
	case "list_subscriptions", "subscription_health_snapshot", "get_runtime_snapshot", "get_capabilities":
		cm.enqueueRequest(&shared_types.ClientRequest{ClientID: clientID, Action: req.Action, Scope: req.Scope, RequestID: req.RequestID})
		return
	case "subscribe_bulk":
		if cm.isControlClient(clientIDStr) && !req.Sticky {
			cm.sendClientError(clientID, req.RequestID, "invalid_request", "control clients can only subscribe to market data with sticky=true")
			return
		}
		if req.Exchange == "" || req.MarketType == "" || len(req.Symbols) == 0 {
			cm.sendClientError(clientID, req.RequestID, "invalid_request", "subscribe_bulk requires exchange, market_type and symbols")
			return
		}
		log.Printf(
			"[CLIENT-MANAGER] 'subscribe_bulk': client=%s exchange=%s market_type=%s data_type=%s symbols=%d",
			clientIDStr,
			req.Exchange,
			req.MarketType,
			req.DataType,
			len(req.Symbols),
		)
		cm.enqueueRequest(&shared_types.ClientRequest{ClientID: clientID, Action: "deploy_batch_register", RequestID: req.RequestID, BatchSent: len(req.Symbols)})
		for _, symbol := range req.Symbols {
			if symbol == "" {
				continue
			}
			cm.enqueueRequest(&shared_types.ClientRequest{ClientID: clientID, Action: "subscribe", RequestID: req.RequestID, Sticky: req.Sticky, Exchange: req.Exchange, Symbol: symbol, MarketType: req.MarketType, DataType: req.DataType, Encoding: currentClientEncoding(cm, clientIDStr), CacheN: req.CacheN, OrderBookDepth: req.OrderBookDepth, OrderBookFreq: req.OrderBookFreq, PushIntervalMS: req.PushIntervalMS})
		}
		return
	case "unsubscribe_bulk":
		if cm.isControlClient(clientIDStr) && !req.Sticky {
			cm.sendClientError(clientID, req.RequestID, "invalid_request", "control clients can only unsubscribe market data with sticky=true")
			return
		}
		if req.Exchange == "" || req.MarketType == "" || len(req.Symbols) == 0 {
			cm.sendClientError(clientID, req.RequestID, "invalid_request", "unsubscribe_bulk requires exchange, market_type and symbols")
			return
		}
		log.Printf(
			"[CLIENT-MANAGER] 'unsubscribe_bulk': client=%s exchange=%s market_type=%s data_type=%s symbols=%d",
			clientIDStr,
			req.Exchange,
			req.MarketType,
			req.DataType,
			len(req.Symbols),
		)
		cm.enqueueRequest(&shared_types.ClientRequest{ClientID: clientID, Action: "deploy_batch_register", RequestID: req.RequestID, BatchSent: len(req.Symbols)})
		for _, symbol := range req.Symbols {
			if symbol == "" {
				continue
			}
			cm.enqueueRequest(&shared_types.ClientRequest{ClientID: clientID, Action: "unsubscribe", RequestID: req.RequestID, Sticky: req.Sticky, Exchange: req.Exchange, Symbol: symbol, MarketType: req.MarketType, DataType: req.DataType, Encoding: currentClientEncoding(cm, clientIDStr), CacheN: req.CacheN, OrderBookDepth: req.OrderBookDepth, OrderBookFreq: req.OrderBookFreq, PushIntervalMS: req.PushIntervalMS})
		}
		return
	case "subscribe_all":
		cm.sendClientError(clientID, req.RequestID, "invalid_request", "subscribe_all is not supported; use subscribe_bulk")
		return
	case "subscribe", "unsubscribe":
		if cm.isControlClient(clientIDStr) && !req.Sticky {
			cm.sendClientError(clientID, req.RequestID, "invalid_request", "control clients can only manage market data with sticky=true")
			return
		}
		if req.Exchange == "" || req.MarketType == "" || req.Symbol == "" {
			cm.sendClientError(clientID, req.RequestID, "invalid_request", "subscribe/unsubscribe requires exchange, symbol and market_type")
			return
		}
	case "disconnect":
		cm.clientsMu.Lock()
		delete(cm.clients, clientIDStr)
		cm.clientsMu.Unlock()
		cm.enqueueRequest(&shared_types.ClientRequest{ClientID: clientID, Action: "disconnect", RequestID: req.RequestID})
		return
	default:
		cm.sendClientError(clientID, req.RequestID, "unknown_action", "unsupported action")
		return
	}

	cm.enqueueRequest(&shared_types.ClientRequest{ClientID: clientID, Action: req.Action, Scope: req.Scope, RequestID: req.RequestID, Sticky: req.Sticky, Exchange: req.Exchange, Symbol: req.Symbol, MarketType: req.MarketType, DataType: req.DataType, Encoding: currentClientEncoding(cm, clientIDStr), CacheN: req.CacheN, OrderBookDepth: req.OrderBookDepth, OrderBookFreq: req.OrderBookFreq, PushIntervalMS: req.PushIntervalMS})
}

func (cm *ClientManager) enqueueRequest(req *shared_types.ClientRequest) {
	if cm.requestCh == nil {
		return
	}
	cm.requestCh <- req
}

func (cm *ClientManager) sendClientError(clientID []byte, requestID, code, message string) {
	payload, err := json.Marshal(&shared_types.ErrorResponse{Type: "error", Code: code, Message: message, RequestID: requestID})
	if err != nil {
		metrics.RecordDropped(metrics.ReasonInternalErr, metrics.TypeTrade)
		return
	}
	cm.enqueueSocketSend(outboundEnvelope{msg: zmq4.NewMsgFrom(clientID, payload), metricType: metrics.TypeTrade})
}

func currentClientEncoding(cm *ClientManager, clientID string) string {
	encoding, ok := cm.clientEncoding(clientID)
	if !ok {
		return ""
	}
	return encoding
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

func normalizeClientRole(input string) (string, bool) {
	switch strings.ToLower(input) {
	case clientRoleFeed:
		return clientRoleFeed, true
	case clientRoleControl, "control_only":
		return clientRoleControl, true
	default:
		return "", false
	}
}

func (cm *ClientManager) isControlClient(clientID string) bool {
	cm.clientsMu.RLock()
	client, exists := cm.clients[clientID]
	isControl := exists && client != nil && client.Role == clientRoleControl
	cm.clientsMu.RUnlock()
	return isControl
}

func (cm *ClientManager) enqueueSocketSend(envelope outboundEnvelope) bool {
	select {
	case cm.sendChP1 <- envelope:
		return true
	default:
		if envelope.priority == priorityP2 && cm.enqueueP2Latest(envelope) {
			cm.markSlowClient(envelope)
			return true
		}
		metrics.RecordDropped(metrics.ReasonBufferFull, envelope.metricType)
		cm.markSlowClient(envelope)
		if caller, stack, ok := logDropCallsite(envelope.metricType, metrics.ReasonBufferFull); ok {
			log.Printf("DROP_CALLSITE type=%s reason=%s caller=%s stack=%s", envelope.metricType, metrics.ReasonBufferFull, caller, stack)
		}
		return false
	}
}

func (cm *ClientManager) markSlowClient(envelope outboundEnvelope) {
	clientID := envelopeClientID(envelope)
	if clientID == "" {
		return
	}

	now := time.Now()
	var shouldKick bool
	var warnLog string
	var slowDrops int

	cm.clientsMu.Lock()
	client := cm.clients[clientID]
	if client != nil {
		if now.Sub(client.LastQueueDropAt) > slowWarnWindow {
			client.SlowQueueDrops = 0
		}
		client.SlowQueueDrops++
		client.LastQueueDropAt = now
		slowDrops = client.SlowQueueDrops
		role := client.Role
		if role == "" {
			role = clientRoleFeed
		}
		if role == clientRoleControl {
			if slowDrops == 1 {
				warnLog = "[CLIENT-MANAGER-WARN] Client %s verursacht Sendestau; weitere Queue-Ueberlaeufe innerhalb von %s fuehren zum Disconnect."
			} else {
				delete(cm.clients, clientID)
				shouldKick = true
			}
		} else if slowDrops == 1 || slowDrops%25 == 0 {
			warnLog = "[CLIENT-MANAGER-WARN] Feed-Client %s verursacht Sendestau; %d Nachrichten innerhalb von %s wurden verworfen oder zusammengefasst."
		}
	}
	cm.clientsMu.Unlock()

	if warnLog != "" {
		if strings.Contains(warnLog, "%d") {
			log.Printf(warnLog, clientID, slowDrops, slowWarnWindow)
		} else {
			log.Printf(warnLog, clientID, slowWarnWindow)
		}
	}
	if shouldKick {
		log.Printf("[CLIENT-MANAGER] Client %s wegen wiederholtem Sendestau getrennt.", clientID)
		cm.enqueueRequest(&shared_types.ClientRequest{ClientID: []byte(clientID), Action: "disconnect"})
	}
}

func envelopeClientID(envelope outboundEnvelope) string {
	if len(envelope.msg.Frames) == 0 {
		return ""
	}
	return string(envelope.msg.Frames[0])
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

func (cm *ClientManager) distributeStatusEvent(clientIDs [][]byte, event *shared_types.StreamStatusEvent) {
	if event == nil || len(clientIDs) == 0 {
		return
	}
	cm.distributeJSONPayload(clientIDs, event)
}

func (cm *ClientManager) distributeJSONPayload(clientIDs [][]byte, payloadValue any) {
	if payloadValue == nil || len(clientIDs) == 0 {
		return
	}
	payload, err := json.Marshal(payloadValue)
	if err != nil {
		metrics.RecordDropped(metrics.ReasonInternalErr, metrics.TypeTrade)
		return
	}
	for _, clientID := range clientIDs {
		cm.enqueueSocketSend(outboundEnvelope{
			msg:        zmq4.NewMsgFrom(clientID, []byte(HeaderJSON), payload),
			metricType: metrics.TypeTrade,
			priority:   priorityP1,
		})
	}
}

func (cm *ClientManager) dropP2ForClient(clientID string) {
	cm.p2Mu.Lock()
	defer cm.p2Mu.Unlock()
	for key := range cm.p2Latest {
		if key.clientID == clientID {
			delete(cm.p2Latest, key)
		}
	}
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
	defer cm.p2Mu.Unlock()
	return len(cm.sendChP1), cap(cm.sendChP1), len(cm.p2Latest), p2LatestMax
}
