package mexc

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"bybit-watcher/internal/exchanges/mexc/mexcproto"
	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
	"google.golang.org/protobuf/proto"
)

type OrderBookShardWorker struct {
	wsURL          string
	marketType     string
	commandCh      chan ShardCommand
	stopCh         <-chan struct{}
	dataCh         chan<- *shared_types.OrderBookUpdate
	statusCh       chan<- *shared_types.StreamStatusEvent
	wg             *sync.WaitGroup
	mu             sync.Mutex
	desiredSymbols map[string]int
	activeSymbols  map[string]int
	symbolFreqs    map[string]string
	orderbooks     map[string]*localOrderBookState
}

type localOrderBookState struct {
	Bids            map[string]shared_types.OrderBookLevel
	Asks            map[string]shared_types.OrderBookLevel
	Nonce           int64
	LastToVersion   int64
	Ready           bool
	SnapshotLoading bool
	Pending         []depthDelta
}

type depthDelta struct {
	Asks        []*mexcproto.PublicAggreDepthV3ApiItem
	Bids        []*mexcproto.PublicAggreDepthV3ApiItem
	FromVersion int64
	ToVersion   int64
	Timestamp   int64
	GoTimestamp int64
	IngestNano  int64
	ExchangeSym string
}

type depthSnapshotResponse struct {
	LastUpdateID json.RawMessage `json:"lastUpdateId"`
	Bids         [][]string      `json:"bids"`
	Asks         [][]string      `json:"asks"`
	Timestamp    int64           `json:"timestamp"`
}

var mexcSnapshotHTTPClient = &http.Client{Timeout: 10 * time.Second}
var mexcSnapshotLimiter = newRequestRateLimiter(20, 10*time.Second)
var mexcSnapshotConcurrencyLimiter = make(chan struct{}, 2)

func NewOrderBookShardWorker(wsURL, marketType string, stopCh <-chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *OrderBookShardWorker {
	return &OrderBookShardWorker{
		wsURL:          wsURL,
		marketType:     marketType,
		commandCh:      make(chan ShardCommand, 32),
		stopCh:         stopCh,
		dataCh:         dataCh,
		statusCh:       statusCh,
		wg:             wg,
		desiredSymbols: make(map[string]int),
		activeSymbols:  make(map[string]int),
		symbolFreqs:    make(map[string]string),
		orderbooks:     make(map[string]*localOrderBookState),
	}
}

func (sw *OrderBookShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[MEXC-OB-SHARD] Starte Worker fuer %s", sw.marketType)

	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			log.Printf("[MEXC-OB-SHARD] Worker beendet.")
			return
		default:
		}

		if reconnectAttempts > 0 {
			backoff := time.Duration(math.Pow(2, float64(reconnectAttempts))) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			jitter := time.Duration(rand.Intn(1000)-500) * time.Millisecond
			time.Sleep(backoff + jitter)
		}

		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[MEXC-OB-SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			sw.emitStatusForSymbols("stream_reconnecting", nil, "connect_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}

		sw.resetOrderBooksForReconnect()
		if reconnectAttempts > 0 {
			sw.emitStatusForSymbols("stream_restored", nil, "", reconnectAttempts, "")
		}

		if err := sw.eventLoop(conn); err != nil {
			sw.emitStatusForSymbols("stream_reconnecting", nil, "read_loop_exit", reconnectAttempts+1, err.Error())
			log.Printf("[MEXC-OB-SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredSymbols() {
			log.Printf("[MEXC-OB-SHARD] Worker beendet.")
			return
		}
		reconnectAttempts++
	}
}

func (sw *OrderBookShardWorker) resetOrderBooksForReconnect() {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	for symbol := range sw.activeSymbols {
		delete(sw.activeSymbols, symbol)
	}
	for symbol := range sw.orderbooks {
		sw.orderbooks[symbol] = newLocalOrderBookState()
	}
}

func (sw *OrderBookShardWorker) hasDesiredSymbols() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.desiredSymbols) > 0
}

func (sw *OrderBookShardWorker) desiredSymbolsSnapshot() map[string]int {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	snapshot := make(map[string]int, len(sw.desiredSymbols))
	for symbol, depth := range sw.desiredSymbols {
		snapshot[symbol] = depth
	}
	return snapshot
}

func (sw *OrderBookShardWorker) eventLoop(conn *websocket.Conn) error {
	msgCh := make(chan socketMessage, 256)
	errCh := make(chan error, 1)
	pingTicker := time.NewTicker(pingEverySec * time.Second)
	batchTicker := time.NewTicker(flushEvery * time.Millisecond)
	defer pingTicker.Stop()
	defer batchTicker.Stop()

	go func() {
		for {
			_ = conn.SetReadDeadline(time.Now().Add(readIdleSec * time.Second))
			messageType, payload, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- socketMessage{messageType: messageType, payload: payload}
		}
	}()

	pendingSubs := make([]symbolSubscription, 0, commandBatchSize)
	pendingUnsubs := make([]symbolSubscription, 0, commandBatchSize)
	for symbol, depth := range sw.desiredSymbolsSnapshot() {
		pendingSubs = queueUniqueSymbolSubscription(pendingSubs, symbolSubscription{
			Symbol: symbol,
			Depth:  depth,
			Freq:   sw.symbolFreqs[symbol],
		})
	}

	flushCommands := func() error {
		if len(pendingSubs) > 0 {
			if err := sw.sendDepthCommand(conn, "SUBSCRIPTION", pendingSubs); err != nil {
				return err
			}
			sw.mu.Lock()
			for _, item := range pendingSubs {
				sw.activeSymbols[item.Symbol] = item.Depth
				sw.symbolFreqs[item.Symbol] = normalizeOrderBookFrequency(item.Freq)
				state := sw.orderbooks[item.Symbol]
				if state == nil {
					state = newLocalOrderBookState()
					sw.orderbooks[item.Symbol] = state
				}
				if !state.SnapshotLoading {
					state.SnapshotLoading = true
					go sw.loadSnapshot(item.Symbol)
				}
			}
			sw.mu.Unlock()
			pendingSubs = pendingSubs[:0]
		}
		if len(pendingUnsubs) > 0 {
			if err := sw.sendDepthCommand(conn, "UNSUBSCRIPTION", pendingUnsubs); err != nil {
				return err
			}
			sw.mu.Lock()
			for _, item := range pendingUnsubs {
				delete(sw.activeSymbols, item.Symbol)
				delete(sw.orderbooks, item.Symbol)
				delete(sw.symbolFreqs, item.Symbol)
			}
			sw.mu.Unlock()
			pendingUnsubs = pendingUnsubs[:0]
		}
		return nil
	}

	for {
		select {
		case msg := <-msgCh:
			switch msg.messageType {
			case websocket.BinaryMessage:
				if err := sw.handleBinaryOrderBookMessage(msg.payload); err != nil {
					metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeOBUpdate)
					log.Printf("[MEXC-OB-SHARD-WARN] Proto-Decode fehlgeschlagen: %v", err)
				}
			case websocket.TextMessage:
				if sw.handleTextMessage(msg.payload) {
					continue
				}
			}
		case cmd := <-sw.commandCh:
			sw.mu.Lock()
			for _, symbol := range cmd.Symbols {
				if cmd.Action == "subscribe" {
					depth := normalizeOrderBookDepth(cmd.Depth)
					freq := normalizeOrderBookFrequency(cmd.Freq)
					if existing, ok := sw.desiredSymbols[symbol]; ok && existing == depth && sw.symbolFreqs[symbol] == freq {
						continue
					}
					sw.desiredSymbols[symbol] = depth
					sw.symbolFreqs[symbol] = freq
					if _, ok := sw.orderbooks[symbol]; !ok {
						sw.orderbooks[symbol] = newLocalOrderBookState()
					}
					pendingSubs = queueUniqueSymbolSubscription(pendingSubs, symbolSubscription{Symbol: symbol, Depth: depth, Freq: freq})
				} else {
					delete(sw.desiredSymbols, symbol)
					if _, ok := sw.activeSymbols[symbol]; ok {
						pendingUnsubs = queueUniqueSymbolSubscription(pendingUnsubs, symbolSubscription{Symbol: symbol, Depth: cmd.Depth, Freq: sw.symbolFreqs[symbol]})
					} else {
						delete(sw.orderbooks, symbol)
						delete(sw.symbolFreqs, symbol)
					}
				}
			}
			sw.mu.Unlock()
		case <-batchTicker.C:
			if err := flushCommands(); err != nil {
				return err
			}
		case <-pingTicker.C:
			if err := conn.WriteJSON(map[string]string{"method": "ping"}); err != nil {
				return err
			}
		case err := <-errCh:
			return err
		case <-sw.stopCh:
			return nil
		}
	}
}

func (sw *OrderBookShardWorker) handleBinaryOrderBookMessage(payload []byte) error {
	var wrapper mexcproto.PushDataV3ApiWrapper
	if err := proto.Unmarshal(payload, &wrapper); err != nil {
		return err
	}

	depths := wrapper.GetPublicAggreDepths()
	if depths == nil {
		return nil
	}

	symbol := wrapper.GetSymbol()
	if symbol == "" {
		return fmt.Errorf("missing symbol in protobuf payload")
	}

	fromVersion, err := strconv.ParseInt(depths.GetFromVersion(), 10, 64)
	if err != nil {
		return fmt.Errorf("parse fromVersion: %w", err)
	}
	toVersion := fromVersion
	if depths.GetToVersion() != "" {
		toVersion, err = strconv.ParseInt(depths.GetToVersion(), 10, 64)
		if err != nil {
			return fmt.Errorf("parse toVersion: %w", err)
		}
	}

	ingestNow := time.Now()
	delta := depthDelta{
		Asks:        depths.GetAsks(),
		Bids:        depths.GetBids(),
		FromVersion: fromVersion,
		ToVersion:   toVersion,
		Timestamp:   wrapper.GetSendTime(),
		GoTimestamp: ingestNow.UnixMilli(),
		IngestNano:  ingestNow.UnixNano(),
		ExchangeSym: symbol,
	}

	update, needReload := sw.applyDelta(symbol, delta)
	if needReload {
		go sw.loadSnapshot(symbol)
		return nil
	}
	if update != nil {
		sw.dataCh <- update
	}
	return nil
}

func (sw *OrderBookShardWorker) applyDelta(symbol string, delta depthDelta) (*shared_types.OrderBookUpdate, bool) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	state := sw.orderbooks[symbol]
	if state == nil {
		state = newLocalOrderBookState()
		sw.orderbooks[symbol] = state
	}

	if !state.Ready {
		state.Pending = append(state.Pending, delta)
		if len(state.Pending) > 200 {
			state.Pending = state.Pending[len(state.Pending)-200:]
		}
		if !state.SnapshotLoading {
			state.SnapshotLoading = true
			go sw.loadSnapshot(symbol)
		}
		return nil, false
	}

	if isStaleDelta(state, delta) {
		return nil, false
	}
	if hasDeltaGap(state, delta) {
		state.Ready = false
		state.Pending = append(state.Pending[:0], delta)
		state.SnapshotLoading = true
		return nil, true
	}

	if err := applyDeltaToState(state, delta); err != nil {
		state.Ready = false
		state.Pending = append(state.Pending[:0], delta)
		state.SnapshotLoading = true
		return nil, true
	}

	return buildOrderBookUpdate(symbol, sw.marketType, sw.desiredSymbols[symbol], state, delta.Timestamp, delta.GoTimestamp, delta.IngestNano), false
}

func (sw *OrderBookShardWorker) handleTextMessage(payload []byte) bool {
	if string(payload) == "PONG" {
		return true
	}

	var msg wsStatusMessage
	if err := json.Unmarshal(payload, &msg); err == nil {
		if msg.Msg == "PONG" {
			return true
		}
	}

	return false
}

func (sw *OrderBookShardWorker) sendDepthCommand(conn *websocket.Conn, method string, symbols []symbolSubscription) error {
	channels := make([]string, 0, len(symbols))
	for _, symbol := range symbols {
		channels = append(channels, aggreOrderBookChannel(symbol.Symbol, symbol.Freq))
	}
	for start := 0; start < len(channels); start += commandBatchSize {
		end := start + commandBatchSize
		if end > len(channels) {
			end = len(channels)
		}
		req := wsCommand{Method: method, Params: channels[start:end]}
		if err := conn.WriteJSON(req); err != nil {
			return err
		}
	}
	return nil
}

type symbolSubscription struct {
	Symbol string
	Depth  int
	Freq   string
}

func queueUniqueSymbolSubscription(queue []symbolSubscription, item symbolSubscription) []symbolSubscription {
	for i, existing := range queue {
		if existing.Symbol != item.Symbol {
			continue
		}
		queue[i] = item
		return queue
	}
	return append(queue, item)
}

func (sw *OrderBookShardWorker) loadSnapshot(symbol string) {
	snapshot, err := fetchDepthSnapshot(symbol)
	if err != nil {
		log.Printf("[MEXC-OB-SNAPSHOT-WARN] Snapshot fehlgeschlagen fuer %s: %v", symbol, err)
		sw.mu.Lock()
		if state := sw.orderbooks[symbol]; state != nil {
			state.SnapshotLoading = false
		}
		sw.mu.Unlock()
		return
	}

	sw.mu.Lock()
	defer sw.mu.Unlock()

	state := sw.orderbooks[symbol]
	if state == nil {
		return
	}

	state.Bids = snapshot.Bids
	state.Asks = snapshot.Asks
	state.Nonce = snapshot.LastUpdateID
	state.LastToVersion = snapshot.LastUpdateID

	cacheIndex := findCacheStartIndex(snapshot.LastUpdateID, state.Pending)
	if cacheIndex < 0 {
		state.Pending = nil
		state.Ready = false
		state.SnapshotLoading = true
		go sw.loadSnapshot(symbol)
		return
	}

	for _, delta := range state.Pending[cacheIndex:] {
		if isStaleDelta(state, delta) {
			continue
		}
		if hasDeltaGap(state, delta) {
			state.Pending = nil
			state.Ready = false
			state.SnapshotLoading = true
			go sw.loadSnapshot(symbol)
			return
		}
		if err := applyDeltaToState(state, delta); err != nil {
			state.Pending = nil
			state.Ready = false
			state.SnapshotLoading = true
			go sw.loadSnapshot(symbol)
			return
		}
	}

	state.Pending = nil
	state.Ready = true
	state.SnapshotLoading = false
}

func (sw *OrderBookShardWorker) emitStatusForSymbols(eventType string, symbols []string, reason string, attempt int, message string) {
	if sw.statusCh == nil {
		return
	}
	now := time.Now().UnixMilli()
	for _, symbol := range symbols {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "mexc",
			MarketType: sw.marketType,
			DataType:   "orderbooks",
			Symbol:     TranslateSymbolFromExchange(symbol),
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  now,
		}
	}
	if len(symbols) == 0 {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "mexc",
			MarketType: sw.marketType,
			DataType:   "orderbooks",
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  now,
		}
	}
}

func newLocalOrderBookState() *localOrderBookState {
	return &localOrderBookState{
		Bids:    make(map[string]shared_types.OrderBookLevel),
		Asks:    make(map[string]shared_types.OrderBookLevel),
		Pending: make([]depthDelta, 0, 32),
	}
}

func fetchDepthSnapshot(symbol string) (*depthSnapshot, error) {
	mexcSnapshotConcurrencyLimiter <- struct{}{}
	defer func() {
		<-mexcSnapshotConcurrencyLimiter
	}()

	time.Sleep(time.Duration(100+rand.Intn(200)) * time.Millisecond)
	mexcSnapshotLimiter.Wait()

	u, err := url.Parse(mexcDepthSnapshotURL)
	if err != nil {
		return nil, err
	}
	query := u.Query()
	query.Set("symbol", symbol)
	query.Set("limit", strconv.Itoa(mexcDepthSnapshotLimit))
	u.RawQuery = query.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", "zmq_manager/1.0")
	resp, err := mexcSnapshotHTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		retryAfter := parseRetryAfter(resp.Header.Get("Retry-After"))
		if retryAfter > 0 {
			time.Sleep(retryAfter)
		}
		return nil, fmt.Errorf("rate limited by mexc snapshot endpoint")
	}
	if resp.StatusCode == http.StatusForbidden {
		time.Sleep(2 * time.Second)
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if len(body) > 0 {
			return nil, fmt.Errorf("snapshot blocked by mexc edge (403) body=%s", string(body))
		}
		return nil, fmt.Errorf("snapshot blocked by mexc edge (403)")
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		if len(body) > 0 {
			return nil, fmt.Errorf("unexpected status %d body=%s", resp.StatusCode, string(body))
		}
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var payload depthSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, err
	}
	lastUpdateID, err := parseFlexibleInt64(payload.LastUpdateID)
	if err != nil {
		return nil, fmt.Errorf("parse lastUpdateId: %w", err)
	}

	snapshot := &depthSnapshot{
		Bids:         make(map[string]shared_types.OrderBookLevel, len(payload.Bids)),
		Asks:         make(map[string]shared_types.OrderBookLevel, len(payload.Asks)),
		LastUpdateID: lastUpdateID,
		Timestamp:    payload.Timestamp,
	}
	for _, level := range payload.Bids {
		if len(level) < 2 {
			continue
		}
		price, err := strconv.ParseFloat(level[0], 64)
		if err != nil {
			return nil, err
		}
		amount, err := strconv.ParseFloat(level[1], 64)
		if err != nil {
			return nil, err
		}
		snapshot.Bids[level[0]] = shared_types.OrderBookLevel{Price: price, Amount: amount}
	}
	for _, level := range payload.Asks {
		if len(level) < 2 {
			continue
		}
		price, err := strconv.ParseFloat(level[0], 64)
		if err != nil {
			return nil, err
		}
		amount, err := strconv.ParseFloat(level[1], 64)
		if err != nil {
			return nil, err
		}
		snapshot.Asks[level[0]] = shared_types.OrderBookLevel{Price: price, Amount: amount}
	}
	return snapshot, nil
}

type requestRateLimiter struct {
	tokens chan struct{}
	stopCh chan struct{}
}

func newRequestRateLimiter(limit int, window time.Duration) *requestRateLimiter {
	if limit <= 0 {
		limit = 1
	}
	interval := window / time.Duration(limit)
	if interval <= 0 {
		interval = time.Millisecond
	}

	rl := &requestRateLimiter{
		tokens: make(chan struct{}, limit),
		stopCh: make(chan struct{}),
	}
	for i := 0; i < limit; i++ {
		rl.tokens <- struct{}{}
	}

	ticker := time.NewTicker(interval)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				select {
				case rl.tokens <- struct{}{}:
				default:
				}
			case <-rl.stopCh:
				return
			}
		}
	}()
	return rl
}

func (rl *requestRateLimiter) Wait() {
	<-rl.tokens
}

func parseRetryAfter(value string) time.Duration {
	if value == "" {
		return 0
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(seconds) * time.Second
}

func parseFlexibleInt64(raw json.RawMessage) (int64, error) {
	if len(raw) == 0 {
		return 0, fmt.Errorf("empty value")
	}

	var asInt int64
	if err := json.Unmarshal(raw, &asInt); err == nil {
		return asInt, nil
	}

	var asString string
	if err := json.Unmarshal(raw, &asString); err == nil {
		return strconv.ParseInt(asString, 10, 64)
	}

	return 0, fmt.Errorf("unsupported int format: %s", string(raw))
}

type depthSnapshot struct {
	Bids         map[string]shared_types.OrderBookLevel
	Asks         map[string]shared_types.OrderBookLevel
	LastUpdateID int64
	Timestamp    int64
}

func findCacheStartIndex(snapshotNonce int64, pending []depthDelta) int {
	if len(pending) == 0 {
		return 0
	}
	firstNonce := pending[0].FromVersion
	if snapshotNonce < firstNonce-1 {
		return -1
	}
	for i, delta := range pending {
		if delta.FromVersion >= snapshotNonce {
			return i
		}
	}
	return len(pending)
}

func isStaleDelta(state *localOrderBookState, delta depthDelta) bool {
	return delta.FromVersion < state.Nonce
}

func hasDeltaGap(state *localOrderBookState, delta depthDelta) bool {
	if state.LastToVersion == 0 {
		return delta.FromVersion > state.Nonce+1
	}
	return delta.FromVersion > state.LastToVersion+1
}

func applyDeltaToState(state *localOrderBookState, delta depthDelta) error {
	if err := applyAggreLevels(state.Asks, delta.Asks); err != nil {
		return err
	}
	if err := applyAggreLevels(state.Bids, delta.Bids); err != nil {
		return err
	}
	state.Nonce = delta.FromVersion
	state.LastToVersion = delta.ToVersion
	return nil
}

func applyAggreLevels(side map[string]shared_types.OrderBookLevel, levels []*mexcproto.PublicAggreDepthV3ApiItem) error {
	for _, level := range levels {
		if level == nil {
			continue
		}
		if level.GetQuantity() == "0" {
			delete(side, level.GetPrice())
			continue
		}
		price, err := strconv.ParseFloat(level.GetPrice(), 64)
		if err != nil {
			return fmt.Errorf("parse price: %w", err)
		}
		amount, err := strconv.ParseFloat(level.GetQuantity(), 64)
		if err != nil {
			return fmt.Errorf("parse quantity: %w", err)
		}
		side[level.GetPrice()] = shared_types.OrderBookLevel{Price: price, Amount: amount}
	}
	return nil
}

func buildOrderBookUpdate(symbol string, marketType string, depth int, state *localOrderBookState, exchangeTimestamp int64, goTimestamp int64, ingestUnixNano int64) *shared_types.OrderBookUpdate {
	if exchangeTimestamp == 0 {
		exchangeTimestamp = goTimestamp
	}
	if depth <= 0 {
		depth = validOrderBookDepths[0]
	}

	update := pools.GetOrderBookUpdate()
	update.Exchange = "mexc"
	update.Symbol = TranslateSymbolFromExchange(symbol)
	update.MarketType = marketType
	update.Timestamp = exchangeTimestamp
	update.GoTimestamp = goTimestamp
	update.IngestUnixNano = ingestUnixNano
	update.UpdateType = metrics.TypeOBUpdate
	update.DataType = "orderbooks"
	update.Bids = topLevelsFromMap(state.Bids, update.Bids[:0], true, depth)
	update.Asks = topLevelsFromMap(state.Asks, update.Asks[:0], false, depth)
	return update
}

func topLevelsFromMap(src map[string]shared_types.OrderBookLevel, dst []shared_types.OrderBookLevel, desc bool, limit int) []shared_types.OrderBookLevel {
	if len(src) == 0 {
		return dst[:0]
	}
	if cap(dst) < limit {
		dst = make([]shared_types.OrderBookLevel, 0, limit)
	} else {
		dst = dst[:0]
	}

	levels := make([]shared_types.OrderBookLevel, 0, len(src))
	for _, level := range src {
		levels = append(levels, level)
	}
	sort.Slice(levels, func(i, j int) bool {
		if desc {
			return levels[i].Price > levels[j].Price
		}
		return levels[i].Price < levels[j].Price
	})
	if len(levels) > limit {
		levels = levels[:limit]
	}
	return append(dst, levels...)
}
