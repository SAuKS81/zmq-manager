package htx

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
)

type localOrderBookState struct {
	Bids  map[string]shared_types.OrderBookLevel
	Asks  map[string]shared_types.OrderBookLevel
	Nonce int64
	Cache []wsEnvelope
	Ready bool
}

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
	orderbooks     map[string]*localOrderBookState
	requestSeq     int64
}

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
		orderbooks:     make(map[string]*localOrderBookState),
	}
}

func (sw *OrderBookShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[HTX-OB-SHARD] Starte Worker fuer %s", sw.marketType)
	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			log.Printf("[HTX-OB-SHARD] Worker beendet.")
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
			log.Printf("[HTX-OB-SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			sw.emitStatusForSymbols("stream_reconnecting", nil, "connect_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}
		sw.resetForReconnect()
		if reconnectAttempts > 0 {
			sw.emitStatusForSymbols("stream_restored", nil, "", reconnectAttempts, "")
		}
		if err := sw.eventLoop(conn); err != nil {
			sw.emitStatusForSymbols("stream_reconnecting", nil, "read_loop_exit", reconnectAttempts+1, err.Error())
			log.Printf("[HTX-OB-SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredSymbols() {
			log.Printf("[HTX-OB-SHARD] Worker beendet.")
			return
		}
		reconnectAttempts++
	}
}

func (sw *OrderBookShardWorker) resetForReconnect() {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.activeSymbols = make(map[string]int)
	for symbol := range sw.orderbooks {
		sw.orderbooks[symbol] = &localOrderBookState{
			Bids: make(map[string]shared_types.OrderBookLevel),
			Asks: make(map[string]shared_types.OrderBookLevel),
		}
	}
}

func (sw *OrderBookShardWorker) hasDesiredSymbols() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.desiredSymbols) > 0
}

func (sw *OrderBookShardWorker) desiredSnapshot() map[string]int {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	out := make(map[string]int, len(sw.desiredSymbols))
	for s, d := range sw.desiredSymbols {
		out[s] = d
	}
	return out
}

func (sw *OrderBookShardWorker) nextRequestID() string {
	sw.requestSeq++
	return fmt.Sprintf("%d", sw.requestSeq)
}

func (sw *OrderBookShardWorker) eventLoop(conn *websocket.Conn) error {
	msgCh := make(chan socketMessage, 256)
	errCh := make(chan error, 1)
	batchTicker := time.NewTicker(flushEveryMS * time.Millisecond)
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

	pendingSubs := make([]symbolDepth, 0, commandBatchSize)
	pendingUnsubs := make([]symbolDepth, 0, commandBatchSize)
	for symbol, depth := range sw.desiredSnapshot() {
		pendingSubs = queueUniqueSymbolDepth(pendingSubs, symbolDepth{Symbol: symbol, Depth: depth})
	}

	flushCommands := func() error {
		for _, item := range pendingSubs {
			if err := sw.sendSubscribe(conn, item.Symbol, item.Depth); err != nil {
				return err
			}
			sw.mu.Lock()
			sw.activeSymbols[item.Symbol] = item.Depth
			if sw.orderbooks[item.Symbol] == nil {
				sw.orderbooks[item.Symbol] = &localOrderBookState{
					Bids: make(map[string]shared_types.OrderBookLevel),
					Asks: make(map[string]shared_types.OrderBookLevel),
				}
			}
			sw.mu.Unlock()
			if sw.marketType == "spot" {
				if err := sw.sendSnapshotReq(conn, item.Symbol, item.Depth); err != nil {
					return err
				}
			}
		}
		pendingSubs = pendingSubs[:0]
		for _, item := range pendingUnsubs {
			if err := sw.sendUnsubscribe(conn, item.Symbol, item.Depth); err != nil {
				return err
			}
			sw.mu.Lock()
			delete(sw.activeSymbols, item.Symbol)
			delete(sw.orderbooks, item.Symbol)
			sw.mu.Unlock()
		}
		pendingUnsubs = pendingUnsubs[:0]
		return nil
	}

	for {
		select {
		case msg := <-msgCh:
			var payload []byte
			switch msg.messageType {
			case websocket.TextMessage:
				payload = msg.payload
			case websocket.BinaryMessage:
				decoded, err := gunzipOrderBookPayload(msg.payload)
				if err != nil {
					return fmt.Errorf("gunzip ws payload: %w", err)
				}
				payload = decoded
			default:
				continue
			}
			if err := sw.handleMessage(conn, payload); err != nil {
				return err
			}
		case cmd := <-sw.commandCh:
			sw.mu.Lock()
			for _, symbol := range cmd.Symbols {
				depth := normalizeOrderBookDepth(cmd.Depth)
				if cmd.Action == "subscribe" {
					if sw.desiredSymbols[symbol] == depth && sw.activeSymbols[symbol] == depth {
						continue
					}
					sw.desiredSymbols[symbol] = depth
					pendingSubs = queueUniqueSymbolDepth(pendingSubs, symbolDepth{Symbol: symbol, Depth: depth})
				} else {
					delete(sw.desiredSymbols, symbol)
					if activeDepth, ok := sw.activeSymbols[symbol]; ok {
						pendingUnsubs = queueUniqueSymbolDepth(pendingUnsubs, symbolDepth{Symbol: symbol, Depth: activeDepth})
					}
				}
			}
			sw.mu.Unlock()
		case <-batchTicker.C:
			if err := flushCommands(); err != nil {
				return err
			}
		case err := <-errCh:
			return err
		case <-sw.stopCh:
			return nil
		}
	}
}

type symbolDepth struct {
	Symbol string
	Depth  int
}

func queueUniqueSymbolDepth(queue []symbolDepth, item symbolDepth) []symbolDepth {
	for i, existing := range queue {
		if existing.Symbol == item.Symbol {
			queue[i] = item
			return queue
		}
	}
	return append(queue, item)
}

func (sw *OrderBookShardWorker) handleMessage(conn *websocket.Conn, payload []byte) error {
	var envelope wsEnvelope
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return fmt.Errorf("decode ws payload: %w", err)
	}
	if envelope.Ping != 0 {
		return conn.WriteJSON(pongMessage{Pong: envelope.Ping})
	}
	if envelope.Status == "error" {
		return fmt.Errorf("htx ws error: %s", string(payload))
	}
	if envelope.Tick == nil && envelope.Data != nil {
		envelope.Tick = envelope.Data
	}
	if envelope.Ch == "" && envelope.Rep != "" {
		envelope.Ch = envelope.Rep
	}
	if envelope.Ch == "" || envelope.Tick == nil {
		return nil
	}

	exchangeSymbol, err := symbolFromChannel(envelope.Ch)
	if err != nil {
		return nil
	}
	state := sw.ensureOrderBookState(exchangeSymbol)
	if sw.marketType == "spot" {
		if envelope.Rep != "" {
			return sw.applySpotSnapshot(conn, exchangeSymbol, state, envelope)
		}
		if !state.Ready && envelope.Tick.PrevSeqNum != 0 {
			state.Cache = append(state.Cache, envelope)
			return nil
		}
		if strings.HasPrefix(envelope.Ch, "market.") && strings.Contains(envelope.Ch, ".mbp.") {
			return sw.applySpotOrderBook(conn, exchangeSymbol, state, envelope)
		}
		return nil
	}
	return sw.applySwapOrderBook(exchangeSymbol, state, envelope)
}

func (sw *OrderBookShardWorker) ensureOrderBookState(exchangeSymbol string) *localOrderBookState {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	state := sw.orderbooks[exchangeSymbol]
	if state == nil {
		state = &localOrderBookState{
			Bids: make(map[string]shared_types.OrderBookLevel),
			Asks: make(map[string]shared_types.OrderBookLevel),
		}
		sw.orderbooks[exchangeSymbol] = state
	}
	return state
}

func (sw *OrderBookShardWorker) applySpotSnapshot(conn *websocket.Conn, exchangeSymbol string, state *localOrderBookState, envelope wsEnvelope) error {
	tick := envelope.Tick
	if tick == nil {
		return nil
	}
	firstCachedPrev := int64(0)
	if len(state.Cache) > 0 && state.Cache[0].Tick != nil {
		firstCachedPrev = state.Cache[0].Tick.PrevSeqNum
	}
	if firstCachedPrev != 0 && tick.SeqNum < firstCachedPrev {
		log.Printf("[HTX-OB-SHARD-INFO] Snapshot noch vor erstem Delta fuer %s (snapshot=%d first_prev=%d), fordere Snapshot erneut an", exchangeSymbol, tick.SeqNum, firstCachedPrev)
		return sw.requestSpotSnapshot(conn, exchangeSymbol)
	}
	state.Bids = make(map[string]shared_types.OrderBookLevel)
	state.Asks = make(map[string]shared_types.OrderBookLevel)
	applyLevels(state.Bids, tick.Bids)
	applyLevels(state.Asks, tick.Asks)
	state.Nonce = tick.SeqNum
	state.Ready = true
	update, err := buildOrderBookUpdateFromState(exchangeSymbol, sw.marketType, coalesceTimestamp(tick.Ts, envelope.Ts), state, metrics.TypeOBSnapshot, time.Now().UnixMilli(), time.Now().UnixNano())
	if err != nil {
		return err
	}
	sw.dataCh <- update
	if len(state.Cache) > 0 {
		cached := state.Cache
		state.Cache = nil
		for _, cachedEnvelope := range cached {
			if err := sw.applySpotOrderBook(conn, exchangeSymbol, state, cachedEnvelope); err != nil {
				return err
			}
		}
	}
	return nil
}

func (sw *OrderBookShardWorker) applySpotOrderBook(conn *websocket.Conn, exchangeSymbol string, state *localOrderBookState, envelope wsEnvelope) error {
	tick := envelope.Tick
	if tick.SeqNum == 0 && envelope.Status == "ok" {
		return nil
	}
	if tick.PrevSeqNum != 0 && state.Nonce != 0 && tick.PrevSeqNum > state.Nonce {
		log.Printf("[HTX-OB-SHARD-INFO] Spot-Seq-Gap fuer %s prev=%d nonce=%d, starte Snapshot-Resync", exchangeSymbol, tick.PrevSeqNum, state.Nonce)
		state.Bids = make(map[string]shared_types.OrderBookLevel)
		state.Asks = make(map[string]shared_types.OrderBookLevel)
		state.Nonce = 0
		state.Ready = false
		state.Cache = append(state.Cache, envelope)
		return sw.requestSpotSnapshot(conn, exchangeSymbol)
	}
	if tick.PrevSeqNum != 0 && state.Nonce != 0 && tick.PrevSeqNum != state.Nonce {
		return nil
	}
	applyLevels(state.Bids, tick.Bids)
	applyLevels(state.Asks, tick.Asks)
	state.Nonce = tick.SeqNum
	state.Ready = true
	update, err := buildOrderBookUpdateFromState(exchangeSymbol, sw.marketType, tick.Ts, state, metrics.TypeOBUpdate, time.Now().UnixMilli(), time.Now().UnixNano())
	if err != nil {
		return err
	}
	sw.dataCh <- update
	return nil
}

func (sw *OrderBookShardWorker) applySwapOrderBook(exchangeSymbol string, state *localOrderBookState, envelope wsEnvelope) error {
	tick := envelope.Tick
	if tick.Event == "snapshot" || state.Nonce == 0 {
		state.Bids = make(map[string]shared_types.OrderBookLevel)
		state.Asks = make(map[string]shared_types.OrderBookLevel)
	}
	if tick.Event == "update" && state.Nonce != 0 && tick.Version-1 != state.Nonce {
		return fmt.Errorf("htx swap orderbook version gap symbol=%s version=%d nonce=%d", exchangeSymbol, tick.Version, state.Nonce)
	}
	applyLevels(state.Bids, tick.Bids)
	applyLevels(state.Asks, tick.Asks)
	state.Nonce = tick.Version
	updateType := metrics.TypeOBUpdate
	if tick.Event == "snapshot" {
		updateType = metrics.TypeOBSnapshot
	}
	update, err := buildOrderBookUpdateFromState(exchangeSymbol, sw.marketType, tick.Ts, state, updateType, time.Now().UnixMilli(), time.Now().UnixNano())
	if err != nil {
		return err
	}
	sw.dataCh <- update
	return nil
}

func applyLevels(side map[string]shared_types.OrderBookLevel, levels [][]json.Number) {
	for _, level := range levels {
		if len(level) < 2 {
			continue
		}
		price := level[0].String()
		priceF, err1 := parseJSONNumberFloat(level[0])
		amountF, err2 := parseJSONNumberFloat(level[1])
		if err1 != nil || err2 != nil {
			continue
		}
		if amountF == 0 {
			delete(side, price)
			continue
		}
		side[price] = shared_types.OrderBookLevel{Price: priceF, Amount: amountF}
	}
}

func buildOrderBookUpdateFromState(exchangeSymbol, marketType string, ts int64, state *localOrderBookState, updateType string, goTimestamp int64, ingestUnixNano int64) (*shared_types.OrderBookUpdate, error) {
	bids := mapToSortedLevels(state.Bids, true)
	asks := mapToSortedLevels(state.Asks, false)
	return &shared_types.OrderBookUpdate{
		Exchange:       "htx",
		Symbol:         TranslateSymbolFromExchange(exchangeSymbol, marketType),
		MarketType:     marketType,
		Timestamp:      ts,
		GoTimestamp:    goTimestamp,
		IngestUnixNano: ingestUnixNano,
		UpdateType:     updateType,
		Bids:           bids,
		Asks:           asks,
		DataType:       "orderbooks",
	}, nil
}

func mapToSortedLevels(side map[string]shared_types.OrderBookLevel, desc bool) []shared_types.OrderBookLevel {
	levels := make([]shared_types.OrderBookLevel, 0, len(side))
	for _, level := range side {
		levels = append(levels, level)
	}
	sort.Slice(levels, func(i, j int) bool {
		if desc {
			return levels[i].Price > levels[j].Price
		}
		return levels[i].Price < levels[j].Price
	})
	return levels
}

func (sw *OrderBookShardWorker) sendSubscribe(conn *websocket.Conn, exchangeSymbol string, depth int) error {
	cmd := spotCommand{Sub: orderBookChannel(TranslateSymbolFromExchange(exchangeSymbol, sw.marketType), depth, sw.marketType), ID: sw.nextRequestID()}
	if sw.marketType == "swap" {
		cmd.DataType = "incremental"
	}
	return conn.WriteJSON(cmd)
}

func (sw *OrderBookShardWorker) sendSnapshotReq(conn *websocket.Conn, exchangeSymbol string, depth int) error {
	cmd := spotCommand{
		Req: orderBookChannel(TranslateSymbolFromExchange(exchangeSymbol, sw.marketType), depth, sw.marketType),
		ID:  sw.nextRequestID(),
	}
	return conn.WriteJSON(cmd)
}

func (sw *OrderBookShardWorker) requestSpotSnapshot(conn *websocket.Conn, exchangeSymbol string) error {
	sw.mu.Lock()
	depth := sw.activeSymbols[exchangeSymbol]
	if depth == 0 {
		depth = sw.desiredSymbols[exchangeSymbol]
	}
	sw.mu.Unlock()
	if depth == 0 {
		depth = 150
	}
	return sw.sendSnapshotReq(conn, exchangeSymbol, depth)
}

func (sw *OrderBookShardWorker) sendUnsubscribe(conn *websocket.Conn, exchangeSymbol string, depth int) error {
	cmd := spotCommand{Unsub: orderBookChannel(TranslateSymbolFromExchange(exchangeSymbol, sw.marketType), depth, sw.marketType), ID: sw.nextRequestID()}
	if sw.marketType == "swap" {
		cmd.DataType = "incremental"
	}
	return conn.WriteJSON(cmd)
}

func (sw *OrderBookShardWorker) emitStatusForSymbols(eventType string, symbols []string, reason string, attempt int, message string) {
	if sw.statusCh == nil {
		return
	}
	now := time.Now().UnixMilli()
	for _, symbol := range symbols {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "htx",
			MarketType: sw.marketType,
			DataType:   "orderbooks",
			Symbol:     TranslateSymbolFromExchange(symbol, sw.marketType),
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  now,
		}
	}
	if len(symbols) == 0 {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "htx",
			MarketType: sw.marketType,
			DataType:   "orderbooks",
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  now,
		}
	}
}

func gunzipOrderBookPayload(payload []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func coalesceTimestamp(primary int64, fallback int64) int64 {
	if primary != 0 {
		return primary
	}
	return fallback
}
