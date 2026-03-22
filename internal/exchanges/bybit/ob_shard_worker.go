package bybit

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/pools"
	"bybit-watcher/internal/shared_types"
	json "github.com/goccy/go-json"
	"github.com/gorilla/websocket"
)

type localOrderbook struct {
	Bids         map[string]shared_types.OrderBookLevel
	Asks         map[string]shared_types.OrderBookLevel
	LastUpdateID int64
}

type OrderBookShardWorker struct {
	wsURL                string
	marketType           string
	commandCh            chan ManagerCommand
	stopCh               <-chan struct{}
	dataCh               chan<- *shared_types.OrderBookUpdate
	statusCh             chan<- *shared_types.StreamStatusEvent
	wg                   *sync.WaitGroup
	mu                   sync.Mutex
	conn                 *websocket.Conn
	orderbooks           map[string]*localOrderbook
	desiredSubscriptions map[string]int
	activeSubscriptions  map[string]int
	topicSymbols         map[string]string
	requestID            atomic.Uint64
}

var (
	bybitOrderBookTopicNeedle = []byte(`"topic":"orderbook.`)
	bybitPongNeedle           = []byte(`"op":"pong"`)
	bybitReadBufPool          = sync.Pool{New: func() any { buf := &bytes.Buffer{}; buf.Grow(16 * 1024); return buf }}
)

type incomingOBMessage struct {
	buf            *bytes.Buffer
	ingestUnixNano int64
}

func NewOrderBookShardWorker(wsURL, marketType string, stopCh <-chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *OrderBookShardWorker {
	return &OrderBookShardWorker{
		wsURL:                wsURL,
		marketType:           marketType,
		commandCh:            make(chan ManagerCommand, 50),
		stopCh:               stopCh,
		dataCh:               dataCh,
		statusCh:             statusCh,
		wg:                   wg,
		orderbooks:           make(map[string]*localOrderbook),
		desiredSubscriptions: make(map[string]int),
		activeSubscriptions:  make(map[string]int),
		topicSymbols:         make(map[string]string),
	}
}

func getBybitDepth(requestedDepth int) int {
	if requestedDepth <= 1 {
		return 1
	}
	if requestedDepth <= 50 {
		return 50
	}
	if requestedDepth <= 200 {
		return 200
	}
	return 1000
}

func (sw *OrderBookShardWorker) Run() {
	defer sw.wg.Done()
	log.Printf("[BYBIT-OB-SHARD] Starte OrderBook Worker fuer %s", sw.marketType)

	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			log.Printf("[BYBIT-OB-SHARD] Worker wird beendet.")
			return
		default:
		}

		if reconnectAttempts > 0 {
			sleepDur := time.Second * time.Duration(reconnectAttempts*2)
			if sleepDur > 30*time.Second {
				sleepDur = 30 * time.Second
			}
			time.Sleep(sleepDur)
		}

		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[BYBIT-OB-SHARD-ERROR] Connect fehlgeschlagen: %v. Versuche in 5s erneut.", err)
			sw.emitStatusForTopics("stream_reconnecting", nil, "connect_failed", reconnectAttempts+1, err.Error())
			reconnectAttempts++
			continue
		}
		sw.conn = conn
		log.Printf("[BYBIT-OB-SHARD] Erfolgreich verbunden mit %s", sw.wsURL)
		if reconnectAttempts > 0 {
			sw.emitStatusForTopics("stream_restored", nil, "", reconnectAttempts, "")
		}
		if err := sw.runSession(context.Background()); err != nil {
			sw.emitStatusForTopics("stream_reconnecting", nil, "read_loop_exit", reconnectAttempts+1, err.Error())
			log.Printf("[BYBIT-OB-SHARD] Verbindung getrennt. Versuche Reconnect... (%v)", err)
		}
		conn.Close()
		if !sw.hasDesiredSubscriptions() {
			log.Printf("[BYBIT-OB-SHARD] Worker wird beendet.")
			return
		}
		reconnectAttempts++
	}
}

func (sw *OrderBookShardWorker) hasDesiredSubscriptions() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.desiredSubscriptions) > 0
}

func (sw *OrderBookShardWorker) desiredTopicsSnapshot() []string {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	topics := make([]string, 0, len(sw.desiredSubscriptions))
	for topic := range sw.desiredSubscriptions {
		topics = append(topics, topic)
	}
	return topics
}

func (sw *OrderBookShardWorker) runSession(ctx context.Context) error {
	msgCh := make(chan incomingOBMessage, 250)
	errCh := make(chan error, 1)
	pingTicker := time.NewTicker(pingEverySec * time.Second)
	maintenanceTicker := time.NewTicker(250 * time.Millisecond)
	defer pingTicker.Stop()
	defer maintenanceTicker.Stop()

	go func() {
		defer close(msgCh)
		for {
			_, buf, err := readWSMessagePooled(sw.conn)
			if err != nil {
				errCh <- err
				return
			}
			if buf == nil {
				continue
			}
			msg := buf.Bytes()
			if bytes.Contains(msg, bybitPongNeedle) {
				recyclePooledBuffer(&bybitReadBufPool, buf)
				continue
			}
			msgCh <- incomingOBMessage{buf: buf, ingestUnixNano: time.Now().UnixNano()}
		}
	}()

	pendingSubs := make([]string, 0, 128)
	pendingUnsubs := make([]string, 0, 128)
	inflight := make(map[string]bybitInflightCommand)

	for _, topic := range sw.desiredTopicsSnapshot() {
		pendingSubs = queueUniqueTopic(pendingSubs, topic)
	}

	flushCommands := func() error {
		chunkSize := bybitCommandChunkSize(sw.marketType)
		if len(pendingSubs) > 0 {
			for _, chunk := range chunkTopics(pendingSubs, chunkSize) {
				reqID, err := sw.sendSubscription("subscribe", chunk)
				if err != nil {
					return err
				}
				inflight[reqID] = bybitInflightCommand{op: "subscribe", topics: chunk, attempt: 1, sentAt: time.Now()}
			}
			pendingSubs = pendingSubs[:0]
		}
		if len(pendingUnsubs) > 0 {
			for _, chunk := range chunkTopics(pendingUnsubs, chunkSize) {
				reqID, err := sw.sendSubscription("unsubscribe", chunk)
				if err != nil {
					return err
				}
				attempt := 1
				for _, topic := range chunk {
					for _, cmd := range inflight {
						if cmd.op != "unsubscribe" {
							continue
						}
						for _, existing := range cmd.topics {
							if existing == topic && cmd.attempt >= attempt {
								attempt = cmd.attempt + 1
							}
						}
					}
				}
				inflight[reqID] = bybitInflightCommand{op: "unsubscribe", topics: chunk, attempt: attempt, sentAt: time.Now()}
			}
			pendingUnsubs = pendingUnsubs[:0]
		}
		return nil
	}

	for {
		select {
		case incoming, ok := <-msgCh:
			if !ok {
				return <-errCh
			}
			msg := incoming.buf.Bytes()
			if !bytes.Contains(msg, bybitOrderBookTopicNeedle) {
				var resp wsCommandResponse
				if err := json.Unmarshal(msg, &resp); err == nil && resp.ReqID != "" {
					recyclePooledBuffer(&bybitReadBufPool, incoming.buf)
					cmd, ok := inflight[resp.ReqID]
					if !ok {
						continue
					}
					delete(inflight, resp.ReqID)
					if !resp.Success {
						if cmd.op == "unsubscribe" && cmd.attempt < 4 {
							log.Printf("[BYBIT-OB-SHARD-ERROR] unsubscribe nack req_id=%s attempt=%d ret_msg=%q retrying", resp.ReqID, cmd.attempt, resp.RetMsg)
							for _, topic := range cmd.topics {
								pendingUnsubs = queueUniqueTopic(pendingUnsubs, topic)
							}
							continue
						}
						sw.emitStatusForTopics("stream_unsubscribe_failed", cmd.topics, "unsubscribe_nack", cmd.attempt, resp.RetMsg)
						sw.emitStatusForTopics("stream_force_closed", cmd.topics, "unsubscribe_nack", cmd.attempt, resp.RetMsg)
						return fmt.Errorf("bybit ob command nack op=%s req_id=%s ret_msg=%q", cmd.op, resp.ReqID, resp.RetMsg)
					}
					sw.mu.Lock()
					switch cmd.op {
					case "subscribe":
						for _, topic := range cmd.topics {
							depth := sw.desiredSubscriptions[topic]
							sw.activeSubscriptions[topic] = depth
							if _, stillDesired := sw.desiredSubscriptions[topic]; !stillDesired {
								pendingUnsubs = queueUniqueTopic(pendingUnsubs, topic)
							}
						}
					case "unsubscribe":
						for _, topic := range cmd.topics {
							delete(sw.activeSubscriptions, topic)
							delete(sw.orderbooks, topic)
							if _, stillDesired := sw.desiredSubscriptions[topic]; !stillDesired {
								delete(sw.topicSymbols, topic)
							}
						}
					}
					sw.mu.Unlock()
					continue
				}
			}
			sw.handleIncomingMessage(msg, incoming.ingestUnixNano)
			recyclePooledBuffer(&bybitReadBufPool, incoming.buf)
		case cmd := <-sw.commandCh:
			depth := getBybitDepth(cmd.Depth)
			topic := fmt.Sprintf("orderbook.%d.%s", depth, cmd.Symbol)
			translated := TranslateSymbolFromExchange(cmd.Symbol, sw.marketType)
			if cmd.Action == "subscribe" {
				sw.mu.Lock()
				sw.desiredSubscriptions[topic] = cmd.Depth
				sw.topicSymbols[topic] = translated
				_, active := sw.activeSubscriptions[topic]
				sw.mu.Unlock()
				if !active {
					pendingSubs = queueUniqueTopic(pendingSubs, topic)
				}
			} else {
				sw.mu.Lock()
				_, hadDesired := sw.desiredSubscriptions[topic]
				_, active := sw.activeSubscriptions[topic]
				delete(sw.desiredSubscriptions, topic)
				delete(sw.orderbooks, topic)
				sw.mu.Unlock()
				if hadDesired || active {
					pendingUnsubs = queueUniqueTopic(pendingUnsubs, topic)
				}
			}
		case <-maintenanceTicker.C:
			if err := flushCommands(); err != nil {
				return err
			}

			now := time.Now()
			for reqID, cmd := range inflight {
				if cmd.op != "unsubscribe" {
					continue
				}
				if now.Sub(cmd.sentAt) < nextBybitRetryDelay(cmd.attempt) {
					continue
				}
				delete(inflight, reqID)
				if cmd.attempt >= 4 {
					sw.emitStatusForTopics("stream_unsubscribe_failed", cmd.topics, "unsubscribe_ack_timeout", cmd.attempt, "")
					sw.emitStatusForTopics("stream_force_closed", cmd.topics, "unsubscribe_ack_timeout", cmd.attempt, "")
					return fmt.Errorf("bybit ob unsubscribe ack timeout for %v after %d attempts", cmd.topics, cmd.attempt)
				}
				log.Printf("[BYBIT-OB-SHARD-ERROR] unsubscribe ack timeout for %v attempt=%d retrying", cmd.topics, cmd.attempt)
				for _, topic := range cmd.topics {
					pendingUnsubs = queueUniqueTopic(pendingUnsubs, topic)
				}
			}
		case <-pingTicker.C:
			if err := sw.conn.WriteJSON(map[string]string{"op": "ping"}); err != nil {
				return err
			}
		case err := <-errCh:
			return err
		case <-ctx.Done():
			return nil
		case <-sw.stopCh:
			return nil
		}
	}
}

func (sw *OrderBookShardWorker) handleIncomingMessage(msg []byte, ingestUnixNano int64) {
	if !bytes.Contains(msg, bybitOrderBookTopicNeedle) {
		return
	}

	var obMsg wsOrderBookMsg
	if err := json.Unmarshal(msg, &obMsg); err != nil {
		metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeOBUpdate)
		return
	}
	if obMsg.Topic == "" {
		return
	}

	goTimestamp := ingestUnixNano / int64(time.Millisecond)
	var updateToSend *shared_types.OrderBookUpdate

	sw.mu.Lock()
	if obMsg.Type == "snapshot" {
		updateToSend = sw.handleSmartSnapshot(obMsg.Topic, obMsg.Data, obMsg.Timestamp, goTimestamp, ingestUnixNano)
	} else if obMsg.Type == "delta" {
		updateToSend = sw.handleDelta(obMsg.Topic, obMsg.Data, obMsg.Timestamp, goTimestamp, ingestUnixNano)
	}
	sw.mu.Unlock()

	if updateToSend != nil {
		sw.dataCh <- updateToSend
	}
}

func (sw *OrderBookShardWorker) handleSmartSnapshot(topic string, data wsOrderBookData, ts int64, goTimestamp int64, ingestUnixNano int64) *shared_types.OrderBookUpdate {
	book, ok := sw.orderbooks[topic]
	if !ok {
		book = &localOrderbook{Bids: make(map[string]shared_types.OrderBookLevel), Asks: make(map[string]shared_types.OrderBookLevel)}
		sw.orderbooks[topic] = book
	}
	book.LastUpdateID = data.UpdateID
	if len(data.Bids) > 0 {
		clear(book.Bids)
		for _, level := range data.Bids {
			if level[0] == "" || level[1] == "" {
				continue
			}
			price, err := strconv.ParseFloat(level[0], 64)
			if err != nil {
				continue
			}
			amount, err := strconv.ParseFloat(level[1], 64)
			if err != nil {
				continue
			}
			book.Bids[level[0]] = shared_types.OrderBookLevel{Price: price, Amount: amount}
		}
	}
	if len(data.Asks) > 0 {
		clear(book.Asks)
		for _, level := range data.Asks {
			if level[0] == "" || level[1] == "" {
				continue
			}
			price, err := strconv.ParseFloat(level[0], 64)
			if err != nil {
				continue
			}
			amount, err := strconv.ParseFloat(level[1], 64)
			if err != nil {
				continue
			}
			book.Asks[level[0]] = shared_types.OrderBookLevel{Price: price, Amount: amount}
		}
	}
	return sw.createUpdate(topic, ts, goTimestamp, ingestUnixNano, metrics.TypeOBSnapshot)
}

func (sw *OrderBookShardWorker) handleDelta(topic string, data wsOrderBookData, ts int64, goTimestamp int64, ingestUnixNano int64) *shared_types.OrderBookUpdate {
	book, ok := sw.orderbooks[topic]
	if !ok {
		return nil
	}
	if data.UpdateID <= book.LastUpdateID {
		metrics.RecordDropped(metrics.ReasonStaleSeq, metrics.TypeOBUpdate)
		return nil
	}

	book.LastUpdateID = data.UpdateID
	for _, level := range data.Bids {
		if level[0] == "" || level[1] == "" {
			continue
		}
		if level[1] == "0" {
			delete(book.Bids, level[0])
		} else {
			amount, err := strconv.ParseFloat(level[1], 64)
			if err != nil {
				continue
			}
			price := 0.0
			if existing, ok := book.Bids[level[0]]; ok {
				price = existing.Price
			} else {
				price, err = strconv.ParseFloat(level[0], 64)
				if err != nil {
					continue
				}
			}
			book.Bids[level[0]] = shared_types.OrderBookLevel{Price: price, Amount: amount}
		}
	}
	for _, level := range data.Asks {
		if level[0] == "" || level[1] == "" {
			continue
		}
		if level[1] == "0" {
			delete(book.Asks, level[0])
		} else {
			amount, err := strconv.ParseFloat(level[1], 64)
			if err != nil {
				continue
			}
			price := 0.0
			if existing, ok := book.Asks[level[0]]; ok {
				price = existing.Price
			} else {
				price, err = strconv.ParseFloat(level[0], 64)
				if err != nil {
					continue
				}
			}
			book.Asks[level[0]] = shared_types.OrderBookLevel{Price: price, Amount: amount}
		}
	}
	return sw.createUpdate(topic, ts, goTimestamp, ingestUnixNano, metrics.TypeOBUpdate)
}

func (sw *OrderBookShardWorker) createUpdate(topic string, ts int64, goTimestamp int64, ingestUnixNano int64, updateType string) *shared_types.OrderBookUpdate {
	book, ok := sw.orderbooks[topic]
	if !ok {
		return nil
	}

	update := pools.GetOrderBookUpdate()
	update.Exchange = "bybit"
	if translated, ok := sw.topicSymbols[topic]; ok {
		update.Symbol = translated
	} else {
		lastDot := strings.LastIndexByte(topic, '.')
		if lastDot < 0 || lastDot+1 >= len(topic) {
			pools.PutOrderBookUpdate(update)
			return nil
		}
		update.Symbol = TranslateSymbolFromExchange(topic[lastDot+1:], sw.marketType)
	}
	update.MarketType = sw.marketType
	update.Timestamp = ts
	update.GoTimestamp = goTimestamp
	update.IngestUnixNano = ingestUnixNano
	update.UpdateType = updateType

	requestedDepth := sw.activeSubscriptions[topic]
	if requestedDepth <= 0 {
		requestedDepth = 20
	}
	if requestedDepth > 20 {
		requestedDepth = 20
	}

	update.Bids = mapToTopLevels(book.Bids, update.Bids, true, requestedDepth)
	update.Asks = mapToTopLevels(book.Asks, update.Asks, false, requestedDepth)
	return update
}

func (sw *OrderBookShardWorker) sendSubscription(op string, topics []string) (string, error) {
	if len(topics) == 0 || sw.conn == nil {
		return "", nil
	}
	if op == "unsubscribe" {
		metrics.RecordUnsubscribeAttempt("bybit", sw.marketType, "orderbook", len(topics))
	}
	reqID := strconv.FormatUint(sw.requestID.Add(1), 10)
	msg := bybitCommandRequest{Op: op, Args: topics, ReqID: reqID}
	if err := sw.conn.WriteJSON(msg); err != nil {
		log.Printf("[BYBIT-OB-SHARD-ERROR] Subscription '%s' fehlgeschlagen: %v", op, err)
		return "", err
	}
	return reqID, nil
}

func (sw *OrderBookShardWorker) emitStatusForTopics(eventType string, topics []string, reason string, attempt int, message string) {
	if sw.statusCh == nil {
		sw.recordStatus(eventType, topics, reason, attempt, message)
		return
	}

	targets := topics
	if len(targets) == 0 {
		sw.mu.Lock()
		targets = make([]string, 0, len(sw.desiredSubscriptions))
		for topic := range sw.desiredSubscriptions {
			targets = append(targets, topic)
		}
		sw.mu.Unlock()
	}

	for _, topic := range targets {
		symbol := ""
		sw.mu.Lock()
		if translated, ok := sw.topicSymbols[topic]; ok {
			symbol = translated
		} else if lastDot := strings.LastIndexByte(topic, '.'); lastDot >= 0 && lastDot+1 < len(topic) {
			symbol = TranslateSymbolFromExchange(topic[lastDot+1:], sw.marketType)
		}
		sw.mu.Unlock()
		if symbol == "" {
			continue
		}
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "bybit",
			MarketType: sw.marketType,
			DataType:   "orderbooks",
			Symbol:     symbol,
			Reason:     reason,
			Attempt:    attempt,
			Message:    message,
			Timestamp:  time.Now().UnixMilli(),
		}
	}
	sw.recordStatus(eventType, targets, reason, attempt, message)
}

func (sw *OrderBookShardWorker) recordStatus(eventType string, topics []string, reason string, attempt int, message string) {
	switch eventType {
	case "stream_reconnecting":
		metrics.RecordStreamReconnect("bybit", sw.marketType, "orderbook", reason)
	case "stream_restored":
		metrics.RecordStreamRestoreSuccess("bybit", sw.marketType, "orderbook")
	case "stream_unsubscribe_failed":
		metrics.RecordUnsubscribeFailure("bybit", sw.marketType, "orderbook", reason)
	case "stream_force_closed":
		metrics.RecordForcedShardRecycle("bybit", sw.marketType, "orderbook", reason)
	}
	metrics.LogStreamLifecycle(eventType, "bybit", fmt.Sprintf("%p", sw), sw.marketType, "orderbook", topics, attempt, reason, message)
}

func readWSMessagePooled(conn *websocket.Conn) (int, *bytes.Buffer, error) {
	msgType, r, err := conn.NextReader()
	if err != nil {
		return 0, nil, err
	}

	buf := bybitReadBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	_, err = buf.ReadFrom(r)
	if err != nil {
		recyclePooledBuffer(&bybitReadBufPool, buf)
		return 0, nil, err
	}

	return msgType, buf, nil
}

func mapToTopLevels(priceMap map[string]shared_types.OrderBookLevel, slice []shared_types.OrderBookLevel, sortDesc bool, limit int) []shared_types.OrderBookLevel {
	if limit <= 0 {
		limit = 20
	}
	if len(priceMap) == 0 {
		if slice != nil {
			return slice[:0]
		}
		return slice
	}
	if cap(slice) < limit {
		slice = make([]shared_types.OrderBookLevel, 0, limit)
	} else {
		slice = slice[:0]
	}
	for _, level := range priceMap {
		price := level.Price
		insertAt := 0
		for insertAt < len(slice) {
			if sortDesc {
				if price > slice[insertAt].Price {
					break
				}
			} else if price < slice[insertAt].Price {
				break
			}
			insertAt++
		}
		if len(slice) < limit {
			slice = append(slice, level)
			if insertAt < len(slice)-1 {
				copy(slice[insertAt+1:], slice[insertAt:len(slice)-1])
				slice[insertAt] = level
			}
		} else if insertAt < limit {
			copy(slice[insertAt+1:], slice[insertAt:len(slice)-1])
			slice[insertAt] = level
		}
	}
	return slice
}
