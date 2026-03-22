package bitmart

import (
	"encoding/json"
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
	Bids map[string]shared_types.OrderBookLevel
	Asks map[string]shared_types.OrderBookLevel
}

type OrderBookShardWorker struct {
	commandCh      chan ShardCommand
	stopCh         <-chan struct{}
	dataCh         chan<- *shared_types.OrderBookUpdate
	statusCh       chan<- *shared_types.StreamStatusEvent
	wg             *sync.WaitGroup
	mu             sync.Mutex
	desiredSymbols map[string]int
	desiredModes   map[string]string
	orderbooks     map[string]*localOrderBookState
}

func NewOrderBookShardWorker(stopCh <-chan struct{}, dataCh chan<- *shared_types.OrderBookUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *OrderBookShardWorker {
	return &OrderBookShardWorker{
		commandCh:      make(chan ShardCommand, 32),
		stopCh:         stopCh,
		dataCh:         dataCh,
		statusCh:       statusCh,
		wg:             wg,
		desiredSymbols: make(map[string]int),
		desiredModes:   make(map[string]string),
		orderbooks:     make(map[string]*localOrderBookState),
	}
}

func (sw *OrderBookShardWorker) Run() {
	defer sw.wg.Done()
	var reconnectAttempts int
	for {
		select {
		case <-sw.stopCh:
			return
		default:
		}
		if reconnectAttempts > 0 {
			backoff := time.Duration(math.Pow(2, float64(reconnectAttempts))) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			time.Sleep(backoff + time.Duration(rand.Intn(1000)-500)*time.Millisecond)
		}
		conn, _, err := websocket.DefaultDialer.Dial(wsSpotURL, nil)
		if err != nil {
			log.Printf("[BITMART-OB-SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			reconnectAttempts++
			continue
		}
		if err := sw.eventLoop(conn); err != nil {
			log.Printf("[BITMART-OB-SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredSymbols() {
			return
		}
		reconnectAttempts++
	}
}

func (sw *OrderBookShardWorker) hasDesiredSymbols() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.desiredSymbols) > 0
}

type desiredOrderBookConfig struct {
	Depth int
	Mode  string
}

func (sw *OrderBookShardWorker) desiredSnapshot() map[string]desiredOrderBookConfig {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	out := make(map[string]desiredOrderBookConfig, len(sw.desiredSymbols))
	for symbol, depth := range sw.desiredSymbols {
		out[symbol] = desiredOrderBookConfig{
			Depth: depth,
			Mode:  sw.desiredModes[symbol],
		}
	}
	return out
}

func (sw *OrderBookShardWorker) eventLoop(conn *websocket.Conn) error {
	msgCh := make(chan socketMessage, 256)
	errCh := make(chan error, 1)
	pingTicker := time.NewTicker(pingIntervalSec * time.Second)
	defer pingTicker.Stop()

	go func() {
		for {
			_ = conn.SetReadDeadline(time.Now().Add(readIdleSec * time.Second))
			mt, payload, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- socketMessage{messageType: mt, payload: payload}
		}
	}()

	initial := sw.desiredSnapshot()
	if err := sw.sendBatchSubscribe(conn, initial, false); err != nil {
		return err
	}

	for {
		select {
		case msg := <-msgCh:
			payload, err := decodePayload(msg.payload)
			if err != nil {
				return err
			}
			var envelope wsOrderBookEnvelope
			if err := json.Unmarshal(payload, &envelope); err != nil {
				continue
			}
			if envelope.Error != nil {
				log.Printf("[BITMART-OB-SHARD-ERROR] code=%d msg=%s", envelope.Error.Code, envelope.Error.Message)
				continue
			}
			for _, entry := range envelope.Data {
				update, err := sw.applyEntry(envelope.Table, entry)
				if err != nil {
					metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeOBUpdate)
					continue
				}
				if update != nil {
					sw.dataCh <- update
				}
			}
		case cmd := <-sw.commandCh:
			sw.mu.Lock()
			for _, symbol := range cmd.Symbols {
				if cmd.Action == "subscribe" {
					nextDepth := NormalizeOrderBookDepth(cmd.Depth)
					nextMode := NormalizeOrderBookMode(cmd.Mode)
					prevDepth, hadPrev := sw.desiredSymbols[symbol]
					prevMode := sw.desiredModes[symbol]
					sw.desiredSymbols[symbol] = nextDepth
					sw.desiredModes[symbol] = nextMode
					if hadPrev && (prevDepth != nextDepth || prevMode != nextMode) {
						if err := sw.sendSubscribe(conn, symbol, prevDepth, prevMode, true); err != nil {
							sw.mu.Unlock()
							return err
						}
					}
				} else {
					delete(sw.desiredSymbols, symbol)
					delete(sw.desiredModes, symbol)
					delete(sw.orderbooks, symbol)
				}
			}
			sw.mu.Unlock()
			batch := make(map[string]desiredOrderBookConfig, len(cmd.Symbols))
			for _, symbol := range cmd.Symbols {
				batch[symbol] = desiredOrderBookConfig{Depth: cmd.Depth, Mode: cmd.Mode}
			}
			if err := sw.sendBatchSubscribe(conn, batch, cmd.Action == "unsubscribe"); err != nil {
				return err
			}
		case <-pingTicker.C:
			if err := conn.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
				return err
			}
		case err := <-errCh:
			return err
		case <-sw.stopCh:
			return nil
		}
	}
}

func (sw *OrderBookShardWorker) sendSubscribe(conn *websocket.Conn, symbol string, depth int, mode string, unsubscribe bool) error {
	op := "subscribe"
	if unsubscribe {
		op = "unsubscribe"
	}
	arg := "spot/" + orderBookChannel(depth, mode) + ":" + symbol
	return conn.WriteJSON(wsSubscribeRequest{Op: op, Args: []string{arg}})
}

func (sw *OrderBookShardWorker) sendBatchSubscribe(conn *websocket.Conn, symbols map[string]desiredOrderBookConfig, unsubscribe bool) error {
	if len(symbols) == 0 {
		return nil
	}
	args := make([]string, 0, len(symbols))
	for symbol, cfg := range symbols {
		args = append(args, "spot/"+orderBookChannel(cfg.Depth, cfg.Mode)+":"+symbol)
	}
	sort.Strings(args)

	op := "subscribe"
	if unsubscribe {
		op = "unsubscribe"
	}
	for start := 0; start < len(args); start += commandBatchSize {
		end := start + commandBatchSize
		if end > len(args) {
			end = len(args)
		}
		if err := conn.WriteJSON(wsSubscribeRequest{Op: op, Args: args[start:end]}); err != nil {
			return err
		}
	}
	return nil
}

func (sw *OrderBookShardWorker) applyEntry(table string, entry wsOrderBookEntry) (*shared_types.OrderBookUpdate, error) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	state := sw.orderbooks[entry.Symbol]
	if state == nil {
		state = &localOrderBookState{
			Bids: make(map[string]shared_types.OrderBookLevel),
			Asks: make(map[string]shared_types.OrderBookLevel),
		}
		sw.orderbooks[entry.Symbol] = state
	}

	if entry.Type == "snapshot" || !strings.Contains(table, "increase") {
		state.Bids = make(map[string]shared_types.OrderBookLevel)
		state.Asks = make(map[string]shared_types.OrderBookLevel)
	}
	if err := normalizeLevels(entry.Bids, state.Bids); err != nil {
		return nil, err
	}
	if err := normalizeLevels(entry.Asks, state.Asks); err != nil {
		return nil, err
	}

	depth := sw.desiredSymbols[entry.Symbol]
	mode := NormalizeOrderBookMode(sw.desiredModes[entry.Symbol])
	limit := depth
	if mode == modeAll {
		limit = 0
	} else if limit == 0 {
		limit = defaultDepthLevel
	}
	return &shared_types.OrderBookUpdate{
		Exchange:       "bitmart",
		Symbol:         TranslateSymbolFromExchange(entry.Symbol),
		MarketType:     "spot",
		Timestamp:      entry.MST,
		GoTimestamp:    time.Now().UnixMilli(),
		IngestUnixNano: time.Now().UnixNano(),
		UpdateType:     entry.TypeOrSnapshot(table),
		Bids:           topLevels(state.Bids, true, limit),
		Asks:           topLevels(state.Asks, false, limit),
		DataType:       "orderbooks",
	}, nil
}

func (entry wsOrderBookEntry) TypeOrSnapshot(table string) string {
	if entry.Type != "" {
		return entry.Type
	}
	if strings.Contains(table, "increase") {
		return "update"
	}
	return "snapshot"
}

func topLevels(src map[string]shared_types.OrderBookLevel, desc bool, limit int) []shared_types.OrderBookLevel {
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
	if limit > 0 && len(levels) > limit {
		levels = levels[:limit]
	}
	return levels
}
