package bitmart

import (
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
)

type socketMessage struct {
	messageType int
	payload     []byte
}

type ShardWorker struct {
	commandCh      chan ShardCommand
	stopCh         <-chan struct{}
	dataCh         chan<- *shared_types.TradeUpdate
	statusCh       chan<- *shared_types.StreamStatusEvent
	wg             *sync.WaitGroup
	mu             sync.Mutex
	desiredSymbols map[string]bool
}

func NewShardWorker(initialSymbols []string, stopCh <-chan struct{}, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *ShardWorker {
	sw := &ShardWorker{
		commandCh:      make(chan ShardCommand, 32),
		stopCh:         stopCh,
		dataCh:         dataCh,
		statusCh:       statusCh,
		wg:             wg,
		desiredSymbols: make(map[string]bool),
	}
	for _, symbol := range initialSymbols {
		sw.desiredSymbols[symbol] = true
	}
	return sw
}

func (sw *ShardWorker) Run() {
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
			log.Printf("[BITMART-SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			reconnectAttempts++
			continue
		}
		if err := sw.eventLoop(conn); err != nil {
			log.Printf("[BITMART-SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		}
		conn.Close()
		if !sw.hasDesiredSymbols() {
			return
		}
		reconnectAttempts++
	}
}

func (sw *ShardWorker) hasDesiredSymbols() bool {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.desiredSymbols) > 0
}

func (sw *ShardWorker) desiredSymbolsSnapshot() []string {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	out := make([]string, 0, len(sw.desiredSymbols))
	for symbol := range sw.desiredSymbols {
		out = append(out, symbol)
	}
	return out
}

func (sw *ShardWorker) eventLoop(conn *websocket.Conn) error {
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

	if err := sw.sendSubscribe(conn, sw.desiredSymbolsSnapshot(), false); err != nil {
		return err
	}

	for {
		select {
		case msg := <-msgCh:
			payload, err := decodePayload(msg.payload)
			if err != nil {
				return err
			}
			var envelope wsEnvelope
			if err := json.Unmarshal(payload, &envelope); err != nil {
				continue
			}
			if envelope.Error != nil {
				log.Printf("[BITMART-SHARD-ERROR] code=%d msg=%s", envelope.Error.Code, envelope.Error.Message)
				continue
			}
			for _, trade := range envelope.Data {
				normalized, err := NormalizeTrade(trade, time.Now().UnixMilli(), time.Now().UnixNano())
				if err != nil {
					metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
					continue
				}
				sw.dataCh <- normalized
			}
		case cmd := <-sw.commandCh:
			sw.mu.Lock()
			for _, symbol := range cmd.Symbols {
				if cmd.Action == "subscribe" {
					sw.desiredSymbols[symbol] = true
				} else {
					delete(sw.desiredSymbols, symbol)
				}
			}
			sw.mu.Unlock()
			if err := sw.sendSubscribe(conn, cmd.Symbols, cmd.Action == "unsubscribe"); err != nil {
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

func (sw *ShardWorker) sendSubscribe(conn *websocket.Conn, symbols []string, unsubscribe bool) error {
	if len(symbols) == 0 {
		return nil
	}
	op := "subscribe"
	if unsubscribe {
		op = "unsubscribe"
	}
	for start := 0; start < len(symbols); start += commandBatchSize {
		end := start + commandBatchSize
		if end > len(symbols) {
			end = len(symbols)
		}
		args := make([]string, 0, end-start)
		for _, symbol := range symbols[start:end] {
			args = append(args, "spot/trade:"+symbol)
		}
		if err := conn.WriteJSON(wsSubscribeRequest{Op: op, Args: args}); err != nil {
			return err
		}
	}
	return nil
}
