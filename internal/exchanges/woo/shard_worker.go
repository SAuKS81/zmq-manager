package woo

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"

	"github.com/gorilla/websocket"
)

type ShardWorker struct {
	wsURL      string
	marketType string
	commandCh  chan shardCommand
	stopCh     <-chan struct{}
	dataCh     chan<- *shared_types.TradeUpdate
	statusCh   chan<- *shared_types.StreamStatusEvent
	wg         *sync.WaitGroup

	mu             sync.Mutex
	desiredSymbols map[string]bool
	nextID         int64
}

func NewShardWorker(wsURL, marketType string, stopCh <-chan struct{}, dataCh chan<- *shared_types.TradeUpdate, statusCh chan<- *shared_types.StreamStatusEvent, wg *sync.WaitGroup) *ShardWorker {
	return &ShardWorker{
		wsURL:          wsURL,
		marketType:     marketType,
		commandCh:      make(chan shardCommand, 1000),
		stopCh:         stopCh,
		dataCh:         dataCh,
		statusCh:       statusCh,
		wg:             wg,
		desiredSymbols: make(map[string]bool),
	}
}

func (sw *ShardWorker) Run() {
	sw.wg.Add(1)
	defer sw.wg.Done()
	log.Printf("[WOO-SHARD] Starte Worker fuer %s", sw.marketType)

	for {
		select {
		case <-sw.stopCh:
			return
		default:
		}
		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[WOO-SHARD] Connect Fehler (%s): %v", sw.marketType, err)
			sw.emitStatus("stream_subscribe_failed", sw.desiredSymbolsSnapshot(), "connect_failed", err.Error())
			time.Sleep(reconnectDelaySeconds * time.Second)
			continue
		}
		if symbols := sw.desiredSymbolsSnapshot(); len(symbols) > 0 {
			_ = sw.sendSubscriptions(conn, "subscribe", symbols)
		}
		if err := sw.eventLoop(conn); err != nil {
			log.Printf("[WOO-SHARD] Disconnect (%s): %v", sw.marketType, err)
		}
		conn.Close()
		time.Sleep(reconnectDelaySeconds * time.Second)
	}
}

func (sw *ShardWorker) desiredSymbolsSnapshot() []string {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	symbols := make([]string, 0, len(sw.desiredSymbols))
	for symbol := range sw.desiredSymbols {
		symbols = append(symbols, symbol)
	}
	return symbols
}

func (sw *ShardWorker) eventLoop(conn *websocket.Conn) error {
	msgCh := make(chan []byte, 256)
	errCh := make(chan error, 1)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			select {
			case msgCh <- msg:
			case <-sw.stopCh:
				return
			}
		}
	}()

	pingTicker := time.NewTicker(pingIntervalSeconds * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case cmd := <-sw.commandCh:
			sw.applyCommand(cmd)
			if err := sw.sendSubscriptions(conn, cmd.Action, cmd.Symbols); err != nil {
				return err
			}
		case msg := <-msgCh:
			if err := sw.handleMessage(conn, msg); err != nil {
				log.Printf("[WOO-SHARD-WARN] Parse Fehler (%s): %v", sw.marketType, err)
			}
		case err := <-errCh:
			return err
		case <-pingTicker.C:
			if err := conn.WriteJSON(map[string]string{"event": "ping"}); err != nil {
				return err
			}
		case <-sw.stopCh:
			return nil
		case <-done:
			return nil
		}
	}
}

func (sw *ShardWorker) applyCommand(cmd shardCommand) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	for _, symbol := range cmd.Symbols {
		switch cmd.Action {
		case "subscribe":
			sw.desiredSymbols[symbol] = true
		case "unsubscribe":
			delete(sw.desiredSymbols, symbol)
		}
	}
}

func (sw *ShardWorker) sendSubscriptions(conn *websocket.Conn, event string, symbols []string) error {
	if len(symbols) == 0 {
		return nil
	}
	for _, symbol := range symbols {
		sw.nextID++
		req := wsRequest{
			ID:    fmt.Sprintf("woo-%s-%d", sw.marketType, sw.nextID),
			Topic: tradeTopic(symbol),
			Event: event,
		}
		if err := conn.WriteJSON(req); err != nil {
			sw.emitStatus("stream_"+event+"_failed", []string{symbol}, "write_failed", err.Error())
			return err
		}
		time.Sleep(subscribeSendDelayMS * time.Millisecond)
	}
	return nil
}

func (sw *ShardWorker) handleMessage(conn *websocket.Conn, msg []byte) error {
	dec := json.NewDecoder(bytes.NewReader(msg))
	dec.UseNumber()
	var envelope wsEnvelope
	if err := dec.Decode(&envelope); err != nil {
		return err
	}
	if envelope.Event == "ping" {
		return conn.WriteJSON(map[string]string{"event": "pong"})
	}
	if envelope.Event == "pong" {
		return nil
	}
	if envelope.Success != nil {
		if !*envelope.Success {
			symbol := symbolFromTopic(envelope.Topic)
			if symbol == "" {
				symbol = symbolFromData(envelope.Data)
			}
			reason := "subscribe_rejected"
			if envelope.Event == "unsubscribe" {
				reason = "unsubscribe_rejected"
			}
			sw.emitStatus("stream_"+envelope.Event+"_failed", []string{symbol}, reason, envelope.Message)
		}
		return nil
	}
	if envelope.Topic == "" || !strings.HasSuffix(envelope.Topic, "@trade") {
		return nil
	}
	goTimestamp := time.Now().UnixMilli()
	ingestNano := time.Now().UnixNano()
	tradeItem, err := decodeTrade(envelope.Data)
	if err != nil {
		metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
		return err
	}
	if tradeItem.Symbol == "" {
		tradeItem.Symbol = symbolFromTopic(envelope.Topic)
	}
	update, err := NormalizeTrade(tradeItem, sw.marketType, envelope.TS, goTimestamp, ingestNano)
	if err != nil {
		metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
		return err
	}
	sw.dataCh <- update
	return nil
}

func (sw *ShardWorker) emitStatus(eventType string, symbols []string, reason string, message string) {
	if sw.statusCh == nil {
		return
	}
	for _, symbol := range symbols {
		if symbol == "" {
			continue
		}
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "woo",
			MarketType: sw.marketType,
			DataType:   "trades",
			Symbol:     TranslateSymbolFromExchange(symbol, sw.marketType),
			Reason:     reason,
			Message:    message,
			Timestamp:  time.Now().UnixMilli(),
		}
	}
}

func symbolFromTopic(topic string) string {
	if idx := strings.Index(topic, "@"); idx >= 0 {
		return topic[:idx]
	}
	return topic
}

func symbolFromData(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var symbol string
	if err := json.Unmarshal(raw, &symbol); err == nil {
		return symbolFromTopic(symbol)
	}
	var payload struct {
		Symbol string `json:"symbol"`
	}
	if err := json.Unmarshal(raw, &payload); err == nil {
		return payload.Symbol
	}
	return ""
}
