package gateio

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
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
	log.Printf("[GATEIO-SHARD] Starte Worker fuer %s", sw.marketType)

	for {
		select {
		case <-sw.stopCh:
			return
		default:
		}
		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[GATEIO-SHARD] Connect Fehler (%s): %v", sw.marketType, err)
			time.Sleep(5 * time.Second)
			continue
		}
		if symbols := sw.desiredSymbolsSnapshot(); len(symbols) > 0 {
			_ = sw.sendSubscriptionChunks(conn, "subscribe", symbols)
		}
		if err := sw.eventLoop(conn); err != nil {
			log.Printf("[GATEIO-SHARD] Disconnect (%s): %v", sw.marketType, err)
		}
		conn.Close()
		time.Sleep(2 * time.Second)
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

	pingTicker := time.NewTicker(20 * time.Second)
	defer pingTicker.Stop()

	for {
		select {
		case cmd := <-sw.commandCh:
			sw.applyCommand(cmd)
			if err := sw.sendSubscriptionChunks(conn, gateEvent(cmd.Action), cmd.Symbols); err != nil {
				return err
			}
		case msg := <-msgCh:
			if err := sw.handleMessage(msg); err != nil {
				log.Printf("[GATEIO-SHARD-WARN] Parse Fehler (%s): %v", sw.marketType, err)
			}
		case err := <-errCh:
			return err
		case <-pingTicker.C:
			if err := conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(5*time.Second)); err != nil {
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

func (sw *ShardWorker) sendSubscriptionChunks(conn *websocket.Conn, event string, symbols []string) error {
	if len(symbols) == 0 {
		return nil
	}
	for i := 0; i < len(symbols); i += subscribeChunk {
		end := i + subscribeChunk
		if end > len(symbols) {
			end = len(symbols)
		}
		chunk := symbols[i:end]
		sw.nextID++
		req := wsRequest{
			Time:    time.Now().Unix(),
			ID:      sw.nextID,
			Channel: tradeChannel(sw.marketType),
			Event:   event,
			Payload: chunk,
		}
		if err := conn.WriteJSON(req); err != nil {
			sw.emitStatus("stream_"+event+"_failed", chunk, "write_failed", err.Error())
			return err
		}
		time.Sleep(150 * time.Millisecond)
	}
	return nil
}

func (sw *ShardWorker) handleMessage(msg []byte) error {
	dec := json.NewDecoder(bytes.NewReader(msg))
	dec.UseNumber()
	var envelope wsEnvelope
	if err := dec.Decode(&envelope); err != nil {
		return err
	}
	if envelope.Error != nil {
		return fmt.Errorf("gate error channel=%s event=%s code=%d message=%s", envelope.Channel, envelope.Event, envelope.Error.Code, envelope.Error.Message)
	}
	if envelope.Event != "update" || envelope.Channel != tradeChannel(sw.marketType) {
		return nil
	}
	goTimestamp := time.Now().UnixMilli()
	ingestNano := time.Now().UnixNano()
	switch sw.marketType {
	case "spot":
		trade, err := decodeSpotTrade(envelope.Result)
		if err != nil {
			metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
			return err
		}
		update, err := NormalizeSpotTrade(trade, goTimestamp, ingestNano)
		if err != nil {
			metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
			return err
		}
		sw.dataCh <- update
	case "swap":
		trades, err := decodeFuturesTrades(envelope.Result)
		if err != nil {
			metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
			return err
		}
		for _, trade := range trades {
			update, err := NormalizeFuturesTrade(trade, goTimestamp, ingestNano)
			if err != nil {
				metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
				continue
			}
			sw.dataCh <- update
		}
	}
	return nil
}

func (sw *ShardWorker) emitStatus(eventType string, symbols []string, reason string, message string) {
	if sw.statusCh == nil {
		return
	}
	for _, symbol := range symbols {
		sw.statusCh <- &shared_types.StreamStatusEvent{
			Type:       eventType,
			Exchange:   "gate",
			MarketType: sw.marketType,
			DataType:   "trades",
			Symbol:     TranslateSymbolFromExchange(symbol, sw.marketType),
			Reason:     reason,
			Message:    message,
			Timestamp:  time.Now().UnixMilli(),
		}
	}
}

func gateEvent(action string) string {
	if action == "unsubscribe" {
		return "unsubscribe"
	}
	return "subscribe"
}
