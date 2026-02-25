package bybit

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"

	"bybit-watcher/internal/metrics"
	"bybit-watcher/internal/shared_types"
	gjson "github.com/goccy/go-json"
	"github.com/gorilla/websocket"
)

// ShardCommand ist ein Befehl, der an einen laufenden Shard gesendet wird.
type ShardCommand struct {
	Action  string
	Symbols []string
}

// ShardWorker verwaltet eine einzelne WebSocket-Verbindung.
type ShardWorker struct {
	wsURL          string
	marketType     string
	initialSymbols []string
	commandCh      chan ShardCommand
	stopCh         <-chan struct{}
	dataCh         chan<- *shared_types.TradeUpdate
	wg             *sync.WaitGroup

	// KORREKTUR: mu schützt den Zugriff auf activeSymbols von verschiedenen Goroutinen
	mu            sync.Mutex
	activeSymbols map[string]bool
}

var (
	bybitTradeTopicNeedle = []byte(`"topic":"publicTrade.`)
	bybitTradePongNeedle  = []byte(`"op":"pong"`)
)

// NewShardWorker erstellt einen neuen Worker.
func NewShardWorker(wsURL, marketType string, initialSymbols []string, stopCh <-chan struct{}, dataCh chan<- *shared_types.TradeUpdate, wg *sync.WaitGroup) *ShardWorker {
	sw := &ShardWorker{
		wsURL:          wsURL,
		marketType:     marketType,
		initialSymbols: initialSymbols, // Wird nur für den allerersten Start benötigt
		commandCh:      make(chan ShardCommand, 10),
		stopCh:         stopCh,
		dataCh:         dataCh,
		wg:             wg,
		activeSymbols:  make(map[string]bool),
	}
	// Initialisiere die Liste der aktiven Symbole
	for _, s := range initialSymbols {
		sw.activeSymbols[s] = true
	}
	return sw
}

// Run startet die Hauptschleife des Workers.
func (sw *ShardWorker) Run() {
	defer sw.wg.Done()

	sw.mu.Lock()
	symbolCount := len(sw.activeSymbols)
	sw.mu.Unlock()
	log.Printf("[SHARD] Starte Worker für %d Symbole (%s)", symbolCount, sw.marketType)

	// NEU: Zähler für aufeinanderfolgende Reconnect-Versuche
	var reconnectAttempts int = 0

reconnectLoop:
	for {
		select {
		case <-sw.stopCh:
			break reconnectLoop
		default:
		}

		// NEU: Exponential Backoff Logik
		if reconnectAttempts > 0 {
			// Berechne die Basis-Wartezeit (z.B. 1s, 2s, 4s, 8s...)
			backoff := time.Duration(math.Pow(2, float64(reconnectAttempts))) * time.Second
			// Begrenze auf ein Maximum (z.B. 30 Sekunden)
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			// Füge zufälligen Jitter hinzu (z.B. +/- 500ms)
			jitter := time.Duration(rand.Intn(1000)-500) * time.Millisecond
			waitTime := backoff + jitter

			log.Printf("[SHARD-BACKOFF] Reconnect-Versuch #%d. Warte für %v...", reconnectAttempts, waitTime)
			time.Sleep(waitTime)
		}

		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			reconnectAttempts++ // Erhöhe den Zähler
			continue
		}

		sw.mu.Lock()
		var symbolsToSub []string
		for s := range sw.activeSymbols {
			symbolsToSub = append(symbolsToSub, s)
		}
		sw.mu.Unlock()

		log.Printf("[SHARD-INFO] (Re)Connect: Versuche %d Symbole zu abonnieren.", len(symbolsToSub))
		if err := sw.sendSubscription(conn, "subscribe", symbolsToSub); err != nil {
			log.Printf("[SHARD-ERROR] (Re)Subscribe fehlgeschlagen: %v", err)
			conn.Close()
			reconnectAttempts++ // Erhöhe den Zähler
			continue
		}

		log.Printf("[SHARD-SUCCESS] (Re)Connect und Subscribe für %d Symbole erfolgreich.", len(symbolsToSub))

		// NEU: Setze den Zähler bei Erfolg zurück
		reconnectAttempts = 0

		err = sw.readLoop(conn)
		conn.Close()

		if err == nil {
			break reconnectLoop
		}

		log.Printf("[SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		reconnectAttempts++ // Erhöhe den Zähler
	}

	log.Printf("[SHARD] Worker beendet.")
}

// sendSubscription sendet eine (Un-)Subscribe-Nachricht.
func (sw *ShardWorker) sendSubscription(conn *websocket.Conn, op string, symbols []string) error {
	if len(symbols) == 0 {
		return nil
	}
	log.Printf("[SHARD-SEND] Sende '%s' für %d Symbole", op, len(symbols))

	args := make([]string, len(symbols))
	for i, s := range symbols {
		args[i] = fmt.Sprintf("publicTrade.%s", s)
	}

	msg := map[string]interface{}{"op": op, "args": args}
	err := conn.WriteJSON(msg)
	if err != nil {
		log.Printf("[SHARD-SEND-ERROR] Fehler beim Senden von '%s': %v", op, err)
	}
	return err
}

// readLoop ist die Hauptschleife zum Lesen von WebSocket-Nachrichten und Befehlen.
func (sw *ShardWorker) readLoop(conn *websocket.Conn) error {
	msgCh := make(chan []byte, 256)
	errCh := make(chan error, 1)
	pingTicker := time.NewTicker(pingEverySec * time.Second)
	defer pingTicker.Stop()

	go func() {
		for {
			_ = conn.SetReadDeadline(time.Now().Add(readIdleSec * time.Second))
			_, message, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			msgCh <- message
		}
	}()

	for {
		select {
		case msg := <-msgCh:
			// ... (Nachrichtenverarbeitung bleibt gleich)
			ingestNow := time.Now()
			goTimestamp := ingestNow.UnixMilli()
			if bytes.Contains(msg, bybitTradePongNeedle) {
				continue
			}
			if bytes.Contains(msg, bybitTradeTopicNeedle) {
				var wsMsg wsMsg
				if err := gjson.Unmarshal(msg, &wsMsg); err != nil {
					metrics.RecordDropped(metrics.ReasonParseError, metrics.TypeTrade)
					continue
				}
				for _, trade := range wsMsg.Data {
					normalizedTrade, err := NormalizeTrade(trade, sw.marketType, goTimestamp, ingestNow.UnixNano())
					if err != nil {
						continue
					}
					sw.dataCh <- normalizedTrade
				}
			}

		case cmd := <-sw.commandCh:
			log.Printf("[SHARD] Erhalte Befehl: %s für %v", cmd.Action, cmd.Symbols)
			if err := sw.sendSubscription(conn, cmd.Action, cmd.Symbols); err != nil {
				return err // Erzwinge Reconnect bei Fehler
			}

			// KORREKTUR: Schütze den Zugriff auf die Map mit einem Mutex
			sw.mu.Lock()
			for _, s := range cmd.Symbols {
				if cmd.Action == "subscribe" {
					sw.activeSymbols[s] = true
				} else {
					delete(sw.activeSymbols, s)
				}
			}
			sw.mu.Unlock()

		case <-pingTicker.C:
			if err := conn.WriteJSON(map[string]string{"op": "ping"}); err != nil {
				return err
			}
		case err := <-errCh:
			return err
		case <-sw.stopCh:
			return nil
		}
	}
}
