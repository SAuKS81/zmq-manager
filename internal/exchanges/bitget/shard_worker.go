package bitget

import (
	"encoding/json"
	"log"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"bybit-watcher/internal/shared_types"
	"github.com/gorilla/websocket"
)

// ShardCommand ist ein Befehl, der an einen laufenden Shard gesendet wird.
type ShardCommand struct {
	Action  string
	Symbols []string
}

// ShardWorker verwaltet eine einzelne WebSocket-Verbindung für Bitget.
type ShardWorker struct {
	wsURL          string
	marketType     string
	initialSymbols []string
	commandCh      chan ShardCommand
	stopCh         <-chan struct{}
	dataCh         chan<- *shared_types.TradeUpdate
	wg             *sync.WaitGroup

	mu            sync.Mutex
	activeSymbols map[string]bool
}

// NewShardWorker erstellt einen neuen Worker.
func NewShardWorker(wsURL, marketType string, initialSymbols []string, stopCh <-chan struct{}, dataCh chan<- *shared_types.TradeUpdate, wg *sync.WaitGroup) *ShardWorker {
	sw := &ShardWorker{
		wsURL:          wsURL,
		marketType:     marketType,
		initialSymbols: initialSymbols,
		commandCh:      make(chan ShardCommand, 10),
		stopCh:         stopCh,
		dataCh:         dataCh,
		wg:             wg,
		activeSymbols:  make(map[string]bool),
	}
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
	log.Printf("[BITGET-SHARD] Starte Worker für %d Symbole (%s)", symbolCount, sw.marketType)

	var reconnectAttempts int = 0

reconnectLoop:
	for {
		select {
		case <-sw.stopCh:
			break reconnectLoop
		default:
		}

		if reconnectAttempts > 0 {
			backoff := time.Duration(math.Pow(2, float64(reconnectAttempts))) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			jitter := time.Duration(rand.Intn(1000)-500) * time.Millisecond
			waitTime := backoff + jitter

			log.Printf("[BITGET-SHARD-BACKOFF] Reconnect-Versuch #%d. Warte für %v...", reconnectAttempts, waitTime)
			time.Sleep(waitTime)
		}

		conn, _, err := websocket.DefaultDialer.Dial(sw.wsURL, nil)
		if err != nil {
			log.Printf("[BITGET-SHARD-ERROR] Connect fehlgeschlagen: %v", err)
			reconnectAttempts++
			continue
		}

		sw.mu.Lock()
		var symbolsToSub []string
		for s := range sw.activeSymbols {
			symbolsToSub = append(symbolsToSub, s)
		}
		sw.mu.Unlock()

		if len(symbolsToSub) > 0 {
			log.Printf("[BITGET-SHARD-INFO] (Re)Connect erfolgreich. Sende initiale Abos für %d Symbole.", len(symbolsToSub))
			// Sende den Befehl asynchron, damit der Start der eventLoop nicht blockiert wird.
			go func() {
				sw.commandCh <- ShardCommand{Action: "subscribe", Symbols: symbolsToSub}
			}()
		} else {
			log.Printf("[BITGET-SHARD-INFO] (Re)Connect erfolgreich. Warte auf Abonnement-Befehle.")
		}

		reconnectAttempts = 0

		err = sw.eventLoop(conn)
		conn.Close()

		if err == nil {
			break reconnectLoop
		}

		log.Printf("[BITGET-SHARD-INFO] Verbindung unterbrochen (Fehler: %v), versuche Reconnect...", err)
		reconnectAttempts++
	}

	log.Printf("[BITGET-SHARD] Worker beendet.")
}

// writePump ist die einzige Goroutine, die Nachrichten an die WebSocket-Verbindung schreibt.
// Sie stellt sicher, dass es keine nebenläufigen Schreibzugriffe gibt.
func (sw *ShardWorker) writePump(conn *websocket.Conn, writeCh <-chan []byte, errCh chan<- error) {
	defer conn.Close()
	for {
		select {
		case message, ok := <-writeCh:
			if !ok {
				// Kanal wurde geschlossen, sende eine Close-Nachricht und beende.
				conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := conn.WriteMessage(websocket.TextMessage, message); err != nil {
				errCh <- err
				return
			}
		case <-sw.stopCh:
			return
		}
	}
}

// readPump ist die einzige Goroutine, die von der WebSocket-Verbindung liest.
func (sw *ShardWorker) readPump(conn *websocket.Conn, msgCh chan<- []byte, errCh chan<- error) {
	defer conn.Close()
	for {
		_ = conn.SetReadDeadline(time.Now().Add(readIdleSec * time.Second))
		_, message, err := conn.ReadMessage()
		if err != nil {
			errCh <- err
			return
		}
		msgCh <- message
	}
}

// eventLoop verwaltet die Logik des Shards: Befehle, Pings und Datenverarbeitung.
func (sw *ShardWorker) eventLoop(conn *websocket.Conn) error {
	msgCh := make(chan []byte)
	errCh := make(chan error, 1)
	writeCh := make(chan []byte, 10) // Puffer für ausgehende Nachrichten
	pingTicker := time.NewTicker(pingEverySec * time.Second)
	defer pingTicker.Stop()

	go sw.readPump(conn, msgCh, errCh)
	go sw.writePump(conn, writeCh, errCh)

	for {
		select {
		case msg := <-msgCh:
			// Debug-Ausgabe für jede eingehende Nachricht
			//log.Printf("[BITGET-SHARD-RECV-VERBOSE] %s", string(msg))
			goTimestamp := time.Now().UnixMilli()
			msgStr := string(msg)

			if msgStr == "pong" {
				continue
			}

			if strings.Contains(msgStr, `"action":"update"`) && strings.Contains(msgStr, `"channel":"trade"`) {
				var wsMsg wsMsg
				if err := json.Unmarshal(msg, &wsMsg); err != nil {
					log.Printf("[BITGET-SHARD-WARN] Fehler beim Unmarshalling: %v", err)
					continue
				}
				for _, trade := range wsMsg.Data {
					normalizedTrade, err := NormalizeTrade(trade, wsMsg.Arg, goTimestamp)
					if err != nil {
						log.Printf("[BITGET-SHARD-WARN] Fehler beim Normalisieren: %v", err)
						continue
					}
					sw.dataCh <- normalizedTrade
				}
			}

		case cmd := <-sw.commandCh:
			log.Printf("[BITGET-SHARD] Erhalte Befehl: %s für %d Symbole", cmd.Action, len(cmd.Symbols))
			// Sende Abos in einer Goroutine, damit die Pausen die eventLoop nicht blockieren.
			go sw.sendSubscription(writeCh, cmd.Action, cmd.Symbols)

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
			writeCh <- []byte("ping")

		case err := <-errCh:
			return err // Beendet die eventLoop und löst einen Reconnect aus

		case <-sw.stopCh:
			close(writeCh) // Signalisiert der writePump, sich zu beenden
			return nil     // Normales Beenden, kein Reconnect
		}
	}
}

// sendSubscription sendet jetzt Nachrichten an einen Kanal, nicht mehr direkt an die Verbindung.
func (sw *ShardWorker) sendSubscription(writeCh chan<- []byte, op string, symbols []string) {
	if len(symbols) == 0 {
		return
	}

	instType, err := getInstType(sw.marketType)
	if err != nil {
		log.Printf("[BITGET-SHARD-ERROR] Ungültiger Market-Type für Abo: %v", err)
		return
	}

	for i := 0; i < len(symbols); i += symbolsPerBatch {
		end := i + symbolsPerBatch
		if end > len(symbols) {
			end = len(symbols)
		}
		batch := symbols[i:end]

		log.Printf("[BITGET-SHARD-SEND] Sende '%s' für Batch %d/%d (%d Symbole)", op, (i/symbolsPerBatch)+1, (len(symbols)+symbolsPerBatch-1)/symbolsPerBatch, len(batch))

		args := make([]wsSubArg, len(batch))
		for j, s := range batch {
			args[j] = wsSubArg{InstType: instType, Channel: "trade", InstID: s}
		}

		req := wsRequest{Op: op, Args: args}
		payload, err := json.Marshal(req)
		if err != nil {
			log.Printf("[BITGET-SHARD-SEND-ERROR] Fehler beim Marshalling: %v", err)
			continue
		}

		writeCh <- payload

		if end < len(symbols) {
			time.Sleep(delayPerBatchMs * time.Millisecond)
		}
	}
}
