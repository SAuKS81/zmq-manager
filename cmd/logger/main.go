package main

import (
	"compress/gzip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"bybit-watcher/internal/shared_types"
	"github.com/go-zeromq/zmq4"
)

const brokerAddress = "tcp://localhost:5555"

// fileManager verwaltet die In-Memory-Puffer für jedes Symbol.
type fileManager struct {
	mu      sync.Mutex
	baseDir string
	buffers map[string][][]string // map[symbol]buffer
}

func newFileManager(baseDir string) *fileManager {
	return &fileManager{
		baseDir: baseDir,
		buffers: make(map[string][][]string),
	}
}

// addRecord fügt einen Datensatz zum Puffer hinzu und leert den Puffer auf die Festplatte, wenn er voll ist.
func (fm *fileManager) addRecord(symbol string, record []string) {
	fm.mu.Lock()
	defer fm.mu.Unlock()

	if _, ok := fm.buffers[symbol]; !ok {
		fm.buffers[symbol] = make([][]string, 0, 200)
	}

	fm.buffers[symbol] = append(fm.buffers[symbol], record)

	if len(fm.buffers[symbol]) >= 200 {
		bufferToFlush := fm.buffers[symbol]
		fm.buffers[symbol] = make([][]string, 0, 200)
		
		go fm.flushBuffer(symbol, bufferToFlush)
	}
}

// flushBuffer öffnet die Datei, schreibt den Batch und schließt sie wieder.
func (fm *fileManager) flushBuffer(symbol string, buffer [][]string) {
	if len(buffer) == 0 {
		return
	}

	dateStr := time.Now().UTC().Format("2006-01-02")
	fileName := fmt.Sprintf("%s_%s.csv.gz", strings.ReplaceAll(symbol, "/", ""), dateStr)
	filePath := filepath.Join(fm.baseDir, fileName)

	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("[LOGGER-ERROR] Datei konnte nicht geöffnet werden für %s: %v", symbol, err)
		return
	}
	defer file.Close()

	stat, _ := file.Stat()
	isEmpty := stat.Size() == 0

	gz := gzip.NewWriter(file)
	defer gz.Close()

	csvWriter := csv.NewWriter(gz)

	if isEmpty {
		// KORREKTUR: Header um die zweite Latenzspalte erweitert
		header := []string{"timestamp", "symbol", "side", "size", "price", "tickDirection", "trdMatchID", "grossValue", "homeNotional", "foreignNotional", "e2e_latency_ms", "internal_latency_ms"}
		if err := csvWriter.Write(header); err != nil {
			log.Printf("[LOGGER-ERROR] Header konnte nicht geschrieben werden für %s: %v", symbol, err)
			return
		}
	}

	if err := csvWriter.WriteAll(buffer); err != nil {
		log.Printf("[LOGGER-ERROR] Fehler beim Schreiben des Batches für %s: %v", symbol, err)
	}
	
	csvWriter.Flush()
}

// flushAllOnShutdown leert alle verbleibenden Puffer beim Beenden.
func (fm *fileManager) flushAllOnShutdown() {
	fm.mu.Lock()
	defer fm.mu.Unlock()
	log.Println("[LOGGER] Flushe alle verbleibenden Puffer vor dem Herunterfahren...")
	
	var wg sync.WaitGroup
	for symbol, buffer := range fm.buffers {
		if len(buffer) > 0 {
			wg.Add(1)
			go func(sym string, buf [][]string) {
				defer wg.Done()
				fm.flushBuffer(sym, buf)
			}(symbol, buffer)
		}
	}
	wg.Wait()
	log.Println("[LOGGER] Alle Puffer erfolgreich geschrieben.")
}

// ======================================================================
// NEUE FUNKTIONEN FÜR DAS LÖSCHEN ALTER DATEIEN
// ======================================================================

// cleanupOldFiles durchsucht ein Verzeichnis und löscht alle .csv.gz-Dateien,
// die nicht das heutige UTC-Datum im Namen haben.
func cleanupOldFiles(dir string) {
	log.Printf("[CLEANUP] Suche nach alten Dateien im Verzeichnis: %s", dir)
	
	files, err := os.ReadDir(dir)
	if err != nil {
		log.Printf("[CLEANUP-ERROR] Konnte Verzeichnis nicht lesen: %v", err)
		return
	}

	todayStr := time.Now().UTC().Format("2006-01-02")
	filesDeleted := 0
	
	for _, file := range files {
		fileName := file.Name()
		if strings.HasSuffix(fileName, ".csv.gz") && !strings.Contains(fileName, todayStr) {
			filePath := filepath.Join(dir, fileName)
			log.Printf("[CLEANUP] Lösche alte Datei: %s", filePath)
			if err := os.Remove(filePath); err != nil {
				log.Printf("[CLEANUP-ERROR] Konnte Datei nicht löschen: %v", err)
			} else {
				filesDeleted++
			}
		}
	}
	if filesDeleted > 0 {
		log.Printf("[CLEANUP] %d alte Dateien gelöscht.", filesDeleted)
	} else {
		log.Printf("[CLEANUP] Keine alten Dateien gefunden.")
	}
}

// dailyCleanup startet eine Goroutine, die einmal täglich alte Dateien bereinigt.
func dailyCleanup(dir string) {
	// Führe den Cleanup einmal beim Start aus.
	cleanupOldFiles(dir)
	
	for {
		now := time.Now().UTC()
		// Nächster Lauf ist um 00:01 Uhr UTC am nächsten Tag
		nextRun := time.Date(now.Year(), now.Month(), now.Day(), 0, 1, 0, 0, time.UTC).Add(24 * time.Hour)
		log.Printf("[CLEANUP] Nächste Bereinigung geplant für: %s", nextRun)
		time.Sleep(time.Until(nextRun))
		cleanupOldFiles(dir)
	}
}
// ======================================================================

func main() {
	log.Println("[LOGGER] Starte Logger-Client...")

	fm := newFileManager("trades")
	if err := os.MkdirAll("trades", 0755); err != nil {
		log.Fatalf("Verzeichnis 'trades' konnte nicht erstellt werden: %v", err)
	}

	// Starte die neue tägliche Bereinigungs-Goroutine
	go dailyCleanup("trades")

	ctx := context.Background()
	socket := zmq4.NewDealer(ctx)
	defer socket.Close()

	if err := socket.Dial(brokerAddress); err != nil {
		log.Fatalf("Verbindung zum Broker fehlgeschlagen: %v", err)
	}
	log.Printf("[LOGGER] Verbunden mit Broker unter %s", brokerAddress)

	subCmd, _ := json.Marshal(map[string]string{
		"action":      "subscribe_all",
		"exchange":    "bybit",
		"market_type": "swap",
	})
	if err := socket.Send(zmq4.NewMsg(subCmd)); err != nil {
		log.Fatalf("Senden von subscribe_all fehlgeschlagen: %v", err)
	}

	go func() {
		for {
			msg, err := socket.Recv()
			if err != nil {
				log.Printf("Fehler beim Empfangen: %v", err)
				break
			}
			recvTime := time.Now()

			if len(msg.Frames) < 2 { continue }
			payload := msg.Frames[1]

			var pingCheck map[string]string
			if json.Unmarshal(payload, &pingCheck) == nil && pingCheck["type"] == "ping" {
				pongMsg, _ := json.Marshal(map[string]string{"message": "pong"})
				socket.Send(zmq4.NewMsg(pongMsg))
				continue
			}
			
			var trades []*shared_types.TradeUpdate
			if err := json.Unmarshal(payload, &trades); err != nil {
				continue
			}

			for _, trade := range trades {
				cleanSymbol := strings.Split(trade.Symbol, ":")[0]
				
				// KORREKTUR: Beide Latenzen berechnen
				e2e_latency_ms := float64(recvTime.UnixMilli() - trade.Timestamp)
				internal_latency_ms := float64(recvTime.UnixMilli() - trade.GoTimestamp)
				
				homeNotional := trade.Amount
				foreignNotional := trade.Amount * trade.Price

				// KORREKTUR: Record mit beiden Latenzen erstellen
				record := []string{
					fmt.Sprintf("%.4f", float64(trade.Timestamp)/1000.0),
					cleanSymbol,
					strings.Title(trade.Side),
					fmt.Sprintf("%.8f", trade.Amount),
					fmt.Sprintf("%.8f", trade.Price),
					"Unknown", trade.TradeID, "0.0",
					fmt.Sprintf("%.8f", homeNotional),
					fmt.Sprintf("%.8f", foreignNotional),
					fmt.Sprintf("%.3f", e2e_latency_ms),      // E2E Latenz
					fmt.Sprintf("%.3f", internal_latency_ms), // Interne Latenz
				}
				
				fm.addRecord(cleanSymbol, record)
			}
		}
	}()

	log.Println("[LOGGER] Logger läuft. Drücke CTRL+C zum Beenden.")
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)
	<-sc

	log.Println("[LOGGER] Beende...")
	fm.flushAllOnShutdown()
}