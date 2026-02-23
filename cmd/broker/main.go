package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"bybit-watcher/internal/broker"
	"bybit-watcher/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"net/http"
	_ "net/http/pprof" // Der _ import registriert die pprof-Handler
)

func main() {
	metrics.Init()
	http.Handle("/metrics", promhttp.Handler())

	go func() {
		log.Println("Starte pprof-Server auf http://localhost:6060")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	log.Println("Starte Bybit-Daten-Broker...")

	// 1. Erstelle den SubscriptionManager. Er benötigt einen Kanal,
	//    um Daten an den ClientManager zu senden.
	// Wir holen uns diesen Kanal vom ClientManager, nachdem er erstellt wurde.
	var clientMgr *broker.ClientManager
	subMgr := broker.NewSubscriptionManager(nil) // Temporär nil

	// 2. Erstelle den ClientManager. Er benötigt einen Kanal,
	//    um Anfragen an den SubscriptionManager zu senden.
	clientMgr = broker.NewClientManager(subMgr.RequestCh)

	// 3. Verbinde die beiden Manager, indem wir die Kanäle zuweisen.
	subMgr.DistributionCh = clientMgr.DistributionCh

	// 4. Starte die Hauptschleifen der Manager in separaten Goroutinen.
	go subMgr.Run()
	go clientMgr.Run()

	log.Println("Broker läuft. Drücke CTRL+C zum Beenden.")

	// Warte auf ein Beendigungssignal (z.B. Strg+C)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)
	<-sc

	log.Println("Beende Broker...")
	// In einer echten Anwendung gäbe es hier noch eine saubere Shutdown-Logik.
}
