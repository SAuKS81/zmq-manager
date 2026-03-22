package main

import (
	"flag"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"bybit-watcher/internal/broker"
	"bybit-watcher/internal/loghub"
	"bybit-watcher/internal/metrics"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	_ "net/http/pprof"
)

func main() {
	pprofBlockRate := flag.Int("pprof-block-rate", 0, "Go block profile rate (0=off)")
	pprofMutexFrac := flag.Int("pprof-mutex-fraction", 0, "Go mutex profile fraction (0=off)")
	flag.Parse()

	if *pprofBlockRate > 0 {
		runtime.SetBlockProfileRate(*pprofBlockRate)
	}
	if *pprofMutexFrac > 0 {
		runtime.SetMutexProfileFraction(*pprofMutexFrac)
	}

	logCloser, err := configureLogging()
	if err != nil {
		log.Printf("[LOGHUB] zentrale Log-Spiegelung deaktiviert: %v", err)
	}
	if logCloser != nil {
		defer logCloser.Close()
	}

	metrics.Init()
	http.Handle("/metrics", promhttp.Handler())

	go func() {
		log.Println("Starte pprof-Server auf http://localhost:6060")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	log.Println("Starte Marktdaten-Broker...")
	if broker.CCXTBuildEnabled() {
		log.Println("[BROKER] CCXT-Support ist in diesem Build aktiviert.")
	} else {
		log.Println("[BROKER] CCXT-Support ist in diesem Build deaktiviert. Nur native Handler stehen zur Verfuegung.")
	}

	var clientMgr *broker.ClientManager
	subMgr := broker.NewSubscriptionManager(nil)
	clientMgr = broker.NewClientManager(subMgr.RequestCh)
	subMgr.DistributionCh = clientMgr.DistributionCh
	clientMgr.StatusCh = subMgr.StatusCh

	go subMgr.Run()
	go clientMgr.Run()
	go sampleQueueMetrics(subMgr, clientMgr)

	log.Println("Broker laeuft. Druecke CTRL+C zum Beenden.")

	sc := make(chan os.Signal, 1)
	signal.Notify(sc, syscall.SIGINT, syscall.SIGTERM)
	<-sc

	log.Println("Beende Broker...")
}

func sampleQueueMetrics(sm *broker.SubscriptionManager, cm *broker.ClientManager) {
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		metrics.SetQueueSample("trade_ingest_to_broker", len(sm.TradeDataCh), cap(sm.TradeDataCh))
		metrics.SetQueueSample("ob_ingest_to_broker", len(sm.OrderBookCh), cap(sm.OrderBookCh))
		metrics.SetQueueSample("broker_request", len(sm.RequestCh), cap(sm.RequestCh))
		metrics.SetQueueSample("broker_to_distribution", len(cm.DistributionCh), cap(cm.DistributionCh))

		p1Len, p1Cap := cm.SendQueueStats()
		metrics.SetQueueSample("router_send_p1", p1Len, p1Cap)
	}
}

func configureLogging() (io.Closer, error) {
	writer, err := loghub.NewFromEnv()
	if err != nil || writer == nil {
		return writer, err
	}

	// Wenn der Central Logger aktiv ist, schreiben wir exklusiv dorthin,
	// um doppelte Writes auf stdout/stderr und Loghub zu vermeiden.
	log.SetOutput(writer)
	return writer, nil
}
