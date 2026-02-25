package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"bybit-watcher/internal/broker"
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

	metrics.Init()
	http.Handle("/metrics", promhttp.Handler())

	go func() {
		log.Println("Starte pprof-Server auf http://localhost:6060")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	log.Println("Starte Bybit-Daten-Broker...")

	var clientMgr *broker.ClientManager
	subMgr := broker.NewSubscriptionManager(nil)
	clientMgr = broker.NewClientManager(subMgr.RequestCh)
	subMgr.DistributionCh = clientMgr.DistributionCh

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

		p1Len, p1Cap, p2Len, p2Cap := cm.SendQueueStats()
		metrics.SetQueueSample("router_send_p1", p1Len, p1Cap)
		metrics.SetQueueSample("router_send_p2_latest", p2Len, p2Cap)
	}
}
