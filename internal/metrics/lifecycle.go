package metrics

import (
	"log"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	unsubscribeAttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zmq_unsubscribe_attempts_total",
			Help: "Total unsubscribe attempts by exchange, market type, and data type.",
		},
		[]string{"exchange", "market_type", "data_type"},
	)

	unsubscribeFailuresTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zmq_unsubscribe_failures_total",
			Help: "Total unsubscribe failures by exchange, market type, data type, and reason.",
		},
		[]string{"exchange", "market_type", "data_type", "reason"},
	)

	forcedShardRecyclesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zmq_forced_shard_recycles_total",
			Help: "Total forced shard recycles by exchange, market type, data type, and reason.",
		},
		[]string{"exchange", "market_type", "data_type", "reason"},
	)

	streamReconnectsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zmq_stream_reconnects_total",
			Help: "Total stream reconnect events by exchange, market type, data type, and reason.",
		},
		[]string{"exchange", "market_type", "data_type", "reason"},
	)

	streamRestoreSuccessTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zmq_stream_restore_success_total",
			Help: "Total successful stream restore events by exchange, market type, and data type.",
		},
		[]string{"exchange", "market_type", "data_type"},
	)
)

func RecordUnsubscribeAttempt(exchange, marketType, dataType string, count int) {
	Init()
	if count <= 0 {
		return
	}
	unsubscribeAttemptsTotal.WithLabelValues(normalizeLifecycleLabel(exchange), normalizeLifecycleLabel(marketType), normalizeLifecycleLabel(dataType)).Add(float64(count))
}

func RecordUnsubscribeFailure(exchange, marketType, dataType, reason string) {
	Init()
	unsubscribeFailuresTotal.WithLabelValues(
		normalizeLifecycleLabel(exchange),
		normalizeLifecycleLabel(marketType),
		normalizeLifecycleLabel(dataType),
		normalizeLifecycleLabel(reason),
	).Inc()
}

func RecordForcedShardRecycle(exchange, marketType, dataType, reason string) {
	Init()
	forcedShardRecyclesTotal.WithLabelValues(
		normalizeLifecycleLabel(exchange),
		normalizeLifecycleLabel(marketType),
		normalizeLifecycleLabel(dataType),
		normalizeLifecycleLabel(reason),
	).Inc()
}

func RecordStreamReconnect(exchange, marketType, dataType, reason string) {
	Init()
	streamReconnectsTotal.WithLabelValues(
		normalizeLifecycleLabel(exchange),
		normalizeLifecycleLabel(marketType),
		normalizeLifecycleLabel(dataType),
		normalizeLifecycleLabel(reason),
	).Inc()
}

func RecordStreamRestoreSuccess(exchange, marketType, dataType string) {
	Init()
	streamRestoreSuccessTotal.WithLabelValues(
		normalizeLifecycleLabel(exchange),
		normalizeLifecycleLabel(marketType),
		normalizeLifecycleLabel(dataType),
	).Inc()
}

func LogStreamLifecycle(eventType, exchange, shard, marketType, dataType string, symbols []string, attempt int, reason, message string) {
	log.Printf(
		"STREAM_LIFECYCLE event=%s exchange=%s shard=%s market_type=%s data_type=%s symbols=%q attempt=%d reason=%q message=%q",
		normalizeLifecycleLabel(eventType),
		normalizeLifecycleLabel(exchange),
		normalizeLifecycleLabel(shard),
		normalizeLifecycleLabel(marketType),
		normalizeLifecycleLabel(dataType),
		strings.Join(symbols, ","),
		attempt,
		reason,
		message,
	)
}

func normalizeLifecycleLabel(v string) string {
	if v == "" {
		return "unknown"
	}
	return v
}
