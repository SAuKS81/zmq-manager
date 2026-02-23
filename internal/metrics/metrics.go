package metrics

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	TypeTrade      = "trade"
	TypeOBUpdate   = "ob_update"
	TypeOBSnapshot = "ob_snapshot"

	ReasonSlowClient  = "slow_client"
	ReasonBufferFull  = "buffer_full"
	ReasonParseError  = "parse_error"
	ReasonStaleSeq    = "stale_sequence"
	ReasonInternalErr = "internal_error"

	ClientTierInternal = "internal"
)

var (
	registerOnce sync.Once

	ingestMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zmq_ingest_messages_total",
			Help: "Total ingest messages by exchange and type.",
		},
		[]string{"exchange", "type"},
	)

	publishMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zmq_publish_messages_total",
			Help: "Total published messages by type and client tier.",
		},
		[]string{"type", "client_tier"},
	)

	droppedMessagesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zmq_dropped_messages_total",
			Help: "Total dropped messages by reason and type.",
		},
		[]string{"reason", "type"},
	)

	processingDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "zmq_processing_duration_seconds",
			Help:    "Duration from ingest timestamp to pre-send timestamp.",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.05},
		},
		[]string{"type"},
	)

	publishTradeInternal      prometheus.Counter
	publishOBUpdateInternal   prometheus.Counter
	publishOBSnapshotInternal prometheus.Counter

	droppedByReasonType map[string]map[string]prometheus.Counter
	observeByType       map[string]prometheus.Observer

	ingestMu       sync.RWMutex
	ingestByLabels map[ingestKey]prometheus.Counter
)

type ingestKey struct {
	exchange string
	typeName string
}

func Init() {
	registerOnce.Do(func() {
		prometheus.MustRegister(ingestMessagesTotal)
		prometheus.MustRegister(publishMessagesTotal)
		prometheus.MustRegister(droppedMessagesTotal)
		prometheus.MustRegister(processingDuration)

		publishTradeInternal = publishMessagesTotal.WithLabelValues(TypeTrade, ClientTierInternal)
		publishOBUpdateInternal = publishMessagesTotal.WithLabelValues(TypeOBUpdate, ClientTierInternal)
		publishOBSnapshotInternal = publishMessagesTotal.WithLabelValues(TypeOBSnapshot, ClientTierInternal)

		droppedByReasonType = map[string]map[string]prometheus.Counter{
			ReasonSlowClient: {
				TypeTrade:      droppedMessagesTotal.WithLabelValues(ReasonSlowClient, TypeTrade),
				TypeOBUpdate:   droppedMessagesTotal.WithLabelValues(ReasonSlowClient, TypeOBUpdate),
				TypeOBSnapshot: droppedMessagesTotal.WithLabelValues(ReasonSlowClient, TypeOBSnapshot),
			},
			ReasonBufferFull: {
				TypeTrade:      droppedMessagesTotal.WithLabelValues(ReasonBufferFull, TypeTrade),
				TypeOBUpdate:   droppedMessagesTotal.WithLabelValues(ReasonBufferFull, TypeOBUpdate),
				TypeOBSnapshot: droppedMessagesTotal.WithLabelValues(ReasonBufferFull, TypeOBSnapshot),
			},
			ReasonParseError: {
				TypeTrade:      droppedMessagesTotal.WithLabelValues(ReasonParseError, TypeTrade),
				TypeOBUpdate:   droppedMessagesTotal.WithLabelValues(ReasonParseError, TypeOBUpdate),
				TypeOBSnapshot: droppedMessagesTotal.WithLabelValues(ReasonParseError, TypeOBSnapshot),
			},
			ReasonStaleSeq: {
				TypeTrade:      droppedMessagesTotal.WithLabelValues(ReasonStaleSeq, TypeTrade),
				TypeOBUpdate:   droppedMessagesTotal.WithLabelValues(ReasonStaleSeq, TypeOBUpdate),
				TypeOBSnapshot: droppedMessagesTotal.WithLabelValues(ReasonStaleSeq, TypeOBSnapshot),
			},
			ReasonInternalErr: {
				TypeTrade:      droppedMessagesTotal.WithLabelValues(ReasonInternalErr, TypeTrade),
				TypeOBUpdate:   droppedMessagesTotal.WithLabelValues(ReasonInternalErr, TypeOBUpdate),
				TypeOBSnapshot: droppedMessagesTotal.WithLabelValues(ReasonInternalErr, TypeOBSnapshot),
			},
		}

		observeByType = map[string]prometheus.Observer{
			TypeTrade:    processingDuration.WithLabelValues(TypeTrade),
			TypeOBUpdate: processingDuration.WithLabelValues(TypeOBUpdate),
		}

		ingestByLabels = make(map[ingestKey]prometheus.Counter, 16)
	})
}

func RecordIngest(exchange, typeName string) {
	Init()
	t := normalizeType(typeName)
	key := ingestKey{exchange: exchange, typeName: t}

	ingestMu.RLock()
	ctr, ok := ingestByLabels[key]
	ingestMu.RUnlock()
	if ok {
		ctr.Inc()
		return
	}

	ingestMu.Lock()
	ctr, ok = ingestByLabels[key]
	if !ok {
		ctr = ingestMessagesTotal.WithLabelValues(exchange, t)
		ingestByLabels[key] = ctr
	}
	ingestMu.Unlock()
	ctr.Inc()
}

func RecordPublish(typeName string) {
	Init()
	switch normalizeType(typeName) {
	case TypeTrade:
		publishTradeInternal.Inc()
	case TypeOBSnapshot:
		publishOBSnapshotInternal.Inc()
	default:
		publishOBUpdateInternal.Inc()
	}
}

func RecordDropped(reason, typeName string) {
	Init()
	r := normalizeReason(reason)
	t := normalizeType(typeName)
	if byType, ok := droppedByReasonType[r]; ok {
		if ctr, ok := byType[t]; ok {
			ctr.Inc()
		}
	}
}

func ObserveProcessing(typeName string, seconds float64) {
	Init()
	t := normalizeProcessingType(typeName)
	if obs, ok := observeByType[t]; ok {
		obs.Observe(seconds)
	}
}

func normalizeType(typeName string) string {
	switch typeName {
	case TypeTrade:
		return TypeTrade
	case TypeOBSnapshot:
		return TypeOBSnapshot
	case TypeOBUpdate:
		fallthrough
	default:
		return TypeOBUpdate
	}
}

func normalizeProcessingType(typeName string) string {
	if typeName == TypeTrade {
		return TypeTrade
	}
	return TypeOBUpdate
}

func normalizeReason(reason string) string {
	switch reason {
	case ReasonSlowClient:
		return ReasonSlowClient
	case ReasonBufferFull:
		return ReasonBufferFull
	case ReasonParseError:
		return ReasonParseError
	case ReasonStaleSeq:
		return ReasonStaleSeq
	case ReasonInternalErr:
		fallthrough
	default:
		return ReasonInternalErr
	}
}
