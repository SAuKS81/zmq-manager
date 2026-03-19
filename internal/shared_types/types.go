package shared_types

// OrderBookLevel repräsentiert einen einzelnen Eintrag (Preis, Menge).
type OrderBookLevel struct {
	Price  float64 `json:"price" msgpack:"p"`
	Amount float64 `json:"amount" msgpack:"a"`
}

// OrderBookUpdate ist die normalisierte Orderbuch-Struktur.
type OrderBookUpdate struct {
	Exchange       string           `json:"exchange" msgpack:"e"`
	Symbol         string           `json:"symbol" msgpack:"s"`
	MarketType     string           `json:"market_type" msgpack:"m"`
	Timestamp      int64            `json:"timestamp" msgpack:"t"`
	GoTimestamp    int64            `json:"go_timestamp" msgpack:"gt"`
	IngestUnixNano int64            `json:"-" msgpack:"-"`
	UpdateType     string           `json:"-" msgpack:"-"`
	Bids           []OrderBookLevel `json:"bids" msgpack:"b"`
	Asks           []OrderBookLevel `json:"asks" msgpack:"a"`
	DataType       string           `json:"data_type" msgpack:"dt"`
}

// TradeUpdate ist die normalisierte Trade-Struktur.
type TradeUpdate struct {
	Exchange       string  `json:"exchange" msgpack:"e"`
	Symbol         string  `json:"symbol" msgpack:"s"`
	MarketType     string  `json:"market_type" msgpack:"m"`
	Timestamp      int64   `json:"timestamp" msgpack:"t"`
	GoTimestamp    int64   `json:"go_timestamp" msgpack:"gt"`
	IngestUnixNano int64   `json:"-" msgpack:"-"`
	Price          float64 `json:"price" msgpack:"p"`
	Amount         float64 `json:"amount" msgpack:"a"`
	Side           string  `json:"side" msgpack:"Si"`
	TradeID        string  `json:"trade_id" msgpack:"ti"`
	DataType       string  `json:"data_type" msgpack:"dt"`
}

// ClientRequest
type ClientRequest struct {
	ClientID       []byte `json:"-" msgpack:"-"` // Wird nicht gesendet
	Action         string `json:"action"`
	Scope          string `json:"scope,omitempty"`
	RequestID      string `json:"request_id,omitempty"`
	Sticky         bool   `json:"sticky,omitempty"`
	Exchange       string `json:"exchange"`
	Symbol         string `json:"symbol"`
	MarketType     string `json:"market_type"`
	DataType       string `json:"data_type"`
	Encoding       string `json:"encoding,omitempty"`
	CacheN         int    `json:"cache_n,omitempty"`
	OrderBookDepth int    `json:"depth,omitempty"`
	OrderBookFreq  string `json:"frequency,omitempty"`
	PushIntervalMS int    `json:"push_interval_ms,omitempty"`
	BatchSent      int    `json:"-"`
}

// BulkClientRequest
type BulkClientRequest struct {
	Action         string   `json:"action"`
	RequestID      string   `json:"request_id,omitempty"`
	Sticky         bool     `json:"sticky,omitempty"`
	Exchange       string   `json:"exchange"`
	Symbols        []string `json:"symbols"`
	MarketType     string   `json:"market_type"`
	DataType       string   `json:"data_type"`
	Encoding       string   `json:"encoding,omitempty"`
	CacheN         int      `json:"cache_n,omitempty"`
	OrderBookDepth int      `json:"depth,omitempty"`
	OrderBookFreq  string   `json:"frequency,omitempty"`
	PushIntervalMS int      `json:"push_interval_ms,omitempty"`
}

type CapabilityParameter struct {
	Type          string `json:"type"`
	Required      bool   `json:"required"`
	Default       any    `json:"default,omitempty"`
	Min           any    `json:"min,omitempty"`
	Max           any    `json:"max,omitempty"`
	Step          any    `json:"step,omitempty"`
	AllowedValues []int  `json:"allowed_values,omitempty"`
}

type ChannelCapability struct {
	Subscribe         bool                           `json:"subscribe"`
	Unsubscribe       bool                           `json:"unsubscribe"`
	BulkSubscribe     bool                           `json:"bulk_subscribe"`
	BulkUnsubscribe   bool                           `json:"bulk_unsubscribe"`
	SupportsRequestID bool                           `json:"supports_request_id"`
	Parameters        map[string]CapabilityParameter `json:"parameters,omitempty"`
}

type StreamStatusEvent struct {
	Type       string   `json:"type"`
	Exchange   string   `json:"exchange,omitempty"`
	MarketType string   `json:"market_type,omitempty"`
	DataType   string   `json:"data_type,omitempty"`
	Symbol     string   `json:"symbol,omitempty"`
	Symbols    []string `json:"symbols,omitempty"`
	Adapter    string   `json:"adapter,omitempty"`
	Reason     string   `json:"reason,omitempty"`
	Status     string   `json:"status,omitempty"`
	RequestID  string   `json:"request_id,omitempty"`
	Attempt    int      `json:"attempt,omitempty"`
	Message    string   `json:"message,omitempty"`
	Timestamp  int64    `json:"ts,omitempty"`
}

type RuntimeSubscriptionItem struct {
	Exchange   string `json:"exchange"`
	MarketType string `json:"market_type"`
	Symbol     string `json:"symbol"`
	DataType   string `json:"data_type"`
	Adapter    string `json:"adapter"`
	Encoding   string `json:"encoding,omitempty"`
	CacheN     int    `json:"cache_n,omitempty"`
	Depth      int    `json:"depth,omitempty"`
	Sticky     bool   `json:"sticky"`
	Running    bool   `json:"running"`
	Owners     int    `json:"owners"`
	Clients    int    `json:"clients"`
}

type SubscriptionHealthItem struct {
	Exchange         string  `json:"exchange"`
	MarketType       string  `json:"market_type"`
	Symbol           string  `json:"symbol"`
	DataType         string  `json:"data_type"`
	Status           string  `json:"status"`
	LastMessageAgeMS int64   `json:"last_message_age_ms"`
	LastMessageTS    int64   `json:"last_message_ts,omitempty"`
	Reconnects1H     int     `json:"reconnects_1h"`
	MessagesPerSec   float64 `json:"messages_per_sec"`
	LatencyMS        float64 `json:"latency_ms,omitempty"`
	BrokerLatencyMS  float64 `json:"broker_latency_ms,omitempty"`
	LastError        string  `json:"last_error,omitempty"`
	LastErrorTS      int64   `json:"last_error_ts,omitempty"`
	LastReconnectTS  int64   `json:"last_reconnect_ts,omitempty"`
	StaleThresholdMS int64   `json:"stale_threshold_ms"`
	SampleWindowSec  int     `json:"sample_window_sec"`
}

type RuntimeSnapshotTotals struct {
	ActiveSubscriptions int     `json:"active_subscriptions"`
	MessagesPerSec      float64 `json:"messages_per_sec"`
	Reconnects24H       int     `json:"reconnects_24h"`
}

type SubscriptionsSnapshotResponse struct {
	Type      string                    `json:"type"`
	Scope     string                    `json:"scope"`
	RequestID string                    `json:"request_id,omitempty"`
	TS        int64                     `json:"ts"`
	Items     []RuntimeSubscriptionItem `json:"items"`
}

type SubscriptionHealthSnapshotResponse struct {
	Type      string                   `json:"type"`
	RequestID string                   `json:"request_id,omitempty"`
	TS        int64                    `json:"ts"`
	Items     []SubscriptionHealthItem `json:"items"`
}

type RuntimeSnapshotResponse struct {
	Type          string                    `json:"type"`
	RequestID     string                    `json:"request_id,omitempty"`
	TS            int64                     `json:"ts"`
	Subscriptions []RuntimeSubscriptionItem `json:"subscriptions"`
	Health        []SubscriptionHealthItem  `json:"health"`
	Totals        RuntimeSnapshotTotals     `json:"totals"`
}

type CapabilitiesItem struct {
	Exchange                      string                                  `json:"exchange"`
	ManagerExchange               string                                  `json:"manager_exchange"`
	Adapter                       string                                  `json:"adapter"`
	MarketTypes                   []string                                `json:"market_types"`
	DataTypes                     []string                                `json:"data_types"`
	Channels                      map[string]map[string]ChannelCapability `json:"channels,omitempty"`
	OrderBookDepths               []int                                   `json:"orderbook_depths,omitempty"`
	UsesBatchSymbols              bool                                    `json:"uses_batch_symbols,omitempty"`
	SupportsTradeUnwatch          bool                                    `json:"supports_trade_unwatch,omitempty"`
	SupportsTradeBatchUnwatch     bool                                    `json:"supports_trade_batch_unwatch,omitempty"`
	SupportsOrderBookUnwatch      bool                                    `json:"supports_orderbook_unwatch,omitempty"`
	SupportsOrderBookBatchUnwatch bool                                    `json:"supports_orderbook_batch_unwatch,omitempty"`
	SupportsCacheN                bool                                    `json:"supports_cache_n"`
	SupportsRequestID             bool                                    `json:"supports_request_id"`
	SupportsDeployQueue           bool                                    `json:"supports_deploy_queue"`
}

type CapabilitiesSnapshotResponse struct {
	Type      string             `json:"type"`
	RequestID string             `json:"request_id,omitempty"`
	TS        int64              `json:"ts"`
	Items     []CapabilitiesItem `json:"items"`
}

type DeployBatchSummaryEvent struct {
	Type      string `json:"type"`
	RequestID string `json:"request_id"`
	TS        int64  `json:"ts"`
	Sent      int    `json:"sent"`
	Acked     int    `json:"acked"`
	Failed    int    `json:"failed"`
}

type RuntimeTotalsTickEvent struct {
	Type                string  `json:"type"`
	TS                  int64   `json:"ts"`
	ActiveSubscriptions int     `json:"active_subscriptions"`
	MessagesPerSec      float64 `json:"messages_per_sec"`
	Reconnects24H       int     `json:"reconnects_24h"`
}

type ErrorResponse struct {
	Type      string `json:"type"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id,omitempty"`
}
