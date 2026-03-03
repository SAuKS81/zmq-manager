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
	Exchange       string `json:"exchange"`
	Symbol         string `json:"symbol"`
	MarketType     string `json:"market_type"`
	DataType       string `json:"data_type"`
	OrderBookDepth int    `json:"depth,omitempty"`
}

// BulkClientRequest
type BulkClientRequest struct {
	Action         string   `json:"action"`
	Exchange       string   `json:"exchange"`
	Symbols        []string `json:"symbols"`
	MarketType     string   `json:"market_type"`
	DataType       string   `json:"data_type"`
	OrderBookDepth int      `json:"depth,omitempty"`
}

type StreamStatusEvent struct {
	Type       string   `json:"type"`
	Exchange   string   `json:"exchange,omitempty"`
	MarketType string   `json:"market_type,omitempty"`
	DataType   string   `json:"data_type,omitempty"`
	Symbol     string   `json:"symbol,omitempty"`
	Symbols    []string `json:"symbols,omitempty"`
	Reason     string   `json:"reason,omitempty"`
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
	Depth      int    `json:"depth,omitempty"`
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
}

type RuntimeSnapshotTotals struct {
	ActiveSubscriptions int     `json:"active_subscriptions"`
	MessagesPerSec      float64 `json:"messages_per_sec"`
	Reconnects24H       int     `json:"reconnects_24h"`
}

type SubscriptionsSnapshotResponse struct {
	Type  string                    `json:"type"`
	Scope string                    `json:"scope"`
	TS    int64                     `json:"ts"`
	Items []RuntimeSubscriptionItem `json:"items"`
}

type SubscriptionHealthSnapshotResponse struct {
	Type  string                   `json:"type"`
	TS    int64                    `json:"ts"`
	Items []SubscriptionHealthItem `json:"items"`
}

type RuntimeSnapshotResponse struct {
	Type          string                    `json:"type"`
	TS            int64                     `json:"ts"`
	Subscriptions []RuntimeSubscriptionItem `json:"subscriptions"`
	Health        []SubscriptionHealthItem  `json:"health"`
	Totals        RuntimeSnapshotTotals     `json:"totals"`
}
