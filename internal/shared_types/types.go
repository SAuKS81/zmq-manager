package shared_types

// OrderBookLevel repräsentiert einen einzelnen Eintrag (Preis, Menge).
type OrderBookLevel struct {
	Price  float64 `json:"price" msgpack:"p"`
	Amount float64 `json:"amount" msgpack:"a"`
}

// OrderBookUpdate ist die normalisierte Orderbuch-Struktur.
type OrderBookUpdate struct {
	Exchange    string           `json:"exchange" msgpack:"e"`
	Symbol      string           `json:"symbol" msgpack:"s"`
	MarketType  string           `json:"market_type" msgpack:"m"`
	Timestamp   int64            `json:"timestamp" msgpack:"t"`
	GoTimestamp int64            `json:"go_timestamp" msgpack:"gt"`
	Bids        []OrderBookLevel `json:"bids" msgpack:"b"`
	Asks        []OrderBookLevel `json:"asks" msgpack:"a"`
	DataType    string           `json:"data_type" msgpack:"dt"`
}

// TradeUpdate ist die normalisierte Trade-Struktur.
type TradeUpdate struct {
	Exchange    string  `json:"exchange" msgpack:"e"`
	Symbol      string  `json:"symbol" msgpack:"s"`
	MarketType  string  `json:"market_type" msgpack:"m"`
	Timestamp   int64   `json:"timestamp" msgpack:"t"`
	GoTimestamp int64   `json:"go_timestamp" msgpack:"gt"`
	Price       float64 `json:"price" msgpack:"p"`
	Amount      float64 `json:"amount" msgpack:"a"`
	Side        string  `json:"side" msgpack:"Si"`
	TradeID     string  `json:"trade_id" msgpack:"ti"`
	DataType    string  `json:"data_type" msgpack:"dt"`
}

// ClientRequest
type ClientRequest struct {
	ClientID       []byte `json:"-" msgpack:"-"` // Wird nicht gesendet
	Action         string `json:"action"`
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