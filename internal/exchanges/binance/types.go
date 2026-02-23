package binance

// HINWEIS: Kein Import von "json" nötig, da wir hier nur Struct-Tags verwenden.

type wsRequest struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     uint64   `json:"id"`
}

// KORREKTUR: Name angepasst an ob_shard_worker.go
// KORREKTUR: Direktes Struct für Data statt json.RawMessage
type wsOrderBookCombined struct {
	Stream string             `json:"stream"`
	Data   wsOrderBookPartial `json:"data"` 
}

type wsTrade struct {
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	TradeID      int64  `json:"t"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}

type wsOrderBookLevel []string

// Hier ist das Mapping entscheidend:
// Spot sendet "bids", Swap sendet "b". Wir brauchen beides.
type wsOrderBookPartial struct {
	EventType    string             `json:"e"`
	EventTime    int64              `json:"E"`
	Symbol       string             `json:"s"`
	LastUpdateID int64              `json:"lastUpdateId"`
	
	BidsSpot     []wsOrderBookLevel `json:"bids"` // Für Spot
	AsksSpot     []wsOrderBookLevel `json:"asks"` // Für Spot
	
	BidsFut      []wsOrderBookLevel `json:"b"`    // Für Futures
	AsksFut      []wsOrderBookLevel `json:"a"`    // Für Futures
}