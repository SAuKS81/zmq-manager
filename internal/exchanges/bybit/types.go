package bybit

// wsTrade ist die Struktur für einen einzelnen Trade in einer Bybit WS-Nachricht.
// Felder wurden angepasst, um float als string zu parsen.
type wsTrade struct {
	Timestamp int64  `json:"T"` // Millisekunden
	Symbol    string `json:"s"`
	Side      string `json:"S"` // "Buy" or "Sell"
	Volume    string `json:"v"` // Menge des Trades
	Price     string `json:"p"` // Preis des Trades
	TradeID   string `json:"i"` // NEU: Trade Match ID
}

// wsMsg ist die Hüllstruktur für Bybit WS-Nachrichten.
type wsMsg struct {
	Topic string    `json:"topic"`
	Type  string    `json:"type"`
	Data  []wsTrade `json:"data"`
}

// wsOrderBookData ist die 'data'-Struktur für Orderbuch-Nachrichten.
type wsOrderBookData struct {
	Bids     [][2]string `json:"b"` // Array von ["preis", "menge"]
	Asks     [][2]string `json:"a"` // Array von ["preis", "menge"]
	UpdateID int64       `json:"u"`
}

// wsOrderBookMsg ist die Hüllstruktur für Orderbuch-Nachrichten.
type wsOrderBookMsg struct {
	Topic     string          `json:"topic"`
	Type      string          `json:"type"`
	Timestamp int64           `json:"ts"` // <--- NEU: Das fehlte!
	Data      wsOrderBookData `json:"data"`
}
