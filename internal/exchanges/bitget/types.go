package bitget

// wsSubArg ist die Struktur für ein einzelnes Abonnement-Argument.
type wsSubArg struct {
	InstType string `json:"instType"` // "SPOT" für Spot, "MC" für USDT-M Swaps
	Channel  string `json:"channel"`  // "trade"
	InstID   string `json:"instId"`   // z.B. "BTCUSDT"
}

// wsRequest ist die Hüllstruktur für eine Anfrage (z.B. subscribe) an den Bitget-Server.
type wsRequest struct {
	Op   string     `json:"op"`
	Args []wsSubArg `json:"args"`
}

// ======================================================================
// KORREKTUR: Neue Struktur für die eigentlichen Trade-Daten
// Dies spiegelt die Dokumentation wider: das 'data'-Feld ist ein Array von Objekten.
// ======================================================================
type wsTradeData struct {
	Timestamp string `json:"ts"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Side      string `json:"side"`
	TradeID   string `json:"tradeId"`
}

// ======================================================================
// KORREKTUR: Die wsMsg verwendet jetzt die neue wsTradeData-Struktur.
// ======================================================================
type wsMsg struct {
	Action string        `json:"action"`
	Arg    wsSubArg      `json:"arg"`
	Data   []wsTradeData `json:"data"` // Geändert von [][]string zu []wsTradeData
}
