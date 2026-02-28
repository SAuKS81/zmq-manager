package bybit

type wsTrade struct {
	Timestamp int64  `json:"T"`
	Symbol    string `json:"s"`
	Side      string `json:"S"`
	Volume    string `json:"v"`
	Price     string `json:"p"`
	TradeID   string `json:"i"`
}

type wsMsg struct {
	Topic string    `json:"topic"`
	Type  string    `json:"type"`
	Data  []wsTrade `json:"data"`
}

type wsOrderBookData struct {
	Symbol   string      `json:"s"`
	Bids     [][2]string `json:"b"`
	Asks     [][2]string `json:"a"`
	UpdateID int64       `json:"u"`
}

type wsOrderBookMsg struct {
	Topic     string          `json:"topic"`
	Type      string          `json:"type"`
	Timestamp int64           `json:"ts"`
	Data      wsOrderBookData `json:"data"`
}

type wsCommandResponse struct {
	Success bool   `json:"success"`
	RetMsg  string `json:"ret_msg"`
	ConnID  string `json:"conn_id"`
	Op      string `json:"op"`
	ReqID   string `json:"req_id"`
}
