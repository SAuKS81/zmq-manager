package htx

import "encoding/json"

type ManagerCommand struct {
	Action string
	Symbol string
	Depth  int
}

type ShardCommand struct {
	Action  string
	Symbols []string
	Depth   int
}

type socketMessage struct {
	messageType int
	payload     []byte
}

type spotCommand struct {
	Sub      string `json:"sub,omitempty"`
	Unsub    string `json:"unsub,omitempty"`
	Req      string `json:"req,omitempty"`
	ID       string `json:"id,omitempty"`
	DataType string `json:"data_type,omitempty"`
}

type pongMessage struct {
	Pong int64 `json:"pong"`
}

type wsEnvelope struct {
	Ping   int64       `json:"ping,omitempty"`
	Status string      `json:"status,omitempty"`
	Subbed string      `json:"subbed,omitempty"`
	Unsub  string      `json:"unsubbed,omitempty"`
	Rep    string      `json:"rep,omitempty"`
	ID     json.Number `json:"id,omitempty"`
	Ch     string      `json:"ch,omitempty"`
	Ts     int64       `json:"ts,omitempty"`
	Tick   *wsTick     `json:"tick,omitempty"`
	Data   *wsTick     `json:"data,omitempty"`
}

type wsTick struct {
	ID         int64           `json:"id,omitempty"`
	Ts         int64           `json:"ts,omitempty"`
	Data       []wsTrade       `json:"data,omitempty"`
	Bids       [][]json.Number `json:"bids,omitempty"`
	Asks       [][]json.Number `json:"asks,omitempty"`
	SeqNum     int64           `json:"seqNum,omitempty"`
	PrevSeqNum int64           `json:"prevSeqNum,omitempty"`
	Version    int64           `json:"version,omitempty"`
	Event      string          `json:"event,omitempty"`
	Mrid       int64           `json:"mrid,omitempty"`
	Ch         string          `json:"ch,omitempty"`
}

type wsTrade struct {
	ID        json.Number `json:"id,omitempty"`
	TradeID   json.Number `json:"tradeId,omitempty"`
	Amount    json.Number `json:"amount,omitempty"`
	Price     json.Number `json:"price,omitempty"`
	Direction string      `json:"direction,omitempty"`
	Ts        int64       `json:"ts,omitempty"`
}
