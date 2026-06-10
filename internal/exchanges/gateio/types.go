package gateio

import "encoding/json"

type managerCommand struct {
	Action string
	Symbol string
}

type shardCommand struct {
	Action  string
	Symbols []string
}

type wsRequest struct {
	Time    int64    `json:"time"`
	ID      int64    `json:"id,omitempty"`
	Channel string   `json:"channel"`
	Event   string   `json:"event"`
	Payload []string `json:"payload"`
}

type wsEnvelope struct {
	Time    int64           `json:"time"`
	TimeMS  int64           `json:"time_ms"`
	ID      int64           `json:"id,omitempty"`
	Channel string          `json:"channel"`
	Event   string          `json:"event"`
	Error   *wsError        `json:"error"`
	Result  json.RawMessage `json:"result"`
}

type wsError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type spotTrade struct {
	ID           json.Number `json:"id"`
	CreateTime   int64       `json:"create_time"`
	CreateTimeMS interface{} `json:"create_time_ms"`
	Side         string      `json:"side"`
	CurrencyPair string      `json:"currency_pair"`
	Amount       string      `json:"amount"`
	Price        string      `json:"price"`
}

type futuresTrade struct {
	ID           json.Number `json:"id"`
	CreateTime   int64       `json:"create_time"`
	CreateTimeMS interface{} `json:"create_time_ms"`
	Price        string      `json:"price"`
	Size         int64       `json:"size"`
	Contract     string      `json:"contract"`
}
