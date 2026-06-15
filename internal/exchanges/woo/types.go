package woo

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
	ID    string `json:"id"`
	Topic string `json:"topic"`
	Event string `json:"event"`
}

type wsEnvelope struct {
	ID      string          `json:"id"`
	Event   string          `json:"event"`
	Success *bool           `json:"success"`
	TS      int64           `json:"ts"`
	Topic   string          `json:"topic"`
	Data    json.RawMessage `json:"data"`
	Message string          `json:"message"`
	Code    interface{}     `json:"code"`
}

type trade struct {
	Symbol     string      `json:"symbol"`
	Price      interface{} `json:"price"`
	Size       interface{} `json:"size"`
	Side       string      `json:"side"`
	TradeID    interface{} `json:"id"`
	TradeID2   interface{} `json:"trade_id"`
	Timestamp  interface{} `json:"ts"`
	Timestamp2 interface{} `json:"timestamp"`
}
