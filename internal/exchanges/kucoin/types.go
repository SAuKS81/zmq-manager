package kucoin

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

type wsCommand struct {
	ID       string `json:"id,omitempty"`
	Type     string `json:"type,omitempty"`
	Topic    string `json:"topic,omitempty"`
	Response bool   `json:"response,omitempty"`
}

type wsEnvelope struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type,omitempty"`
	Topic   string          `json:"topic,omitempty"`
	Subject string          `json:"subject,omitempty"`
	Message string          `json:"message,omitempty"`
	Result  any             `json:"result,omitempty"`
	Code    json.Number     `json:"code,omitempty"`
	Data    *wsClassicTrade `json:"data,omitempty"`
}

type wsClassicTrade struct {
	Price        string `json:"price,omitempty"`
	Amount       string `json:"size,omitempty"`
	Symbol       string `json:"symbol,omitempty"`
	Side         string `json:"side,omitempty"`
	TradeID      string `json:"tradeId,omitempty"`
	Time         string `json:"time,omitempty"`
	Sequence     string `json:"sequence,omitempty"`
	MakerOrderID string `json:"makerOrderId,omitempty"`
	TakerOrderID string `json:"takerOrderId,omitempty"`
}

type tokenResponse struct {
	Code string            `json:"code"`
	Data tokenResponseData `json:"data"`
}

type tokenResponseData struct {
	Token           string                `json:"token"`
	InstanceServers []tokenInstanceServer `json:"instanceServers"`
}

type tokenInstanceServer struct {
	Endpoint     string `json:"endpoint"`
	PingInterval int64  `json:"pingInterval"`
	PingTimeout  int64  `json:"pingTimeout"`
}

type wsOrderBookEnvelope struct {
	ID      string              `json:"id,omitempty"`
	Type    string              `json:"type,omitempty"`
	Topic   string              `json:"topic,omitempty"`
	Subject string              `json:"subject,omitempty"`
	Result  any                 `json:"result,omitempty"`
	Code    json.Number         `json:"code,omitempty"`
	Data    *wsOrderBookPayload `json:"data,omitempty"`
}

type wsOrderBookPayload struct {
	Asks      json.RawMessage `json:"asks,omitempty"`
	Bids      json.RawMessage `json:"bids,omitempty"`
	Timestamp int64           `json:"timestamp,omitempty"`
}
