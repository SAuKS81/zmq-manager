package bitmart

type ManagerCommand struct {
	Action string
	Symbol string
	Depth  int
	Mode   string
}

type ShardCommand struct {
	Action  string
	Symbols []string
	Depth   int
	Mode    string
}

type wsSubscribeRequest struct {
	Op   string   `json:"op"`
	Args []string `json:"args"`
}

type wsEnvelope struct {
	Table string           `json:"table"`
	Data  []wsSpotTrade    `json:"data"`
	Error *wsErrorEnvelope `json:"error,omitempty"`
	Event string           `json:"event,omitempty"`
}

type wsOrderBookEnvelope struct {
	Table string             `json:"table"`
	Data  []wsOrderBookEntry `json:"data"`
	Error *wsErrorEnvelope   `json:"error,omitempty"`
	Event string             `json:"event,omitempty"`
}

type wsErrorEnvelope struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type wsSpotTrade struct {
	Price  string `json:"price"`
	ST     int64  `json:"s_t"`
	MST    int64  `json:"ms_t"`
	Side   string `json:"side"`
	Size   string `json:"size"`
	Symbol string `json:"symbol"`
}

type wsOrderBookEntry struct {
	Asks    [][]string `json:"asks"`
	Bids    [][]string `json:"bids"`
	MST     int64      `json:"ms_t"`
	Symbol  string     `json:"symbol"`
	Type    string     `json:"type,omitempty"`
	Version int64      `json:"version,omitempty"`
}
