package coinex

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
	Method string      `json:"method"`
	Params interface{} `json:"params"`
	ID     int64       `json:"id"`
}

type wsError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type wsEnvelope struct {
	ID     *int64         `json:"id,omitempty"`
	Method string         `json:"method,omitempty"`
	Result interface{}    `json:"result,omitempty"`
	Error  *wsError       `json:"error,omitempty"`
	Data   *wsDealsUpdate `json:"data,omitempty"`
}

type wsDealsUpdate struct {
	Market   string   `json:"market"`
	DealList []wsDeal `json:"deal_list"`
}

type wsDeal struct {
	DealID    int64  `json:"deal_id"`
	CreatedAt int64  `json:"created_at"`
	Side      string `json:"side"`
	Price     string `json:"price"`
	Amount    string `json:"amount"`
}

type wsDepthEnvelope struct {
	ID     *int64         `json:"id,omitempty"`
	Method string         `json:"method,omitempty"`
	Result interface{}    `json:"result,omitempty"`
	Error  *wsError       `json:"error,omitempty"`
	Data   *wsDepthUpdate `json:"data,omitempty"`
}

type wsDepthUpdate struct {
	Market string       `json:"market"`
	IsFull bool         `json:"is_full"`
	Depth  wsDepthBooks `json:"depth"`
}

type wsDepthBooks struct {
	Asks      [][]string `json:"asks"`
	Bids      [][]string `json:"bids"`
	Last      string     `json:"last"`
	UpdatedAt int64      `json:"updated_at"`
	Checksum  any        `json:"checksum"`
}
