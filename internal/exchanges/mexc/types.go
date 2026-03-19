package mexc

type ManagerCommand struct {
	Action string
	Symbol string
	Depth  int
	Freq   string
}

type ShardCommand struct {
	Action  string
	Symbols []string
	Depth   int
	Freq    string
}

type wsCommand struct {
	Method string   `json:"method"`
	Params []string `json:"params,omitempty"`
}

type wsStatusMessage struct {
	Code int    `json:"code"`
	Msg  string `json:"msg"`
}
