package binance

type wsRequest struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
	ID     uint64   `json:"id"`
}

type wsCommandResponse struct {
	Result any    `json:"result"`
	ID     uint64 `json:"id"`
	Code   int    `json:"code"`
	Msg    string `json:"msg"`
}

type wsOrderBookCombined struct {
	Stream string             `json:"stream"`
	Data   wsOrderBookPartial `json:"data"`
}

type wsKlineCombined struct {
	Stream string  `json:"stream"`
	Data   wsKline `json:"data"`
}

type wsTrade struct {
	EventType    string `json:"e"`
	EventTime    int64  `json:"E"`
	Symbol       string `json:"s"`
	TradeID      int64  `json:"t"`
	Price        string `json:"p"`
	Quantity     string `json:"q"`
	TradeTime    int64  `json:"T"`
	IsBuyerMaker bool   `json:"m"`
}

type wsKline struct {
	EventType string      `json:"e"`
	EventTime int64       `json:"E"`
	Symbol    string      `json:"s"`
	Kline     wsKlineData `json:"k"`
}

type wsKlineData struct {
	StartTime        int64  `json:"t"`
	CloseTime        int64  `json:"T"`
	Symbol           string `json:"s"`
	Interval         string `json:"i"`
	Open             string `json:"o"`
	Close            string `json:"c"`
	High             string `json:"h"`
	Low              string `json:"l"`
	Volume           string `json:"v"`
	TradeCount       int64  `json:"n"`
	Closed           bool   `json:"x"`
	QuoteVolume      string `json:"q"`
	TakerBuyVolume   string `json:"V"`
	TakerBuyTurnover string `json:"Q"`
}

type wsOrderBookLevel []string

type wsOrderBookPartial struct {
	EventType    string             `json:"e"`
	EventTime    int64              `json:"E"`
	Symbol       string             `json:"s"`
	LastUpdateID int64              `json:"lastUpdateId"`
	BidsSpot     []wsOrderBookLevel `json:"bids"`
	AsksSpot     []wsOrderBookLevel `json:"asks"`
	BidsFut      []wsOrderBookLevel `json:"b"`
	AsksFut      []wsOrderBookLevel `json:"a"`
}
