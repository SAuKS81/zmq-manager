package bitget

import "time"

type bitgetPendingCommand struct {
	op      string
	attempt int
	sentAt  time.Time
}

type bitgetEventResponse struct {
	Event string   `json:"event"`
	Code  string   `json:"code"`
	Msg   string   `json:"msg"`
	Arg   wsSubArg `json:"arg"`
}

func nextBitgetRetryDelay(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 500 * time.Millisecond
	case 2:
		return 2 * time.Second
	default:
		return 5 * time.Second
	}
}
