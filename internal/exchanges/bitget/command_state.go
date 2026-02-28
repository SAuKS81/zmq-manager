package bitget

import (
	"time"

	json "github.com/goccy/go-json"
)

type bitgetPendingCommand struct {
	op      string
	attempt int
	sentAt  time.Time
}

type bitgetEventResponse struct {
	Event string          `json:"event"`
	Code  string          `json:"code"`
	Msg   string          `json:"msg"`
	Op    string          `json:"op"`
	Arg   json.RawMessage `json:"arg"`
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

func parseBitgetResponseArgs(raw json.RawMessage) []wsSubArg {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}

	var single wsSubArg
	if err := json.Unmarshal(raw, &single); err == nil && single.InstID != "" {
		return []wsSubArg{single}
	}

	var multi []wsSubArg
	if err := json.Unmarshal(raw, &multi); err == nil && len(multi) > 0 {
		return multi
	}

	return nil
}
