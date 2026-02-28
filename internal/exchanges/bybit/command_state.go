package bybit

import "time"

type bybitInflightCommand struct {
	op      string
	topics  []string
	attempt int
	sentAt  time.Time
}

type bybitCommandRequest struct {
	Op    string   `json:"op"`
	Args  []string `json:"args"`
	ReqID string   `json:"req_id,omitempty"`
}

func queueUniqueTopic(dst []string, topic string) []string {
	for _, existing := range dst {
		if existing == topic {
			return dst
		}
	}
	return append(dst, topic)
}

func nextBybitRetryDelay(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 500 * time.Millisecond
	case 2:
		return 2 * time.Second
	default:
		return 5 * time.Second
	}
}
