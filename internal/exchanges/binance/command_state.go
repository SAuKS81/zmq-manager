package binance

import "time"

type binanceBatchRequest struct {
	id      uint64
	streams []string
}

type binanceInflightCommand struct {
	method  string
	streams []string
	attempt int
	sentAt  time.Time
}

func queueUniqueStream(dst []string, stream string) []string {
	for _, existing := range dst {
		if existing == stream {
			return dst
		}
	}
	return append(dst, stream)
}

func nextUnsubscribeRetryDelay(attempt int) time.Duration {
	switch attempt {
	case 1:
		return 500 * time.Millisecond
	case 2:
		return 2 * time.Second
	default:
		return 5 * time.Second
	}
}
