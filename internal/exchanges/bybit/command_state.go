package bybit

import "time"

const bybitMaxArgsPerCommand = 10

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

func chunkTopics(topics []string, chunkSize int) [][]string {
	if len(topics) == 0 {
		return nil
	}
	if chunkSize <= 0 {
		chunkSize = len(topics)
	}
	chunks := make([][]string, 0, (len(topics)+chunkSize-1)/chunkSize)
	for start := 0; start < len(topics); start += chunkSize {
		end := start + chunkSize
		if end > len(topics) {
			end = len(topics)
		}
		chunk := append([]string(nil), topics[start:end]...)
		chunks = append(chunks, chunk)
	}
	return chunks
}
