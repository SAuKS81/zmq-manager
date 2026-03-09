//go:build ccxt
// +build ccxt

package ccxt

import (
	"fmt"
	"log"
	"strings"
	"time"

	"bybit-watcher/internal/shared_types"
)

func emitStatus(statusCh chan<- *shared_types.StreamStatusEvent, event *shared_types.StreamStatusEvent) {
	if statusCh == nil || event == nil {
		return
	}
	if event.Timestamp == 0 {
		event.Timestamp = time.Now().UnixMilli()
	}
	if event.Adapter == "" {
		event.Adapter = "ccxt"
	}
	select {
	case statusCh <- event:
	default:
		log.Printf("[CCXT-STATUS-WARN] status channel full, dropping event type=%s exchange=%s market_type=%s data_type=%s symbol=%s symbols=%d message=%s",
			event.Type,
			event.Exchange,
			event.MarketType,
			event.DataType,
			event.Symbol,
			len(event.Symbols),
			event.Message,
		)
	}
}

func summarizeSymbols(symbols []string, limit int) string {
	if len(symbols) == 0 {
		return "-"
	}
	if limit <= 0 || len(symbols) <= limit {
		return strings.Join(symbols, ",")
	}
	head := append([]string(nil), symbols[:limit]...)
	return fmt.Sprintf("%s ... (+%d more)", strings.Join(head, ","), len(symbols)-limit)
}
