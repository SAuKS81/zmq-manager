//go:build ccxt
// +build ccxt

package ccxt

import "strings"

const badSymbolPrefix = "does not have market symbol "

func extractMissingMarketSymbol(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	msg := err.Error()
	idx := strings.Index(msg, badSymbolPrefix)
	if idx == -1 {
		return "", false
	}
	s := msg[idx+len(badSymbolPrefix):]
	if nl := strings.IndexByte(s, '\n'); nl >= 0 {
		s = s[:nl]
	}
	s = strings.TrimRight(s, "] ")
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false
	}
	return s, true
}

func removeSymbolFromBatch(batch []string, symbol string) []string {
	out := batch[:0]
	for _, s := range batch {
		if s != symbol {
			out = append(out, s)
		}
	}
	return out
}
