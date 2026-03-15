//go:build ccxt
// +build ccxt

package ccxt

import (
	"regexp"
	"strings"
)

const badSymbolPrefix = "does not have market symbol "
const stackTraceMarker = "\nStack trace:"
const stackMarker = "\nStack:"
const debugStackMarker = "\ngoroutine "

var symbolPattern = regexp.MustCompile(`([A-Za-z0-9._-]+/[A-Za-z0-9._-]+(?::[A-Za-z0-9._-]+)?)`)

func extractMissingMarketSymbol(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	msg := trimErrorEnvelope(strings.TrimSpace(err.Error()))
	lower := strings.ToLower(msg)

	// Primary pattern used by many ccxt errors.
	if idx := strings.Index(lower, badSymbolPrefix); idx >= 0 {
		s := msg[idx+len(badSymbolPrefix):]
		if nl := strings.IndexByte(s, '\n'); nl >= 0 {
			s = s[:nl]
		}
		s = strings.TrimRight(s, "] ")
		s = strings.Trim(s, "`'\" ")
		if s != "" {
			return s, true
		}
	}

	// Stricter fallback for CCXT panic wrappers that still carry a BadSymbol marker
	// in the headline but may include unrelated symbols in the appended stack trace.
	if strings.Contains(lower, "badsymbol") || strings.Contains(lower, "[badsymbol]") || strings.Contains(lower, "[bad symbol]") {
		if m := symbolPattern.FindStringSubmatch(msg); len(m) > 1 && m[1] != "" {
			return m[1], true
		}
	}

	return "", false
}

func trimErrorEnvelope(msg string) string {
	if idx := strings.Index(msg, stackTraceMarker); idx >= 0 {
		msg = msg[:idx]
	}
	if idx := strings.Index(msg, stackMarker); idx >= 0 {
		msg = msg[:idx]
	}
	if idx := strings.Index(msg, debugStackMarker); idx >= 0 {
		msg = msg[:idx]
	}
	return strings.TrimSpace(msg)
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
