//go:build ccxt
// +build ccxt

package ccxt

import (
	"regexp"
	"strings"
)

const badSymbolPrefix = "does not have market symbol "
const badSymbolPrefix2 = "bad symbol"

var symbolPattern = regexp.MustCompile(`([A-Za-z0-9._-]+/[A-Za-z0-9._-]+(?::[A-Za-z0-9._-]+)?)`)

func extractMissingMarketSymbol(err error) (string, bool) {
	if err == nil {
		return "", false
	}
	msg := strings.TrimSpace(err.Error())
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

	// Fallback for variants like "BadSymbol", "bad symbol", etc.
	if strings.Contains(lower, badSymbolPrefix2) || strings.Contains(lower, "badsymbol") || strings.Contains(lower, "market symbol") {
		if m := symbolPattern.FindStringSubmatch(msg); len(m) > 1 && m[1] != "" {
			return m[1], true
		}
	}

	return "", false
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
