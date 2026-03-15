//go:build ccxt
// +build ccxt

package ccxt

import (
	"context"
	"time"
)

func reconnectDelay(cfg ExchangeConfig, attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}

	delay := cfg.ReconnectBaseDelay
	if delay <= 0 {
		delay = 10 * time.Second
	}
	maxDelay := cfg.ReconnectMaxDelay
	if maxDelay <= 0 {
		maxDelay = delay
	}

	for i := 1; i < attempt && delay < maxDelay; i++ {
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
			break
		}
	}

	if jitter := cfg.ReconnectJitter; jitter > 0 {
		jitterOffset := time.Duration(time.Now().UnixNano() % int64(jitter))
		delay += jitterOffset
		if delay > maxDelay {
			delay = maxDelay
		}
	}

	return delay
}

func sleepWithContext(ctx context.Context, delay time.Duration) bool {
	if delay <= 0 {
		return true
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
