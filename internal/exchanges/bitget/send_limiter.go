package bitget

import (
	"sync"
	"time"
)

// bitgetSendLimiter enforces a minimum gap between outbound WS commands
// across all shards that share it.
type bitgetSendLimiter struct {
	mu       sync.Mutex
	lastSend time.Time
	gap      time.Duration
}

func newBitgetSendLimiter(gap time.Duration) *bitgetSendLimiter {
	return &bitgetSendLimiter{gap: gap}
}

func (l *bitgetSendLimiter) Wait(stopCh <-chan struct{}) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	wait := l.gap - now.Sub(l.lastSend)
	if wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-timer.C:
		case <-stopCh:
			return false
		}
	}

	l.lastSend = time.Now()
	return true
}
