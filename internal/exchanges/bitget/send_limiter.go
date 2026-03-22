package bitget

import (
	"sync"
	"time"
)

// bitgetSendLimiter enforces a minimum gap between outbound WS commands
// across all shards that share it.
type bitgetSendLimiter struct {
	mu          sync.Mutex
	nextAllowed time.Time
	gap         time.Duration
}

func newBitgetSendLimiter(gap time.Duration) *bitgetSendLimiter {
	return &bitgetSendLimiter{gap: gap}
}

func (l *bitgetSendLimiter) Wait(stopCh <-chan struct{}) bool {
	for {
		l.mu.Lock()
		now := time.Now()
		if l.nextAllowed.IsZero() || !now.Before(l.nextAllowed) {
			l.nextAllowed = now.Add(l.gap)
			l.mu.Unlock()
			return true
		}
		wait := time.Until(l.nextAllowed)
		l.mu.Unlock()

		timer := time.NewTimer(wait)
		select {
		case <-timer.C:
		case <-stopCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return false
		}
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
	}
}
