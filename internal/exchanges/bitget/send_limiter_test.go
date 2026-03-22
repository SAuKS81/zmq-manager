package bitget

import (
	"sync"
	"testing"
	"time"
)

func TestBitgetSendLimiterWaitDoesNotSerializeUnderLock(t *testing.T) {
	limiter := newBitgetSendLimiter(25 * time.Millisecond)
	stopCh := make(chan struct{})

	start := time.Now()

	var wg sync.WaitGroup
	results := make(chan time.Duration, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if !limiter.Wait(stopCh) {
				t.Errorf("wait aborted unexpectedly")
				return
			}
			results <- time.Since(start)
		}()
	}

	wg.Wait()
	close(results)

	var delays []time.Duration
	for delay := range results {
		delays = append(delays, delay)
	}
	if len(delays) != 2 {
		t.Fatalf("expected 2 completion delays, got %d", len(delays))
	}

	var maxDelay time.Duration
	for _, delay := range delays {
		if delay > maxDelay {
			maxDelay = delay
		}
	}

	if maxDelay > 80*time.Millisecond {
		t.Fatalf("expected limiter to avoid excessive serialized waiting, max delay=%v", maxDelay)
	}
}
