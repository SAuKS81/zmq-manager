//go:build ccxt
// +build ccxt

package ccxt

import (
	"sync/atomic"
	"time"
)

const (
	workerStopGracePeriod    = 250 * time.Millisecond
	workerStopForceTimeout   = 2 * time.Second
	activeWorkerPollInterval = 10 * time.Millisecond
)

func waitForActiveWorkers(counter *atomic.Int64, timeout time.Duration) bool {
	if counter == nil {
		return true
	}

	if timeout <= 0 {
		return counter.Load() == 0
	}

	deadline := time.Now().Add(timeout)
	for {
		if counter.Load() == 0 {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(activeWorkerPollInterval)
	}
}
