package broker

import (
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type dropTracker struct {
	lastByKey sync.Map // map[string]*atomic.Int64 (unix seconds)
}

func (dt *dropTracker) shouldLog(key string, nowSec int64) bool {
	val, _ := dt.lastByKey.LoadOrStore(key, &atomic.Int64{})
	lastPtr := val.(*atomic.Int64)
	last := lastPtr.Load()
	if nowSec-last < 1 {
		return false
	}
	return lastPtr.CompareAndSwap(last, nowSec)
}

func captureDropCallsite(skip int) (string, string) {
	pcs := make([]uintptr, 16)
	n := runtime.Callers(skip, pcs)
	if n == 0 {
		return "unknown", "unknown"
	}

	frames := runtime.CallersFrames(pcs[:n])
	caller := "unknown"
	stack := strings.Builder{}
	wrote := 0

	for {
		frame, more := frames.Next()
		if frame.Function != "" {
			if wrote == 0 {
				caller = frame.Function + "@" + frame.File + ":" + strconv.Itoa(frame.Line)
			}
			if wrote < 6 {
				if wrote > 0 {
					stack.WriteString(" | ")
				}
				stack.WriteString(frame.Function)
				stack.WriteString("@")
				stack.WriteString(frame.File)
				stack.WriteString(":")
				stack.WriteString(strconv.Itoa(frame.Line))
			}
			wrote++
		}
		if !more || wrote >= 6 {
			break
		}
	}

	if stack.Len() == 0 {
		return caller, "unknown"
	}
	return caller, stack.String()
}

var bufferFullDropTracker dropTracker

func logDropCallsite(typeName, reason string) (caller string, stack string, logged bool) {
	key := typeName + "|" + reason
	if !bufferFullDropTracker.shouldLog(key, time.Now().Unix()) {
		return "", "", false
	}
	caller, stack = captureDropCallsite(3)
	return caller, stack, true
}
