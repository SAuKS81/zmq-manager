package broker

import (
	"sort"
	"strings"
	"sync"
	"time"

	"bybit-watcher/internal/shared_types"
)

const (
	runtimeStatusRunning      = "running"
	runtimeStatusDegraded     = "degraded"
	runtimeStatusReconnecting = "reconnecting"
	runtimeStatusFailed       = "failed"
	runtimeStatusStopped      = "stopped"
)

type runtimeKey struct {
	Exchange   string
	MarketType string
	Symbol     string
	DataType   string
}

type runtimeHealthState struct {
	LastMessageTS int64
	LastLatencyMS float64
	Status        string
	LastError     string

	buckets        [60]int
	bucketUnixSec  int64
	reconnects1H   []int64
	reconnects24H  []int64
}

type runtimeTracker struct {
	mu     sync.Mutex
	states map[runtimeKey]*runtimeHealthState
}

func newRuntimeTracker() *runtimeTracker {
	return &runtimeTracker{
		states: make(map[runtimeKey]*runtimeHealthState),
	}
}

func (rt *runtimeTracker) recordTrade(trade *shared_types.TradeUpdate) {
	if trade == nil {
		return
	}
	rt.recordTradeForSymbol(trade, trade.Symbol)
}

func (rt *runtimeTracker) recordTradeForSymbol(trade *shared_types.TradeUpdate, symbol string) {
	if trade == nil || symbol == "" {
		return
	}
	rt.recordMessage(runtimeKey{
		Exchange:   trade.Exchange,
		MarketType: trade.MarketType,
		Symbol:     symbol,
		DataType:   "trades",
	}, trade.GoTimestamp, trade.IngestUnixNano)
}

func (rt *runtimeTracker) recordOrderBook(ob *shared_types.OrderBookUpdate) {
	if ob == nil {
		return
	}
	rt.recordOrderBookForSymbol(ob, ob.Symbol)
}

func (rt *runtimeTracker) recordOrderBookForSymbol(ob *shared_types.OrderBookUpdate, symbol string) {
	if ob == nil || symbol == "" {
		return
	}
	rt.recordMessage(runtimeKey{
		Exchange:   ob.Exchange,
		MarketType: ob.MarketType,
		Symbol:     symbol,
		DataType:   "orderbooks",
	}, ob.GoTimestamp, ob.IngestUnixNano)
}

func (rt *runtimeTracker) recordMessage(key runtimeKey, eventTS int64, ingestUnixNano int64) {
	now := time.Now()
	nowMs := now.UnixMilli()
	nowSec := now.Unix()

	rt.mu.Lock()
	defer rt.mu.Unlock()

	state := rt.getOrCreateLocked(key)
	state.LastMessageTS = maxInt64(eventTS, nowMs)
	if ingestUnixNano > 0 {
		state.LastLatencyMS = float64(now.UnixNano()-ingestUnixNano) / 1_000_000.0
	}
	state.LastError = ""
	if state.Status != runtimeStatusReconnecting {
		state.Status = runtimeStatusRunning
	}
	rt.advanceBucketsLocked(state, nowSec)
	state.buckets[0]++
}

func (rt *runtimeTracker) recordStatus(event *shared_types.StreamStatusEvent) {
	if event == nil {
		return
	}

	symbols := make([]string, 0, 1+len(event.Symbols))
	if event.Symbol != "" {
		symbols = append(symbols, event.Symbol)
	}
	symbols = append(symbols, event.Symbols...)
	if len(symbols) == 0 {
		symbols = append(symbols, "")
	}

	for _, symbol := range symbols {
		rt.recordStatusForSymbol(event, symbol)
	}
}

func (rt *runtimeTracker) recordStatusForSymbol(event *shared_types.StreamStatusEvent, symbol string) {
	if event == nil {
		return
	}
	nowMs := time.Now().UnixMilli()

	rt.mu.Lock()
	defer rt.mu.Unlock()

	key := runtimeKey{
		Exchange:   event.Exchange,
		MarketType: event.MarketType,
		Symbol:     symbol,
		DataType:   event.DataType,
	}
	state := rt.getOrCreateLocked(key)
	if event.Message != "" {
		state.LastError = event.Message
	} else if event.Reason != "" {
		state.LastError = event.Reason
	}
	switch event.Type {
	case runtimeStatusReconnecting, "stream_reconnecting":
		state.Status = runtimeStatusReconnecting
		state.reconnects1H = append(state.reconnects1H, nowMs)
		state.reconnects24H = append(state.reconnects24H, nowMs)
	case "stream_restored":
		state.Status = runtimeStatusRunning
		if event.Message == "" && event.Reason == "" {
			state.LastError = ""
		}
	case "stream_unsubscribe_failed":
		state.Status = runtimeStatusFailed
	case "stream_force_closed":
		state.Status = runtimeStatusStopped
	}
	rt.pruneReconnectsLocked(state, nowMs)
}

func (rt *runtimeTracker) snapshotHealth(subs []shared_types.RuntimeSubscriptionItem, now time.Time) ([]shared_types.SubscriptionHealthItem, shared_types.RuntimeSnapshotTotals) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	items := make([]shared_types.SubscriptionHealthItem, 0, len(subs))
	totals := shared_types.RuntimeSnapshotTotals{
		ActiveSubscriptions: len(subs),
	}
	nowMs := now.UnixMilli()
	nowSec := now.Unix()

	for _, sub := range subs {
		key := runtimeKey{
			Exchange:   sub.Exchange,
			MarketType: sub.MarketType,
			Symbol:     sub.Symbol,
			DataType:   sub.DataType,
		}
		state := rt.lookupStateLocked(key)

		item := shared_types.SubscriptionHealthItem{
			Exchange:   sub.Exchange,
			MarketType: sub.MarketType,
			Symbol:     sub.Symbol,
			DataType:   sub.DataType,
			Status:     runtimeStatusDegraded,
		}

		if state == nil {
			if sub.Running {
				item.Status = runtimeStatusDegraded
			} else {
				item.Status = runtimeStatusStopped
			}
			items = append(items, item)
			continue
		}

		rt.advanceBucketsLocked(state, nowSec)
		rt.pruneReconnectsLocked(state, nowMs)

		item.LastMessageTS = state.LastMessageTS
		if state.LastMessageTS > 0 {
			item.LastMessageAgeMS = maxInt64(0, nowMs-state.LastMessageTS)
		}
		item.Reconnects1H = len(state.reconnects1H)
		item.MessagesPerSec = rt.messagesPerSecondLocked(state)
		item.LatencyMS = state.LastLatencyMS
		item.LastError = state.LastError
		item.Status = rt.normalizeStatusLocked(state, sub.Running, item.LastMessageAgeMS)

		items = append(items, item)
		totals.MessagesPerSec += item.MessagesPerSec
		totals.Reconnects24H += len(state.reconnects24H)
	}

	sort.Slice(items, func(i, j int) bool {
		return lessRuntimeHealth(items[i], items[j])
	})
	return items, totals
}

func (rt *runtimeTracker) isRunning(key runtimeKey) bool {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	state := rt.lookupStateLocked(key)
	if state == nil {
		return true
	}
	return state.Status != runtimeStatusFailed && state.Status != runtimeStatusStopped
}

func (rt *runtimeTracker) lookupStateLocked(key runtimeKey) *runtimeHealthState {
	if state := rt.states[key]; state != nil {
		return state
	}
	if strings.HasSuffix(key.Exchange, "_native") {
		key.Exchange = strings.TrimSuffix(key.Exchange, "_native")
		if state := rt.states[key]; state != nil {
			return state
		}
	}
	return nil
}

func (rt *runtimeTracker) getOrCreateLocked(key runtimeKey) *runtimeHealthState {
	state, ok := rt.states[key]
	if ok {
		return state
	}
	state = &runtimeHealthState{
		Status: runtimeStatusRunning,
	}
	rt.states[key] = state
	return state
}

func (rt *runtimeTracker) advanceBucketsLocked(state *runtimeHealthState, nowSec int64) {
	if state.bucketUnixSec == 0 {
		state.bucketUnixSec = nowSec
		return
	}
	if nowSec <= state.bucketUnixSec {
		return
	}

	diff := nowSec - state.bucketUnixSec
	if diff >= int64(len(state.buckets)) {
		for i := range state.buckets {
			state.buckets[i] = 0
		}
		state.bucketUnixSec = nowSec
		return
	}

	for i := int64(0); i < diff; i++ {
		for j := len(state.buckets) - 1; j > 0; j-- {
			state.buckets[j] = state.buckets[j-1]
		}
		state.buckets[0] = 0
		state.bucketUnixSec++
	}
}

func (rt *runtimeTracker) pruneReconnectsLocked(state *runtimeHealthState, nowMs int64) {
	oneHourAgo := nowMs - int64(time.Hour/time.Millisecond)
	dayAgo := nowMs - int64(24*time.Hour/time.Millisecond)
	state.reconnects1H = pruneAfter(state.reconnects1H, oneHourAgo)
	state.reconnects24H = pruneAfter(state.reconnects24H, dayAgo)
}

func (rt *runtimeTracker) messagesPerSecondLocked(state *runtimeHealthState) float64 {
	total := 0
	for _, c := range state.buckets {
		total += c
	}
	return float64(total) / 60.0
}

func (rt *runtimeTracker) normalizeStatusLocked(state *runtimeHealthState, running bool, ageMs int64) string {
	if !running {
		return runtimeStatusStopped
	}
	switch state.Status {
	case runtimeStatusFailed:
		return runtimeStatusFailed
	case runtimeStatusStopped:
		return runtimeStatusStopped
	case runtimeStatusReconnecting:
		return runtimeStatusReconnecting
	}
	if state.LastMessageTS == 0 {
		return runtimeStatusDegraded
	}
	if ageMs > 5000 {
		return runtimeStatusDegraded
	}
	return runtimeStatusRunning
}

func pruneAfter(items []int64, min int64) []int64 {
	if len(items) == 0 {
		return items
	}
	idx := 0
	for idx < len(items) && items[idx] < min {
		idx++
	}
	if idx == 0 {
		return items
	}
	out := make([]int64, len(items)-idx)
	copy(out, items[idx:])
	return out
}

func lessRuntimeHealth(a, b shared_types.SubscriptionHealthItem) bool {
	if a.Exchange != b.Exchange {
		return a.Exchange < b.Exchange
	}
	if a.MarketType != b.MarketType {
		return a.MarketType < b.MarketType
	}
	if a.Symbol != b.Symbol {
		return a.Symbol < b.Symbol
	}
	return a.DataType < b.DataType
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
