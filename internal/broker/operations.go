package broker

import (
	"time"

	"bybit-watcher/internal/shared_types"
)

type pendingActivation struct {
	ClientID   []byte
	RequestID  string
	EventType  string
	ExactKey   runtimeKey
	Adapter    string
	Status     string
	RecordedAt int64
}

type requestOperationContext struct {
	RequestID string
	Action    string
	ClientID  []byte
	Adapter   string
}

type deployBatchState struct {
	ClientID []byte
	Sent     int
	Acked    int
	Failed   int
}

func (sm *SubscriptionManager) registerDeployBatch(req *shared_types.ClientRequest) {
	if req == nil || req.RequestID == "" || req.BatchSent <= 0 {
		return
	}
	if sm.deployBatches == nil {
		sm.deployBatches = make(map[string]*deployBatchState)
	}
	sm.deployBatches[req.RequestID] = &deployBatchState{
		ClientID: append([]byte(nil), req.ClientID...),
		Sent:     req.BatchSent,
	}
}

func (sm *SubscriptionManager) recordDeployBatchResult(requestID string, failed bool) {
	if requestID == "" || sm.deployBatches == nil {
		return
	}
	state := sm.deployBatches[requestID]
	if state == nil {
		return
	}
	if failed {
		state.Failed++
	} else {
		state.Acked++
	}
	if state.Acked+state.Failed < state.Sent {
		return
	}

	delete(sm.deployBatches, requestID)
	sm.sendJSONToClient(state.ClientID, &shared_types.DeployBatchSummaryEvent{
		Type:      "deploy_batch_summary",
		RequestID: requestID,
		TS:        time.Now().UnixMilli(),
		Sent:      state.Sent,
		Acked:     state.Acked,
		Failed:    state.Failed,
	})
}

func (sm *SubscriptionManager) noteRequestContext(key runtimeKey, req *shared_types.ClientRequest, action string) {
	if req == nil || req.RequestID == "" {
		return
	}
	if sm.requestContexts == nil {
		sm.requestContexts = make(map[runtimeKey]requestOperationContext)
	}
	sm.requestContexts[key] = requestOperationContext{
		RequestID: req.RequestID,
		Action:    action,
		ClientID:  append([]byte(nil), req.ClientID...),
		Adapter:   adapterFromExchangeRoute(req.Exchange),
	}
}

func (sm *SubscriptionManager) consumeRequestContext(key runtimeKey) requestOperationContext {
	if sm.requestContexts == nil {
		return requestOperationContext{}
	}
	ctx := sm.requestContexts[key]
	delete(sm.requestContexts, key)
	return ctx
}

func (sm *SubscriptionManager) peekRequestContext(key runtimeKey) requestOperationContext {
	if sm.requestContexts == nil {
		return requestOperationContext{}
	}
	return sm.requestContexts[key]
}

func (sm *SubscriptionManager) queuePendingActivation(key runtimeKey, req *shared_types.ClientRequest, eventType string) {
	if req == nil || req.RequestID == "" {
		return
	}
	if sm.pendingActivations == nil {
		sm.pendingActivations = make(map[runtimeKey]pendingActivation)
	}
	sm.pendingActivations[key] = pendingActivation{
		ClientID:   append([]byte(nil), req.ClientID...),
		RequestID:  req.RequestID,
		EventType:  eventType,
		ExactKey:   key,
		Adapter:    adapterFromExchangeRoute(req.Exchange),
		Status:     runtimeStatusRunning,
		RecordedAt: time.Now().UnixMilli(),
	}
}

func (sm *SubscriptionManager) emitPendingActivation(key runtimeKey) {
	if sm.pendingActivations == nil {
		return
	}
	pending, ok := sm.pendingActivations[key]
	if !ok {
		return
	}
	delete(sm.pendingActivations, key)
	sm.sendStatusToClient(pending.ClientID, &shared_types.StreamStatusEvent{
		Type:       pending.EventType,
		Exchange:   pending.ExactKey.Exchange,
		MarketType: pending.ExactKey.MarketType,
		Symbol:     pending.ExactKey.Symbol,
		DataType:   pending.ExactKey.DataType,
		Adapter:    pending.Adapter,
		RequestID:  pending.RequestID,
		Status:     pending.Status,
		Timestamp:  time.Now().UnixMilli(),
	})
}

func (sm *SubscriptionManager) sendStatusToClient(clientID []byte, event *shared_types.StreamStatusEvent) {
	if sm.DistributionCh == nil || len(clientID) == 0 || event == nil {
		return
	}
	sm.DistributionCh <- &DistributionMessage{
		ClientIDs:  [][]byte{append([]byte(nil), clientID...)},
		RawPayload: event,
	}
}

func (sm *SubscriptionManager) sendJSONToClient(clientID []byte, payload any) {
	if sm.DistributionCh == nil || len(clientID) == 0 || payload == nil {
		return
	}
	sm.DistributionCh <- &DistributionMessage{
		ClientIDs:  [][]byte{append([]byte(nil), clientID...)},
		RawPayload: payload,
	}
}
