package broker

// DistributionMessage verpackt die Rohdaten mit der Liste der Clients.
type DistributionMessage struct {
	ClientIDs [][]byte
	Broadcast bool

	// Die Rohdaten (z.B. []*TradeUpdate) als Interface
	RawPayload interface{}

	// Symbol für Debugging-Zwecke
	DebugSymbol string

	// NEU: Callback, der aufgerufen wird, wenn die Nachricht gesendet wurde.
	// Hier kann der SubscriptionManager Speicher aufräumen (Pooling).
	OnComplete func()
}
