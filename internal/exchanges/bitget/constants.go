package bitget

const (
	wsURL = "wss://ws.bitget.com/v2/ws/public"

	readIdleSec  = 90 
	pingEverySec = 25

	// Sharding & Rate Limits
	symbolsPerShard = 50
	symbolsPerBatch = 20 // NEU: Anzahl der Symbole pro Abo-Befehl
	delayPerBatchMs = 500  // NEU: Pause nach jedem Abo-Batch in Millisekunden
)