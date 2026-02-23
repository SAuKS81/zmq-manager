package bybit

const (
	// WebSocket Endpoints
	spotWsURL   = "wss://stream.bybit.com/v5/public/spot"
	linearWsURL = "wss://stream.bybit.com/v5/public/linear"

	// WebSocket Timings
	readIdleSec  = 60
	pingEverySec = 15

	// Sharding
	symbolsPerShard = 40
)