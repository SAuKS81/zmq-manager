package binance

const (
	// WebSocket Endpoints (Combined Streams für Metadaten)
	// Wir nutzen /stream statt /ws, damit wir Nachrichten im Format
	// {"stream":"<name>","data":{...}} erhalten. Das liefert uns das Symbol.
	spotWsURL    = "wss://stream.binance.com:9443/stream"
	futuresWsURL = "wss://fstream.binance.com/stream"

	// WebSocket Timings
	pingEverySec = 60
	readIdleSec  = 180

	// Sharding Limits
	spotSymbolsPerShard = 500
	swapSymbolsPerShard = 180 
)