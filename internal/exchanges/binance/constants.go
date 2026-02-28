package binance

import "os"

var (
	// WebSocket Endpoints (Combined Streams for metadata)
	// We use /stream so messages include {"stream":"...","data":...}.
	spotWsURL    = getenvDefault("BINANCE_SPOT_WS_URL", "wss://stream.binance.com:9443/stream")
	futuresWsURL = getenvDefault("BINANCE_FUTURES_WS_URL", "wss://fstream.binance.com/stream")
)

const (
	// WebSocket Timings
	pingEverySec = 60
	readIdleSec  = 180

	// Sharding Limits
	spotSymbolsPerShard = 500
	swapSymbolsPerShard = 180
)

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
