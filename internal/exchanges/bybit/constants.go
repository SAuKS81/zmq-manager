package bybit

import "os"

var (
	// WebSocket Endpoints
	spotWsURL   = getenvDefault("BYBIT_SPOT_WS_URL", "wss://stream.bybit.com/v5/public/spot")
	linearWsURL = getenvDefault("BYBIT_LINEAR_WS_URL", "wss://stream.bybit.com/v5/public/linear")
)

const (
	// WebSocket Timings
	readIdleSec  = 60
	pingEverySec = 15

	// Sharding
	symbolsPerShard = 40
)

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
