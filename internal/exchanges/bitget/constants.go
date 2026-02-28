package bitget

import "os"

var (
	wsURL = getenvDefault("BITGET_WS_URL", "wss://ws.bitget.com/v2/ws/public")
)

const (
	readIdleSec  = 90
	pingEverySec = 25

	// Sharding & Rate Limits
	symbolsPerShard = 50
	symbolsPerBatch = 20 // number of symbols per subscribe command
	delayPerBatchMs = 500
	idleShutdownMs  = 2000
)

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
