package coinex

import "os"

var (
	wsSpotURL = getenvDefault("COINEX_SPOT_WS_URL", "wss://socket.coinex.com/v2/spot")
	wsSwapURL = getenvDefault("COINEX_SWAP_WS_URL", "wss://socket.coinex.com/v2/futures")
)

const (
	readIdleSec      = 90
	defaultPingMS    = 20000
	symbolsPerShard  = 100
	commandBatchSize = 100
	flushEveryMS     = 200
)

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
