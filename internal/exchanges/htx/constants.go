package htx

import "os"

var (
	wsSpotURL = getenvDefault("HTX_SPOT_WS_URL", "wss://api.huobi.pro/ws")
	wsSwapURL = getenvDefault("HTX_SWAP_WS_URL", "wss://api.hbdm.com/linear-swap-ws")
)

const (
	readIdleSec      = 90
	symbolsPerShard  = 100
	commandBatchSize = 100
	flushEveryMS     = 100
)

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
