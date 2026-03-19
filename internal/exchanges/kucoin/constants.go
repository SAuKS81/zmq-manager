package kucoin

import "os"

var (
	wsSpotURL  = getenvDefault("KUCOIN_SPOT_WS_URL", "wss://ws-api-spot.kucoin.com")
	wsTokenURL = getenvDefault("KUCOIN_SPOT_WS_TOKEN_URL", "https://api.kucoin.com/api/v1/bullet-public")
)

const (
	readIdleSec      = 90
	defaultPingMS    = 25000
	symbolsPerShard  = 100
	commandBatchSize = 50
	flushEveryMS     = 200
)

var validOrderBookDepths = []int{1, 5, 50}

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
