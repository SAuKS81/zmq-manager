package mexc

import "os"

var (
	wsURL                = getenvDefault("MEXC_WS_URL", "wss://wbs-api.mexc.com/ws")
	mexcDepthSnapshotURL = getenvDefault("MEXC_DEPTH_SNAPSHOT_URL", "https://api.mexc.com/api/v3/depth")
)

const (
	readIdleSec            = 90
	pingEverySec           = 20
	symbolsPerShard        = 30
	commandBatchSize       = 30
	flushEvery             = 200
	mexcDefaultStreamFreq  = "100ms"
	mexcDepthSnapshotLimit = 1000
)

var validOrderBookDepths = []int{5, 10, 20}
var validOrderBookFrequencies = map[string]bool{"10ms": true, "100ms": true}

func getenvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
