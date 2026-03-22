package bitmart

const (
	wsSpotURL         = "wss://ws-manager-compress.bitmart.com/api?protocol=1.1"
	symbolsPerShard   = 100
	commandBatchSize  = 20
	pingIntervalSec   = 15
	readIdleSec       = 45
	defaultDepthLevel = 100
	modeLevel100      = "level100"
	modeAll           = "all"
)

var validOrderBookDepths = []int{100}
