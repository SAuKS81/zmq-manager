package woo

import (
	"os"
	"strings"
)

const (
	defaultWsURL = "wss://wss.woox.io/ws/stream"

	symbolsPerShard       = 50
	subscribeSendDelayMS  = 50
	reconnectDelaySeconds = 2
	pingIntervalSeconds   = 10
)

func publicWsURL() string {
	if value := strings.TrimSpace(os.Getenv("WOOX_WS_URL")); value != "" {
		return value
	}
	return defaultWsURL
}
