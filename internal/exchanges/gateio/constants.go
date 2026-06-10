package gateio

const (
	spotWsURL = "wss://api.gateio.ws/ws/v4/"
	swapWsURL = "wss://fx-ws.gateio.ws/v4/ws/usdt"

	channelSpotTrades = "spot.trades"
	channelSwapTrades = "futures.trades"

	symbolsPerShard = 200
	subscribeChunk  = 50
)
