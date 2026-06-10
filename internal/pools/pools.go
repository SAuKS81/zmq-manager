package pools

import (
	"bybit-watcher/internal/shared_types"
	"sync"
)

var (
	// Pool für OrderBookUpdate-Objekte
	OrderBookUpdatePool = sync.Pool{
		New: func() interface{} {
			// OPTIMIERUNG: Wir reservieren festen Speicher für 25 Level.
			// Binance sendet meistens 10 oder 20. Wir müssen also NIE
			// neuen Speicher anfordern. Das entlastet den GC massiv.
			return &shared_types.OrderBookUpdate{
				Bids: make([]shared_types.OrderBookLevel, 0, 25),
				Asks: make([]shared_types.OrderBookLevel, 0, 25),
			}
		},
	}

	// Pool für TradeUpdate-Objekte
	TradeUpdatePool = sync.Pool{
		New: func() interface{} {
			return &shared_types.TradeUpdate{}
		},
	}

	OHLCVUpdatePool = sync.Pool{
		New: func() interface{} {
			return &shared_types.OHLCVUpdate{}
		},
	}
)

func GetOrderBookUpdate() *shared_types.OrderBookUpdate {
	return OrderBookUpdatePool.Get().(*shared_types.OrderBookUpdate)
}

func PutOrderBookUpdate(ob *shared_types.OrderBookUpdate) {
	ob.Exchange = ""
	ob.Symbol = ""
	ob.MarketType = ""
	ob.Timestamp = 0
	ob.GoTimestamp = 0
	ob.IngestUnixNano = 0
	ob.UpdateType = ""
	ob.DataType = ""

	// OPTIMIERUNG: Slices nicht auf nil setzen!
	// Wir behalten den Speicher (Capacity), setzen nur Länge auf 0.
	ob.Bids = ob.Bids[:0]
	ob.Asks = ob.Asks[:0]

	OrderBookUpdatePool.Put(ob)
}

func GetTradeUpdate() *shared_types.TradeUpdate {
	return TradeUpdatePool.Get().(*shared_types.TradeUpdate)
}

func PutTradeUpdate(t *shared_types.TradeUpdate) {
	t.Exchange = ""
	t.Symbol = ""
	t.MarketType = ""
	t.Timestamp = 0
	t.GoTimestamp = 0
	t.IngestUnixNano = 0
	t.Price = 0
	t.Amount = 0
	t.Side = ""
	t.TradeID = ""
	t.DataType = ""
	TradeUpdatePool.Put(t)
}

func GetOHLCVUpdate() *shared_types.OHLCVUpdate {
	return OHLCVUpdatePool.Get().(*shared_types.OHLCVUpdate)
}

func PutOHLCVUpdate(k *shared_types.OHLCVUpdate) {
	k.Exchange = ""
	k.Symbol = ""
	k.MarketType = ""
	k.Interval = ""
	k.Timestamp = 0
	k.GoTimestamp = 0
	k.IngestUnixNano = 0
	k.Open = 0
	k.High = 0
	k.Low = 0
	k.Close = 0
	k.Volume = 0
	k.Turnover = 0
	k.Confirm = false
	k.DataType = ""
	OHLCVUpdatePool.Put(k)
}
