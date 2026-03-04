//go:build ccxt
// +build ccxt

package ccxt

import (
	"strings"
	"time"
)

func canonicalExchangeName(exchangeName string) string {
	switch strings.ToLower(exchangeName) {
	case "huobi":
		return "htx"
	default:
		return strings.ToLower(exchangeName)
	}
}

// ExchangeConfig definiert das Lifecycle- und Sharding-Verhalten fuer einen
// spezifischen Markt-Typ einer Boerse im CCXT-Adapter.
type ExchangeConfig struct {
	Enabled                       bool
	UseForSymbols                 bool
	BatchSize                     int
	SymbolsPerShard               int
	SubscribePause                time.Duration
	BatchSubscribePause           time.Duration
	NewShardPause                 time.Duration
	SupportsTradeUnwatch          bool
	SupportsTradeBatchUnwatch     bool
	SupportsOrderBookUnwatch      bool
	SupportsOrderBookBatchUnwatch bool
}

// ExchangePolicySet haelt die Konfiguration pro Markt-Typ.
type ExchangePolicySet struct {
	Spot ExchangeConfig
	Swap ExchangeConfig
}

func (p ExchangePolicySet) configForMarket(marketType string) ExchangeConfig {
	switch marketType {
	case "swap":
		return p.Swap
	default:
		return p.Spot
	}
}

var defaultExchangePolicy = ExchangePolicySet{
	Spot: ExchangeConfig{
		Enabled:         true,
		UseForSymbols:   false,
		BatchSize:       1,
		SymbolsPerShard: 1,
		SubscribePause:  300 * time.Millisecond,
		NewShardPause:   1200 * time.Millisecond,
	},
	Swap: ExchangeConfig{
		Enabled:         true,
		UseForSymbols:   false,
		BatchSize:       1,
		SymbolsPerShard: 1,
		SubscribePause:  300 * time.Millisecond,
		NewShardPause:   1200 * time.Millisecond,
	},
}

// exchangePolicies enthaelt nur bewusste, verifizierte oder klar dokumentierte
// Abweichungen vom konservativen Default.
var exchangePolicies = map[string]ExchangePolicySet{
	"binance": {
		Spot: ExchangeConfig{
			Enabled:                       true,
			UseForSymbols:                 true,
			BatchSize:                     200,
			SymbolsPerShard:               200,
			SubscribePause:                100 * time.Millisecond,
			NewShardPause:                 1100 * time.Millisecond,
			SupportsTradeUnwatch:          true,
			SupportsTradeBatchUnwatch:     true,
			SupportsOrderBookUnwatch:      true,
			SupportsOrderBookBatchUnwatch: true,
		},
		Swap: ExchangeConfig{
			Enabled:                       true,
			UseForSymbols:                 true,
			BatchSize:                     200,
			SymbolsPerShard:               200,
			SubscribePause:                250 * time.Millisecond,
			NewShardPause:                 1100 * time.Millisecond,
			SupportsTradeUnwatch:          true,
			SupportsTradeBatchUnwatch:     true,
			SupportsOrderBookUnwatch:      true,
			SupportsOrderBookBatchUnwatch: true,
		},
	},
	"bitget": {
		Spot: ExchangeConfig{
			Enabled:                   true,
			UseForSymbols:             true,
			BatchSize:                 20,
			SymbolsPerShard:           20,
			SubscribePause:            500 * time.Millisecond,
			BatchSubscribePause:       500 * time.Millisecond,
			NewShardPause:             1100 * time.Millisecond,
			SupportsTradeBatchUnwatch: true,
		},
		Swap: ExchangeConfig{
			Enabled:                   true,
			UseForSymbols:             true,
			BatchSize:                 20,
			SymbolsPerShard:           20,
			SubscribePause:            500 * time.Millisecond,
			BatchSubscribePause:       500 * time.Millisecond,
			NewShardPause:             1100 * time.Millisecond,
			SupportsTradeBatchUnwatch: true,
		},
	},
	"bybit": {
		Spot: ExchangeConfig{
			Enabled:                       true,
			UseForSymbols:                 true,
			BatchSize:                     10,
			SymbolsPerShard:               40,
			SubscribePause:                100 * time.Millisecond,
			NewShardPause:                 1100 * time.Millisecond,
			SupportsTradeBatchUnwatch:     true,
			SupportsOrderBookBatchUnwatch: true,
		},
		Swap: ExchangeConfig{
			Enabled:                       true,
			UseForSymbols:                 true,
			BatchSize:                     10,
			SymbolsPerShard:               40,
			SubscribePause:                100 * time.Millisecond,
			NewShardPause:                 1100 * time.Millisecond,
			SupportsTradeBatchUnwatch:     true,
			SupportsOrderBookBatchUnwatch: true,
		},
	},
	"coinex": {
		Spot: ExchangeConfig{
			Enabled:         true,
			UseForSymbols:   true,
			BatchSize:       500,
			SymbolsPerShard: 1000,
			SubscribePause:  100 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
		Swap: ExchangeConfig{
			Enabled:         false,
			UseForSymbols:   true,
			BatchSize:       500,
			SymbolsPerShard: 1000,
			SubscribePause:  250 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
	},
	"htx": {
		Spot: ExchangeConfig{
			Enabled:         true,
			UseForSymbols:   false,
			BatchSize:       1000,
			SymbolsPerShard: 1000,
			SubscribePause:  110 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
		Swap: ExchangeConfig{
			Enabled:         true,
			UseForSymbols:   false,
			BatchSize:       1000,
			SymbolsPerShard: 1000,
			SubscribePause:  110 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
	},
	"kucoin": {
		Spot: ExchangeConfig{
			Enabled:         true,
			UseForSymbols:   true,
			BatchSize:       100,
			SymbolsPerShard: 200,
			SubscribePause:  500 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
		Swap: ExchangeConfig{
			Enabled:         false,
			UseForSymbols:   false,
			BatchSize:       100,
			SymbolsPerShard: 300,
			SubscribePause:  200 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
	},
	"bitmart": {
		Spot: ExchangeConfig{
			Enabled:         true,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 50,
			SubscribePause:  200 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
		Swap: ExchangeConfig{
			Enabled:         false,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 50,
			SubscribePause:  200 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
	},
	"mexc": {
		Spot: ExchangeConfig{
			Enabled:         true,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 30,
			SubscribePause:  100 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
		Swap: ExchangeConfig{
			Enabled:         false,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 30,
			SubscribePause:  100 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
	},
	"woo": {
		Spot: ExchangeConfig{
			Enabled:         true,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 40,
			SubscribePause:  100 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
		Swap: ExchangeConfig{
			Enabled:         true,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 40,
			SubscribePause:  100 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
	},
}

func getConfig(exchangeName, marketType string) ExchangeConfig {
	exchangeName = canonicalExchangeName(exchangeName)
	marketType = strings.ToLower(marketType)

	policy, ok := exchangePolicies[exchangeName]
	if !ok {
		policy = defaultExchangePolicy
	}

	cfg := policy.configForMarket(marketType)
	if cfg.BatchSubscribePause > 0 && cfg.SubscribePause == 0 {
		cfg.SubscribePause = cfg.BatchSubscribePause
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 1
	}
	if cfg.SymbolsPerShard <= 0 {
		cfg.SymbolsPerShard = 1
	}
	if cfg.NewShardPause <= 0 {
		cfg.NewShardPause = defaultExchangePolicy.configForMarket(marketType).NewShardPause
	}
	if cfg.SubscribePause <= 0 {
		cfg.SubscribePause = defaultExchangePolicy.configForMarket(marketType).SubscribePause
	}
	return cfg
}
