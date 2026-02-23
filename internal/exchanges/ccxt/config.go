package ccxt

import "time"

// ExchangeConfig definiert das Verhalten für einen spezifischen Markt-Typ einer Börse.
type ExchangeConfig struct {
	Enabled         bool
	UseForSymbols   bool
	BatchSize       int
	// SymbolsPerShard gilt jetzt für BEIDE Modi:
	// - Im Batch-Modus: Max. Symbole pro Shard/Verbindung insgesamt.
	// - Im Single-Modus: Max. Einzel-Watcher pro Shard/Verbindung.
	SymbolsPerShard int
	SubscribePause  time.Duration
	BatchSubscribePause time.Duration
	NewShardPause time.Duration
}

// marketConfigs ist die zentrale Konfigurations-Map.
// Struktur: map[exchangeName] -> map[marketType] -> ExchangeConfig
var marketConfigs = map[string]map[string]ExchangeConfig{
	"mexc": {
		"spot": {
			Enabled:         true,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 30, // Aber bis zu 40 pro Verbindung insgesamt
			SubscribePause:  100 * time.Millisecond,
		},
		"swap": {
			Enabled:         false,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 30,
			SubscribePause:  100 * time.Millisecond,
		},
	},
	"binance": {
		"spot": {
			Enabled:         true,
			UseForSymbols:   true, // Setze auf false, um den neuen Modus zu testen
			BatchSize:       200,
			SymbolsPerShard: 200,   // Erlaube 100 Einzel-Streams pro Verbindung
			SubscribePause:  100 * time.Millisecond,
		},
		"swap": {
			Enabled:         true,
			UseForSymbols:   true, 
			BatchSize:       200,
			SymbolsPerShard: 200,
			SubscribePause:  250 * time.Millisecond,
		},
	},
	"bybit": {
		// Bybit wird über den nativen Handler abgedeckt, aber als Beispiel:
		"spot": {
			Enabled:         true,
			UseForSymbols:   true,
			BatchSize:       10, // Bybit erlaubt nur 10 Themen pro `op`
			SymbolsPerShard: 40, // Aber bis zu 40 pro Verbindung insgesamt
			SubscribePause:  100 * time.Millisecond,
		},
		"swap": {
			Enabled:         true,
			UseForSymbols:   true,
			BatchSize:       10,
			SymbolsPerShard: 40,
			SubscribePause:  100 * time.Millisecond,
		},
	},
	"kucoin": {
		"spot": {
			Enabled:         true,
			UseForSymbols:   true, // Setze auf false, um den neuen Modus zu testen
			BatchSize:       100,
			SymbolsPerShard: 200,   // Erlaube 100 Einzel-Streams pro Verbindung
			SubscribePause:  500 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
		"swap": {
			Enabled:         false,
			UseForSymbols:   false, 
			BatchSize:       100,
			SymbolsPerShard: 300,
			SubscribePause:  200 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
	},
	"coinex": {
		"spot": {
			Enabled:         true,
			UseForSymbols:   true, // Setze auf false, um den neuen Modus zu testen
			BatchSize:       500,
			SymbolsPerShard: 1000,   // Erlaube 100 Einzel-Streams pro Verbindung
			SubscribePause:  100 * time.Millisecond,
		},
		"swap": {
			Enabled:         false,
			UseForSymbols:   true, 
			BatchSize:       500,
			SymbolsPerShard: 1000,
			SubscribePause:  250 * time.Millisecond,
		},
	},
	"htx": {
		"spot": {
			Enabled:         true,
			UseForSymbols:   false, 
			BatchSize:       1000,
			SymbolsPerShard: 1000,
			SubscribePause:  110 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
		"swap": {
			Enabled:         true,
			UseForSymbols:   false, 
			BatchSize:       1000,
			SymbolsPerShard: 1000,
			SubscribePause:  110 * time.Millisecond,
			NewShardPause:   1100 * time.Millisecond,
		},
	},
	"bitmart": {
		"spot": {
			Enabled:         true,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 100, // Aber bis zu 40 pro Verbindung insgesamt
			SubscribePause:  50 * time.Millisecond,
		},
		"swap": {
			Enabled:         true,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 100,
			SubscribePause:  50 * time.Millisecond,
		},
	},
	"bitget": {
		"spot": {
			Enabled:             true,
			UseForSymbols:       true,
			BatchSize:           20,
			SymbolsPerShard:     20,
			BatchSubscribePause: 500 * time.Millisecond,
			NewShardPause:       1100 * time.Millisecond, // WICHTIG: > 1 Sekunde!
		},
		"swap": {
			Enabled:             true,
			UseForSymbols:       true,
			BatchSize:           20,
			SymbolsPerShard:     20,
			BatchSubscribePause: 500 * time.Millisecond,
			NewShardPause:       1100 * time.Millisecond,
		},
	},
	"woo": {
		"spot": {
			Enabled:         true,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 40, // Aber bis zu 40 pro Verbindung insgesamt
			SubscribePause:  100 * time.Millisecond,
		},
		"swap": {
			Enabled:         true,
			UseForSymbols:   false,
			BatchSize:       1,
			SymbolsPerShard: 40,
			SubscribePause:  100 * time.Millisecond,
		},
	},
	// Ein Standard-Fallback für alle nicht explizit konfigurierten Märkte.
	"default": {
		"spot": {
			UseForSymbols:   false, // Standardmäßig den sicheren Einzel-Modus verwenden
			SymbolsPerShard: 1,     // Irrelevant, aber zur Vollständigkeit
			BatchSize:       1,
			SubscribePause:  200 * time.Millisecond,
		},
		"swap": {
			UseForSymbols:   false,
			SymbolsPerShard: 1,
			BatchSize:       1,
			SubscribePause:  200 * time.Millisecond,
		},
	},
}

// getConfig holt die spezifische Konfiguration oder den passenden Fallback.
func getConfig(exchangeName, marketType string) ExchangeConfig {
	if exchangeConf, ok := marketConfigs[exchangeName]; ok {
		if marketConf, ok := exchangeConf[marketType]; ok {
			return marketConf // Spezifische Konfig gefunden
		}
		// Fallback auf den "spot"-Markt der Börse, falls vorhanden
		if defaultMarket, ok := exchangeConf["spot"]; ok {
			return defaultMarket
		}
	}
	// Letzter Fallback auf die globalen Default-Werte
	return marketConfigs["default"][marketType]
}