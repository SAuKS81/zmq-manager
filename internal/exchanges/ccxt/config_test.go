//go:build ccxt
// +build ccxt

package ccxt

import (
	"testing"
	"time"
)

func TestGetConfigFallsBackToConservativeDefault(t *testing.T) {
	cfg := getConfig("totally-unknown-exchange", "spot")

	if !cfg.Enabled {
		t.Fatal("expected default config to stay enabled for unknown exchange")
	}
	if cfg.UseForSymbols {
		t.Fatal("expected default config to disable batch-for-symbols mode")
	}
	if cfg.BatchSize != 1 {
		t.Fatalf("expected default batch size 1, got %d", cfg.BatchSize)
	}
	if cfg.SymbolsPerShard != 1 {
		t.Fatalf("expected default symbols per shard 1, got %d", cfg.SymbolsPerShard)
	}
	if cfg.SubscribePause != 500*time.Millisecond {
		t.Fatalf("expected default subscribe pause 500ms, got %s", cfg.SubscribePause)
	}
	if cfg.NewShardPause != 2500*time.Millisecond {
		t.Fatalf("expected default new shard pause 2500ms, got %s", cfg.NewShardPause)
	}
	if cfg.ReconnectBaseDelay != 10*time.Second {
		t.Fatalf("expected default reconnect base delay 10s, got %s", cfg.ReconnectBaseDelay)
	}
	if cfg.ReconnectMaxDelay != 90*time.Second {
		t.Fatalf("expected default reconnect max delay 90s, got %s", cfg.ReconnectMaxDelay)
	}
	if cfg.ReconnectJitter != 3*time.Second {
		t.Fatalf("expected default reconnect jitter 3s, got %s", cfg.ReconnectJitter)
	}
}

func TestGetConfigUsesExchangeOverride(t *testing.T) {
	cfg := getConfig("bitget", "swap")

	if !cfg.UseForSymbols {
		t.Fatal("expected bitget swap config to use batch-for-symbols mode")
	}
	if cfg.BatchSize != 20 {
		t.Fatalf("expected bitget swap batch size 20, got %d", cfg.BatchSize)
	}
	if cfg.SubscribePause != 500*time.Millisecond {
		t.Fatalf("expected bitget subscribe pause 500ms, got %s", cfg.SubscribePause)
	}
}

func TestGetConfigSupportsHuobiAlias(t *testing.T) {
	htxCfg := getConfig("htx", "spot")
	huobiCfg := getConfig("huobi", "spot")

	if htxCfg != huobiCfg {
		t.Fatalf("expected huobi alias to resolve to htx policy, got %#v vs %#v", huobiCfg, htxCfg)
	}
}

func TestGetConfigIncludesBitmartPolicy(t *testing.T) {
	cfg := getConfig("bitmart", "spot")

	if !cfg.Enabled {
		t.Fatal("expected bitmart spot config to be enabled")
	}
	if !cfg.UseForSymbols {
		t.Fatal("expected bitmart spot config to use batch-for-symbols mode")
	}
	if cfg.BatchSize != 20 {
		t.Fatalf("expected bitmart batch size 20, got %d", cfg.BatchSize)
	}
	if cfg.SymbolsPerShard != 80 {
		t.Fatalf("expected bitmart symbols per shard 80, got %d", cfg.SymbolsPerShard)
	}
	if cfg.SubscribePause != 500*time.Millisecond {
		t.Fatalf("expected bitmart subscribe pause 500ms, got %s", cfg.SubscribePause)
	}
	if cfg.NewShardPause != 2500*time.Millisecond {
		t.Fatalf("expected bitmart new shard pause 2500ms, got %s", cfg.NewShardPause)
	}
	if cfg.ReconnectBaseDelay != 15*time.Second {
		t.Fatalf("expected bitmart reconnect base delay 15s, got %s", cfg.ReconnectBaseDelay)
	}
	if cfg.ReconnectMaxDelay != 120*time.Second {
		t.Fatalf("expected bitmart reconnect max delay 120s, got %s", cfg.ReconnectMaxDelay)
	}
	if cfg.ReconnectJitter != 5*time.Second {
		t.Fatalf("expected bitmart reconnect jitter 5s, got %s", cfg.ReconnectJitter)
	}
}

func TestGetConfigIncludesMexcPolicy(t *testing.T) {
	cfg := getConfig("mexc", "spot")

	if !cfg.Enabled {
		t.Fatal("expected mexc spot config to be enabled")
	}
	if cfg.UseForSymbols {
		t.Fatal("expected mexc spot config to stay in single-watch mode")
	}
	if cfg.SymbolsPerShard != 30 {
		t.Fatalf("expected mexc symbols per shard 30, got %d", cfg.SymbolsPerShard)
	}
	if cfg.SubscribePause != 500*time.Millisecond {
		t.Fatalf("expected mexc subscribe pause 500ms, got %s", cfg.SubscribePause)
	}
}

func TestGetConfigDisablesBybitOrderBookUnwatch(t *testing.T) {
	spotCfg := getConfig("bybit", "spot")
	swapCfg := getConfig("bybit", "swap")

	if spotCfg.SupportsOrderBookUnwatch || spotCfg.SupportsOrderBookBatchUnwatch {
		t.Fatalf("expected bybit spot orderbook unwatch to stay disabled, got %#v", spotCfg)
	}
	if swapCfg.SupportsOrderBookUnwatch || swapCfg.SupportsOrderBookBatchUnwatch {
		t.Fatalf("expected bybit swap orderbook unwatch to stay disabled, got %#v", swapCfg)
	}
}

func TestGetConfigReflectsKucoinTradeBatchLifecycle(t *testing.T) {
	cfg := getConfig("kucoin", "spot")

	if !cfg.UseForSymbols {
		t.Fatal("expected kucoin spot to stay in batch mode")
	}
	if !cfg.OneTradeBatchPerShard {
		t.Fatal("expected kucoin spot to enforce one trade batch per shard")
	}
	if !cfg.SupportsTradeBatchUnwatch {
		t.Fatalf("expected kucoin spot batch unwatch to stay enabled, got %#v", cfg)
	}
	if cfg.RecycleExchangeOnTradeChange {
		t.Fatalf("expected kucoin spot to keep exchange reuse on trade list changes, got %#v", cfg)
	}
}
