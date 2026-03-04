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
	if cfg.SubscribePause != 300*time.Millisecond {
		t.Fatalf("expected default subscribe pause 300ms, got %s", cfg.SubscribePause)
	}
	if cfg.NewShardPause != 1200*time.Millisecond {
		t.Fatalf("expected default new shard pause 1200ms, got %s", cfg.NewShardPause)
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
	if cfg.BatchSize != 1 {
		t.Fatalf("expected bitmart batch size 1, got %d", cfg.BatchSize)
	}
	if cfg.SymbolsPerShard != 50 {
		t.Fatalf("expected bitmart symbols per shard 50, got %d", cfg.SymbolsPerShard)
	}
	if cfg.SubscribePause != 200*time.Millisecond {
		t.Fatalf("expected bitmart subscribe pause 200ms, got %s", cfg.SubscribePause)
	}
}
