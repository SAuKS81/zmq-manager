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
