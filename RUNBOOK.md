# RUNBOOK

## Zweck

Linux/Vultr-first Ablauf fuer reproduzierbare Ingest-Baselines.

## Voraussetzungen

1. Broker laeuft mit pprof/metrics auf `http://127.0.0.1:6060`
2. Symbol-Liste vorhanden (z. B. mit Generator):

```bash
chmod +x ./scripts/gen_symbols_binance_spot.sh ./scripts/baseline_ingest.sh
./scripts/gen_symbols_binance_spot.sh --limit 1000 --out ./scripts/symbols_binance_spot_top1000.txt
```

## Baseline ausfuehren

```bash
./scripts/baseline_ingest.sh \
  --run-name ingest200 \
  --symbols-file ./scripts/symbols_binance_spot_top1000.txt \
  --symbols-limit 200 \
  --exchanges binance_native,bybit_native \
  --duration 60s
```

Hinweis zur Orderbook-Tiefe:

- `--ob-depth` wird pro Exchange auf die naechste dokumentierte native Stufe normalisiert
- Beispiele:
  - Binance native: `5/10/20`
  - Bybit native: `1/50/200/1000`

## Mutex/Block Profiling (optional)

Broker mit Profiling-Flags starten:

```bash
go run ./cmd/broker/main.go --pprof-block-rate 1 --pprof-mutex-fraction 5
```

Dann die Profile ziehen:

```bash
curl -sS http://127.0.0.1:6060/debug/pprof/block > block.pprof
curl -sS http://127.0.0.1:6060/debug/pprof/mutex > mutex.pprof
```

## Artefakte

Run-Verzeichnis:

- `~/pprof_runs/<timestamp>_<run-name>/`

Inhalt:

- `cpu.pprof`, `heap.pprof`, `allocs.pprof`
- `metrics_pre.txt`, `metrics_post.txt`
- `smoke.log`, `meta.json`, `bundle.tar.gz`

## Gate lesen

In `meta.json`:

- `PASS`
- `FAIL_DROPS_DETECTED`

Bei `FAIL_DROPS_DETECTED` wird nicht abgebrochen, aber der Run gilt als Gate-Fail.
