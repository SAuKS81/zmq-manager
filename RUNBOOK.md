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
