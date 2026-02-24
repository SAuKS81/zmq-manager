# Phase 1.5 Baseline Tooling

Dieses Paket liefert reproduzierbare Ingest-Baseline-Runs mit:
- `clients/smoke_client.go`
- `scripts/baseline_ingest.sh`

## Run-Output

Alle Runs werden unter folgendem Pfad abgelegt:
- `~/pprof_runs/<timestamp>_<run-name>/`

Artefakte je Run:
- `cpu.pprof`
- `heap.pprof`
- `allocs.pprof`
- `metrics_pre.txt`
- `metrics_post.txt`
- `smoke.log`
- `meta.json`
- `bundle.tar.gz`

## Gate

`baseline_ingest.sh` prüft nach dem Lauf `zmq_dropped_messages_total`.

In `meta.json` wird gesetzt:
- `"gate_status": "PASS"` oder
- `"gate_status": "FAIL_DROPS_DETECTED"`

## Beispielaufrufe

### ingest200

```bash
./scripts/baseline_ingest.sh \
  --run-name ingest200 \
  --symbols-file ./scripts/symbols_example_50.txt \
  --symbols-limit 200 \
  --exchanges binance_native,bybit_native \
  --duration 60s
```

### ingest500

```bash
./scripts/baseline_ingest.sh \
  --run-name ingest500 \
  --symbols-file ./symbols_top1000.txt \
  --symbols-limit 500 \
  --exchanges binance_native,bybit_native \
  --duration 60s
```

### ingest1000

```bash
./scripts/baseline_ingest.sh \
  --run-name ingest1000 \
  --symbols-file ./symbols_top1000.txt \
  --symbols-limit 1000 \
  --exchanges binance_native,bybit_native \
  --duration 60s
```
