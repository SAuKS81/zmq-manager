# Phase 1.5 Baseline Tooling

Dieses Paket liefert reproduzierbare Ingest-Baseline-Runs mit:
- `clients/smoke_client.go`
- `scripts/baseline_ingest.sh`
- `scripts/gen_symbols_binance_spot.sh`
- `scripts/gen_symbols_bybit_spot.sh`
- `scripts/gen_symbols_bybit_swap.sh`
- `scripts/gen_symbols_bitget_spot.sh`
- `scripts/gen_symbols_bitget_swap.sh`

## Prerequisite: Symbol List

Für frische Server:

```bash
chmod +x ./scripts/gen_symbols_binance_spot.sh ./scripts/gen_symbols_bybit_spot.sh ./scripts/gen_symbols_bybit_swap.sh ./scripts/gen_symbols_bitget_spot.sh ./scripts/gen_symbols_bitget_swap.sh ./scripts/baseline_ingest.sh
./scripts/gen_symbols_binance_spot.sh --limit 1000 --out ./scripts/symbols_binance_spot_top1000.txt
```

Danach kann direkt mit `--symbols-limit 200/500/1000` gefahren werden.

Bybit:

```bash
./scripts/gen_symbols_bybit_spot.sh --limit 1000 --out ./scripts/symbols_bybit_spot_top1000.txt
./scripts/gen_symbols_bybit_swap.sh --limit 1000 --out ./scripts/symbols_bybit_swap_top1000.txt
```

Bitget:

```bash
./scripts/gen_symbols_bitget_spot.sh --limit 1000 --out ./scripts/symbols_bitget_spot_top1000.txt
./scripts/gen_symbols_bitget_swap.sh --limit 1000 --out ./scripts/symbols_bitget_swap_top1000.txt
```

CCXT (research):

```bash
go run -tags ccxt ./cmd/broker/main.go --pprof-block-rate 1 --pprof-mutex-fraction 5
```

Für CCXT-Baselines `--exchanges` ohne `_native` verwenden (z. B. `binance,bybit,bitget`), Symbolformat bleibt CCXT-style (`BTC/USDT`, `BTC/USDT:USDT`).

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
  --symbols-file ./scripts/symbols_binance_spot_top1000.txt \
  --symbols-limit 200 \
  --exchanges binance_native,bybit_native \
  --duration 60s
```

### ingest500

```bash
./scripts/baseline_ingest.sh \
  --run-name ingest500 \
  --symbols-file ./scripts/symbols_binance_spot_top1000.txt \
  --symbols-limit 500 \
  --exchanges binance_native,bybit_native \
  --duration 60s
```

### ingest1000

```bash
./scripts/baseline_ingest.sh \
  --run-name ingest1000 \
  --symbols-file ./scripts/symbols_binance_spot_top1000.txt \
  --symbols-limit 1000 \
  --exchanges binance_native,bybit_native \
  --duration 60s
```
