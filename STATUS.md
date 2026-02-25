# STATUS

## Umgesetzt

- Referenz-Tooling-Stand: `578b9f2` (Phase 1.5 Baseline Tooling)
- P0-1/P0-2: race-fix client registry + typed request decoding
- P0-3: CCXT shutdown deadlock fix
- P0-5: disconnect cleanup in subscription maps
- Single writer fuer ROUTER send path
- Metrics MVP (`/metrics`, ingest/publish/drop + processing histogram)
- Phase 1.5 tooling: skalierbarer smoke client + baseline ingest script

## Offen

- Backpressure-Verhalten unter ingest500+ stabilisieren
- Mutex/block profile als Standardlauf in Baseline integrieren
- Ingest scaling 1000+ Streams ueber mehrere Exchanges absichern
- Drop-Ursachen weiter reduzieren (`buffer_full`)
- Gate-Report-Automation fuer Vergleich mehrerer Runs
- Prio1 Gate explizit ausweisen: Trades+ToB drops_delta = 0 bei ingest500

## Letzte Messungen

Stand: 2026-02-24

- ingest200: PASS
- ingest500: FAIL_DROPS_DETECTED (`buffer_full` bei `trade` und `ob_update`)

Run-Artefakte liegen unter:

- `~/pprof_runs/<timestamp>_ingest200/`
- `~/pprof_runs/<timestamp>_ingest500/`

## Naechste Schritte (Top 5)

1. ingest500 Drops im Single-Writer/Buffer-Pfad eingrenzen
2. fixed baseline matrix: 200/500/1000 mit identischer Symbolbasis
3. mutex + block pprof je Run sichern
4. per-run Gate Summary in `meta.json` erweitern (ohne Heuristik)
5. ingest1000 Multi-Exchange Lauf als reproduzierbaren Referenz-Run etablieren
