# STATUS

## Stand (2026-02-26)

Aktiver Branch: `phase1.5-baseline-tooling`

## Abgehakt

- P0-1/P0-2 umgesetzt: race-fix client registry + typed request decoding
- P0-3 umgesetzt: CCXT shutdown deadlock fix
- P0-5 umgesetzt: disconnect cleanup in subscription maps
- Single-writer fuer ROUTER send path umgesetzt
- Metrics MVP (`/metrics`) + processing histogram umgesetzt
- Baseline Tooling umgesetzt: smoke client + `baseline_ingest.sh`
- Prio-Pfad stabilisiert:
  - P1/P2 Send-Priorisierung und P2 latest-only Verhalten
  - publish nur bei erfolgreicher send-Weitergabe
  - drops als Delta (post-pre) im Baseline-Report
- Performance keep-Commits:
  - `5a89fb2` bybit `mapToTopLevels` bounded slice path
  - `f7e5f19` in-batch P2 collapse vor encode
  - `1ff06de` msgpack buffer pooling im broker encode hot path
  - `dfb4c03` msgpack encoder context pooling
  - `967b100` bybit trade read-loop: string alloc cut + goccy decode
  - `32ae4d2` bybit OB read via `NextReader` + pooled buffer
  - `afbeeec` batch C: binance+bitget WS read path auf `NextReader` + pooled buffer
  - `e1da05b` bybit OB hot path: `GoTimestamp` aus ingest ableiten statt `time.Now()` pro Update
  - `5d5eb8c` bybit WS read path: pooled buffer ohne extra `[]byte`-Kopie (trade + ob)
- Deterministisches Test-Harness vorbereitet (Replay statt Live-WS):
  - neuer lokaler Replay-Server: `cmd/wsreplay`
  - WS-URL-Overrides per Env fuer alle nativen Exchanges:
    - Bybit: `BYBIT_SPOT_WS_URL`, `BYBIT_LINEAR_WS_URL`
    - Binance: `BINANCE_SPOT_WS_URL`, `BINANCE_FUTURES_WS_URL`
    - Bitget: `BITGET_WS_URL`
- Replay-Referenz (Bybit native, 500/500, 60s) festgezogen auf Commit `660574a`:
  - gueltige Runs: `ccxt_repro_replay_fix_1`, `_2`, `_4` (alle `PASS`, Drops `0`)
  - ausgeklammert: `ccxt_repro_replay_fix_3` (Smoke `ob/s=0.00`)
  - Median `alloc_space total`: ~`744 MB`
  - Median `cpu total samples (30s)`: ~`9.39s`
  - typische Smoke-Rate: trades ~`9.7k/s`, ob ~`39-40/s`
- Replay-Referenz (Binance native, replay slow `--tick 1s`) festgezogen auf Commit `660574a`:
  - Runs: `binance_replay_ref_slow_1..3` (alle `PASS`, Drops `0`)
  - typische Smoke-Rate: trades ~`972/s`, ob ~`965-972/s`
  - Hinweis: bei `--tick 100ms` bewusst Overload (FAIL_DROPS_DETECTED) und nicht als Referenz genutzt
- Replay-Referenz (Bitget native, replay slow `--tick 1s`) festgezogen auf Commit `660574a`:
  - Runs: `bitget_replay_ref_slow_1..3` (alle `PASS`, Drops `0`)
  - typische Smoke-Rate: trades ~`972/s`
  - `ob/s=0.00` by design (bitget_native aktuell trade-only Pfad)
- Batch-C Validierung (Commit `afbeeec`) abgeschlossen:
  - Binance clean runs: `binance_replay_post_batchc_clean_1..3` (alle `PASS`, Drops `0`)
  - Bitget clean runs: `bitget_replay_post_batchc_clean_1..3` (alle `PASS`, Drops `0`, `ob/s=0.00` by design)
- Bybit Clock-Hotpath Validierung (Commit `e1da05b`):
  - Smoke-Run `bybit_replay_post_clockopt_smoke_1` (`PASS`, Drops `0`)
  - Entscheidung: `KEEP` (Owner-Entscheid)
- Bybit No-Copy Read-Path Validierung (Commit `5d5eb8c`):
  - Smoke-Run `bybit_replay_post_nocopy_smoke_1` (`PASS`, Drops `0`)
  - Entscheidung: `KEEP` (Owner-Entscheid)
- Revertete Experimente (nicht behalten):
  - `c776e81` revert von partial OB message-shape decode
  - `3ddc220` revert single-client cache skip/header change
  - `eeb7e89` revert direct parse from pooled OB buffer (`d727e9f`)

## Offen

- Baseline v2 (quiet regime) final einfrieren:
  - Live-Runs bleiben volatil; Bewertung weiter als A/B-Paarvergleich
- Replay-Profil als zweite Referenzspur:
  - ist jetzt nutzbar als deterministische Neben-Referenz fuer Bybit/Binance/Bitget
- `io.ReadAll` / TLS/WS read-path weiter reduzieren (naechster Fokus: broker encode/context hotpath)
- Bitget Lastbild technisch klaeren (trade/s bei 1000 subs einordnen, OB spaeter wenn OB-Pfad vorhanden)
- CCXT Haertung finalisieren (BadSymbol/Checksum robust, kein panic)
- `baseline_ingest.sh` tar-Warnung beseitigen (`file changed as we read it`)
- Mutex/Block-Profil als regulaeren Kontrolllauf nachziehen

## Aktueller Arbeitsmodus

- Run-Workflow bleibt unveraendert nach Vorgabe von Stephan (`baseline_ingest.sh` Block)
- Pro Performance-Patch:
  1. Commit pinnen
  2. Run ausfuehren
  3. gegen Referenz bewerten (`keep`/`revert`)
