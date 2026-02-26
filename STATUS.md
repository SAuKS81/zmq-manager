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
- Deterministisches Test-Harness vorbereitet (Replay statt Live-WS):
  - neuer lokaler Replay-Server: `cmd/wsreplay`
  - WS-URL-Overrides per Env fuer alle nativen Exchanges:
    - Bybit: `BYBIT_SPOT_WS_URL`, `BYBIT_LINEAR_WS_URL`
    - Binance: `BINANCE_SPOT_WS_URL`, `BINANCE_FUTURES_WS_URL`
    - Bitget: `BITGET_WS_URL`
- Revertete Experimente (nicht behalten):
  - `c776e81` revert von partial OB message-shape decode
  - `3ddc220` revert single-client cache skip/header change
  - `eeb7e89` revert direct parse from pooled OB buffer (`d727e9f`)

## Offen

- Baseline v2 (quiet regime) final einfrieren:
  - bisher starke Run-Streuung, daher noch kein stabiler Referenzwert
  - bis dahin nur A/B-Paarvergleich fuer keep/revert
- Replay-Profil als zweite Referenzspur etablieren (konstant), Live-Run bleibt Realitaets-Check
- `io.ReadAll` / TLS/WS read-path weiter reduzieren (isolierte Patches)
- Bitget Lastbild technisch klaeren (trade/s bei 1000 subs einordnen)
- CCXT Haertung finalisieren (BadSymbol/Checksum robust, kein panic)
- `baseline_ingest.sh` tar-Warnung beseitigen (`file changed as we read it`)
- Mutex/Block-Profil als regulaeren Kontrolllauf nachziehen

## Aktueller Arbeitsmodus

- Run-Workflow bleibt unveraendert nach Vorgabe von Stephan (`baseline_ingest.sh` Block)
- Pro Performance-Patch:
  1. Commit pinnen
  2. Run ausfuehren
  3. gegen Referenz bewerten (`keep`/`revert`)
