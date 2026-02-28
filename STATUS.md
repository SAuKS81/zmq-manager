# STATUS

## Stand (2026-02-28)

Aktiver Branch: `phase1.6-stream-lifecycle-hardening`

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
  - `afdbbbf` broker send path: P1 OB innerhalb Client-Batch senden (weniger Send/Context-Overhead)
  - `c700692` broker encode path: Header-Frame-Reuse statt per-send `[]byte(...)` Alloc
  - `71009b5` binance WS read pool: Buffer pre-grow 32KB (weniger `bytes.growSlice`)
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
- Binance P1-Batch Send-Optimierung (Commit `afdbbbf`):
  - Smoke-Run `binance_replay_post_p1batch_smoke_1` (`PASS`, Drops `0`)
  - Entscheidung: `KEEP` (Owner-Entscheid)
- Broker Header-Alloc Cut (Commit `c700692`):
  - Smoke-Run `binance_replay_post_headeralloc_smoke_1` (`PASS`, Drops `0`)
  - Entscheidung: `KEEP` (Owner-Entscheid)
- Binance Read-Buffer Pregrow (Commit `71009b5`):
  - Smoke-Run `binance_replay_post_readbuf32k_smoke_1` (`PASS`, Drops `0`)
  - Entscheidung: `KEEP` (Owner-Entscheid)
- Performance-Pfad `broker encode/context hotpath` (Runde 2026-02-26) abgeschlossen:
  - Ergebnis: nur `71009b5` als zusaetzlicher `KEEP`
  - alle weiteren Kandidaten dieser Runde wurden nach Smoke-Vergleich reverted
- Disconnect-/Cleanup-Kette verifiziert und gehaertet:
  - `b8637d1`: smoke shutdown graceful + `DISCONNECT_SENT`
  - `cf37c1e`: disconnect propagiert echte unsubs + sofortiger Client-Cleanup
  - `3c83bb7`: OB-unsubscribe robust bei fehlender `depth` (disconnect-Pfad)
  - Verifikation: `v2_disconnect_verify_fix2_1` (`PASS`, Drops `0`, `DISCONNECT_SENT`, kein Timeout-Abbau mehr noetig)
- Live-Serienlauf ohne Broker-Neustart verifiziert (mit Idle-Gate):
  - `v2_live_freeze_bybit_clean_1..3` jeweils `PASS`, Drops `0`
  - Entscheidender Ablaufpunkt: vor dem naechsten Run erst Broker-Idle bestaetigen (`[STATS] Trades: 0 | OrderBooks: 0`, mehrfach in Folge)
  - Heap-Bestaetigung ueber `inuse_space` nach Idle: ~`33.8MB` -> `33.9MB` -> `35.4MB` (stabil, kein klarer Leak-Trend)
  - Hinweis: `alloc_space` bleibt kumulativ und ist fuer prozesslange Serienlaeufe nicht als Leak-Indikator geeignet
- Binance Live-Freeze (final, hohe Marktvolatilitaet) verifiziert:
  - `v2_live_freeze_binance_clean_1..3` jeweils `PASS`, Drops `0` auf Commit `893cbee`
  - Unsubscribe-Pfad fixiert (Depth-Wildcard bei remove, kein `depth=20` Override bei unsubscribe)
  - Hinweis: hoehere trades/s durch Marktphase; Vergleich primar ueber Gate/Drops/Hotspot-Stabilitaet
- Bitget Live-Freeze (final) verifiziert:
  - `v2_live_freeze_bitget_clean_1..3` jeweils `PASS`, Drops `0` auf Commit `038b1af`
  - Unsubscribe-Haertung aktiv: serialisierter remove-Pfad + gebuendelte Unsub-Batches
  - `ob/s=0` bleibt by design (aktueller bitget-native Pfad trade-only)
  - Bitget-Orderbooks sind bewusst nach hinten priorisiert; in absehbarer Zeit kein Implementierungsbedarf
- Replay-Referenzspur final festgeschrieben (tick `1s`, Commit `038b1af`):
  - Bybit replay: `PASS`, Drops `0`, stabile Rate (`trades~972/s`, `ob~40/s`)
  - Binance replay: `PASS`, Drops `0`, stabile Rate (`trades~972/s`, `ob~972/s`)
  - Bitget replay: als Referenz `PASS` festgeschrieben (`trades~972/s`, `ob=0 by design`);
    frueher Einzel-Ausreisser mit kleinen Drops dokumentiert, aber Gesamtbewertung stabil/akzeptiert
- CCXT-Haertung finalisiert (Commit `72a0dc6`):
  - `BadSymbol`/missing-symbol Erkennung robuster (mehrere Fehlertext-Varianten)
  - Normalize-Pfade gehaertet (defensive Feldpruefung + Panic-Guard)
  - Worker behandeln Normalize-Fehler kontrolliert (Warn-Log statt stilles Schlucken/Panik)
  - Verifikation: `v2_ccxt_hardening_smoke_1` (`PASS`, Drops `0`, `DISCONNECT_SENT`)
- Baseline-Bundle/Tar-Warnung behoben (Commit `c73490b`):
  - deterministische Bundle-Erstellung per Datei-Snapshot
  - keine `tar: .: file changed as we read it` Warnung mehr im Verifikationslauf
- Legacy-`clients/`-Ordner bereinigt:
  - alte Go/Python-Testclients bewusst geloescht
  - neuer schlanker `clients/smoke_client.go` fuer Baseline/Verifikation neu aufgebaut
  - Repo-Build/Test damit wieder sauber
- P7-3 Client-Signalierung erster produktiver Schnitt abgeschlossen:
  - Broker verteilt `StreamStatusEvent` gezielt an abonnierte Clients
  - Multi-Symbol-Status-Events (`symbol` oder `symbols`) werden korrekt geroutet
  - `clients/smoke_client.go` loggt und zaehlt `stream_reconnecting`, `stream_restored`, `stream_unsubscribe_failed`, `stream_force_closed`
  - lokal vorlaeufig abgenommen; finale Beobachtung im Beta-Deployment
- P7-4 Native Adapter Lifecycle-Haertung erster produktiver Schnitt abgeschlossen:
  - Binance: verifizierter Ack/Nack + Retry + gezielter Recycle
  - Bybit: verifizierter Ack/Nack + Retry + gezielter Recycle; Command-Chunking marktspezifisch (`spot=10`, `swap` bis Shard-Groesse)
  - Bitget: verifizierter paced Pfad mit globaler Sendetaktung und Empty-Shard-Cleanup nach finalem Unsubscribe-Flush
- P7-2 Selektiver Shard-Recycle abgeschlossen:
  - native Reconnect-/Recycle-Pfade ziehen Resubscribe-Ziellisten jetzt explizit aus `desired*` statt implizit aus gemischtem Zustand
  - damit werden nur noch gewuenschte Streams/Symbole/Themen nach einem Recycle wieder aufgebaut
  - stale `active*`-Eintraege koennen entfernte Streams nicht mehr versehentlich zurueckbringen
  - abgesichert durch Unit-Tests fuer Binance Trade/OB und Bybit Trade/OB Snapshot-Pfade
- P7-6 Metriken und Logs abgeschlossen:
  - neue Lifecycle-Counter unter `/metrics`:
    - `zmq_unsubscribe_attempts_total`
    - `zmq_unsubscribe_failures_total`
    - `zmq_forced_shard_recycles_total`
    - `zmq_stream_reconnects_total`
    - `zmq_stream_restore_success_total`
  - Bybit, Bitget und Binance haengen ihre nativen Lifecycle-Pfade jetzt an diese Counter
  - neue strukturierte Lifecycle-Logs im Format `STREAM_LIFECYCLE ...` mit `event`, `exchange`, `shard`, `market_type`, `data_type`, `symbols`, `attempt`, `reason`, `message`
- Mutex/Block-Kontrolllauf standardisiert (abgeschlossen):
  - Broker-Start fuer Kontrolllauf immer mit `--pprof-block-rate 1 --pprof-mutex-fraction 5`
  - Kontrollprofil je Referenzpfad: `profile?seconds=30`, `mutex`, `block`
  - Mindestfrequenz: nach jedem Keep-Commit im Hotpath oder mindestens 1x taeglich im aktiven Tuning
  - Regression-Trigger:
    - `mutex` oder `block` Top-Flat +30% gg. letzter Referenz bei gleicher Lastklasse
    - oder neues Lock/Block-Hotspot >10% Flat in Top-Ansicht
  - Bei Trigger: kein Keep ohne Gegenprobe (zweiter Run) oder Root-Cause-Notiz
- Revertete Experimente (nicht behalten):
  - `c776e81` revert von partial OB message-shape decode
  - `3ddc220` revert single-client cache skip/header change
  - `eeb7e89` revert direct parse from pooled OB buffer (`d727e9f`)
  - `0b99be2` revert von `efa37bf` (OB batch scratch pool)
  - `f62bcd6` revert von `b4d7c3b` (single OB JSON envelope)
  - `dda2c9d` revert von `f967e8f` (send latency unix nanos)
  - `171aa1f` revert von `0815c1e` (msgpack pre-grow 32KB)
  - `c338171` revert von `aab9bf5` (clientID alloc cut)
  - `6f23206` revert von `e98d991` (manual single OB msgpack)
  - `17f5540` revert von `585c84b` (binance OB read no-copy)
  - `06c3d71` revert von `9b69206` (stream parse index)
  - `7d8bc69` revert von `c21600d` (lazy per-encoding maps)
  - `7487789` revert von `a166311` (lazy P2 init)

## Offen

- aktuell keine offenen Punkte im Baseline-v2 Pfad
- spaeterer Doku-Task ausserhalb des Baseline-v2 Pfads:
  - Bitget-OB-Design-Notiz erstellen
  - nur technische Dokumentation (WS-Channel, Subscribe/Unsubscribe, Snapshot/Delta, Mapping auf `shared_types.OrderBookUpdate`, Recovery-Regeln)
  - explizit ohne Implementierung und ohne Testlauf
- neuer technischer Backlog ausserhalb des Baseline-v2 Pfads:
  - P7-1 Unsubscribe-State-Machine fuer native Adapter
    - Ziel: `unsubscribe` nicht nur senden, sondern pro Stream/Batch verfolgen (`pending`, `acked`, `retry`, `failed`, `removed`)
    - inkl. Retry-Policy, Timeout und forced shard close als letzte Eskalation
    - in Arbeit:
      - erster Schnitt fuer `binance_native` umgesetzt und auf Vultr verifiziert (`unsubscribe` fuehrt sauber auf `trades=0`/`ob=0`)
      - erster Schnitt fuer `bybit_native` umgesetzt:
        - Trade- und OB-Shards verfolgen Command-Responses ueber `req_id`
        - Unsubscribe-Ack-Timeout/Nack fuehrt zu begrenztem Retry und danach zu gezieltem Shard-Recycle
        - Replay-Server (`cmd/wsreplay`) liefert passende Bybit-Acks fuer diesen Pfad
      - neue minimale Broker-Plumbing fuer `StreamStatusEvent` liegt:
        - Broker kann `stream_reconnecting` / `stream_restored` / `stream_unsubscribe_failed` / `stream_force_closed` verteilen
        - neuer schlanker `clients/smoke_client.go` loggt diese JSON-Status-Events
      - Vultr-Verifikation fuer `bybit_native` erfolgt; Lifecycle-Pfad laeuft stabil
  - P7-5 CCXT-Sonderpfad
    - eigene `unsubscribe`-Funktion fuer CCXT
    - betroffene Worker/Batches lokal neu aufbauen statt native WS-Unsub-Logik zu spiegeln
    - in Arbeit:
      - ConnectionManager nutzt jetzt eigene lokale Unsubscribe-Helfer fuer Trade- und Orderbook-Shards
      - CCXT-Worker rufen jetzt explizit `UnWatchTrades`, `UnWatchOrderBook`, `UnWatchTradesForSymbols` und `UnWatchOrderBookForSymbols` auf
      - die verwendeten CCXT-Go-Pro-Interfaces enthalten die `*ForSymbols`-Varianten weiterhin
      - CCXT-Konfiguration ist jetzt als getypte Default-/Override-Registry aufgebaut
      - unbekannte Exchanges fallen konservativ auf einen langsamen Default zurueck (`BatchSize=1`, `SymbolsPerShard=1`, erhoehte Subscribe-/Shard-Pausen)
      - nur verifizierte Boersen behalten explizite Overrides; fuer 20+ weitere Arbitrage-Ziele ist keine Einzelkonfig mehr noetig
      - `UnWatch*` wird jetzt nur noch genutzt, wenn die Policy die Capability explizit freigibt
      - unbekannte oder nicht verifizierte Exchanges fallen beim Unsubscribe auf harten Shard-Recycle mit Neuaufbau nur der verbleibenden `desired`-Streams zurueck
      - `LoadMarkets()` fuer Batch-Symbolfilter laeuft jetzt ueber einen einmaligen, gecachten CCXT-Market-Load pro `exchange/marketType` statt shardweise; das vermeidet REST-Weight-Spikes und Binance-418-Bans
  - P7-7 Abnahme
    - unsubscribe wird verfolgt
    - Fehler sind sichtbar
    - andere Streams werden nach forced recycle sauber wiederhergestellt
    - Clients sehen `reconnecting`/`restored`
    - noch offen bis `P7-5` abgeschlossen ist

## Aktueller Arbeitsmodus

- Run-Workflow bleibt unveraendert nach Vorgabe von Stephan (`baseline_ingest.sh` Block)
- Pro Performance-Patch:
  1. Commit pinnen
  2. Run ausfuehren
  3. gegen Referenz bewerten (`keep`/`revert`)

## Baseline-v2 Freeze-Regel (verbindlich)

- Pro Exchange/Markttyp genau 3 Runs mit identischen Parametern.
- Zwischen zwei Runs ohne Broker-Neustart:
  - erst weiter, wenn Broker-Idle bestaetigt ist (`[STATS] Trades: 0 | OrderBooks: 0`) mindestens 3x in Folge.
- Gueltigkeit eines Runs:
  - `gate_status=PASS`
  - `drops_delta_total=0`
  - Smoke-Client zeigt `SUBSCRIBE_DONE` und `DISCONNECT_SENT`.
- Vergleichsregel:
  - `alloc_space`/`cpu` aus Run-Artefakten fuer Hotspot-Vergleich,
  - Heap-Stabilitaet nur ueber `inuse_space` nach Idle bewerten (nicht ueber `alloc_space`).
- Ausreisserregel:
  - genau 1 Ausreisser ist erlaubt, wenn 2/3 Runs konsistent sind und der Ausreisser klar erklaerbar ist (z. B. Live-Marktrauschen/kurzer Spike).
  - sonst Serie wiederholen.

## Baseline-v2 Freeze-Status

- Bybit live freeze: funktional verifiziert (disconnect/unsubscribe + idle-gated Serienlauf, `inuse_space` stabil).
- Binance live freeze: final verifiziert (`v2_live_freeze_binance_clean_1..3`, `PASS`, Drops `0`).
- Bitget live freeze: final verifiziert (`v2_live_freeze_bitget_clean_1..3`, `PASS`, Drops `0`, trade-only/`ob/s=0` by design).
