# USER GUIDE

## Zweck

Dieses Dokument beschreibt den praktischen Betrieb des `zmq_manager` so, dass ein neuer Nutzer ohne Code-Lesen versteht:

- was der Broker tut
- wie Clients sich verbinden
- welche Requests unterstuetzt werden
- welche Datenformate ueber ZMQ geliefert werden
- wie native Adapter und CCXT sich unterscheiden
- wie Profiling, Baselines und Lifecycle-Haertung funktionieren

Der Guide ist bewusst betriebsnah geschrieben. Architektur- und Team-Details stehen weiterhin in:

- [PROJECT.md](./PROJECT.md)
- [RUNBOOK.md](./RUNBOOK.md)
- [STATUS.md](./STATUS.md)
- [scripts/README_baseline.md](./scripts/README_baseline.md)

## 1. Was der `zmq_manager` ist

Der Broker sammelt Marktdaten von mehreren Boersen ein, normalisiert sie und verteilt sie ueber einen ZMQ `ROUTER` Socket an Clients.

Pipeline:

`Exchange WS/CCXT -> normalize -> SubscriptionManager -> ClientManager -> ZMQ ROUTER -> Client`

Unterstuetzte Datenarten:

- `trades`
- `orderbooks`

Unterstuetzte Adaptertypen:

- native Adapter:
  - `binance_native`
  - `bybit_native`
  - `bitget_native`
- CCXT-Pro Adapter:
  - z. B. `binance`, `bybit`, `bitget`

Wichtige Prioritaetsregel:

- Trades und Top-of-Book/L1 laufen im priorisierten Pfad
- tiefere Orderbook-Updates laufen im degradierten Latest-Only-Pfad
- Drops duerfen nie still passieren; sie werden explizit gezaehlt

Wichtige Versandregel:

- Trades werden nicht mehr unbounded greedy gebatcht
- der Broker verwendet fuer Trades ein kleines Micro-Batch-Fenster:
  - maximal `32` Trades
  - oder maximal `10ms`
  - was zuerst eintritt
- Orderbooks bleiben davon unberuehrt

## 2. Wie der Broker gestartet wird

Standardstart:

```bash
go run ./cmd/broker/main.go
```

Start mit pprof fuer Baselines und Diagnose:

```bash
go run ./cmd/broker/main.go --pprof-block-rate 1 --pprof-mutex-fraction 5
```

Start mit CCXT-Unterstuetzung:

```bash
go run -tags ccxt ./cmd/broker/main.go --pprof-block-rate 1 --pprof-mutex-fraction 5
```

Der Broker startet zusaetzlich:

- Prometheus Metrics: `http://127.0.0.1:6060/metrics`
- pprof: `http://127.0.0.1:6060/debug/pprof/`

## 3. Wo der Broker lauscht

Der `ClientManager` bindet abhaengig vom Betriebssystem unterschiedlich:

- Linux:
  - `ipc:///tmp/feed_broker.ipc`
- Nicht-Linux:
  - `tcp://127.0.0.1:5555`

Das ist in [internal/broker/client_manager.go](./internal/broker/client_manager.go) fest verdrahtet.

Praktische Folge:

- auf Linux sollte ein lokaler Client standardmaessig `ipc:///tmp/feed_broker.ipc` verwenden
- auf Windows/Mac `tcp://127.0.0.1:5555`

## 4. Wie ein Client mit dem Broker spricht

Der Broker verwendet ZMQ `ROUTER`; Clients sprechen typischerweise als `DEALER`.

Der Client sendet JSON-Requests an den Broker.

Unterstuetzte Request-Aktionen:

- `subscribe`
- `unsubscribe`
- `subscribe_bulk`
- `unsubscribe_bulk`
- `subscribe_all`
- `disconnect`
- `list_subscriptions`
- `subscription_health_snapshot`
- `get_runtime_snapshot`
- `get_capabilities`

Optional kann ein Client pro Operator-Request eine `request_id` mitsenden.
Der Broker fuehrt diese `request_id` in:

- Control-Responses
- Ack-/Apply-Events
- Batch-Summary-Events

Optional kann ein Client sein Ziel-Encoding setzen:

- `json`
- `msgpack`
- `binary`

Hinweis:

- `binary` wird intern wie `msgpack` behandelt
- Standard-Encoding fuer neue Clients ist `json`

## 5. Request-Formate

### 5.0 Request-Korrelation

Fuer Deploy- und Queue-Workflows sollte das UI konsequent eine `request_id` setzen:

```json
{
  "action": "subscribe_bulk",
  "request_id": "deploy-123",
  "exchange": "bybit_native",
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "market_type": "spot",
  "data_type": "trades"
}
```

Der Broker verwendet diese `request_id` fuer:

- Control-Responses
- `stream_*_acked`
- `stream_*_failed`
- `deploy_batch_summary`

### 5.1 Einzel-Subscribe

```json
{
  "action": "subscribe",
  "exchange": "binance_native",
  "symbol": "BTCUSDT",
  "market_type": "spot",
  "data_type": "trades",
  "encoding": "msgpack"
}
```

Mit Orderbook-Tiefe:

```json
{
  "action": "subscribe",
  "exchange": "bybit_native",
  "symbol": "BTCUSDT",
  "market_type": "swap",
  "data_type": "orderbooks",
  "depth": 5,
  "encoding": "msgpack"
}
```

### 5.2 Einzel-Unsubscribe

```json
{
  "action": "unsubscribe",
  "exchange": "binance_native",
  "symbol": "BTCUSDT",
  "market_type": "spot",
  "data_type": "trades"
}
```

### 5.3 Bulk-Subscribe

`subscribe_bulk` ist nur eine Client-Bequemlichkeit. Intern zerlegt der Broker diesen Request in viele einzelne `subscribe`-Requests.

```json
{
  "action": "subscribe_bulk",
  "exchange": "binance",
  "symbols": ["BTC/USDT", "ETH/USDT", "SOL/USDT"],
  "market_type": "spot",
  "data_type": "trades",
  "encoding": "msgpack"
}
```

### 5.4 Subscribe-All

Aktuell nur fuer Trades sinnvoll.

```json
{
  "action": "subscribe_all",
  "exchange": "binance_native",
  "market_type": "spot",
  "data_type": "trades"
}
```

### 5.4a Bulk-Unsubscribe

```json
{
  "action": "unsubscribe_bulk",
  "request_id": "deploy-124",
  "exchange": "bybit_native",
  "symbols": ["BTCUSDT", "ETHUSDT"],
  "market_type": "spot",
  "data_type": "trades"
}
```

### 5.5 Disconnect

```json
{
  "action": "disconnect"
}
```

Wichtig:

- `disconnect` ist kein bloesser Client-Abbruch
- der Broker bereinigt dabei alle zugeordneten Subscriptions und routed das Unsubscribe an den **exakten** Adapterpfad zurueck
- seit `P7-5` bleibt dabei der exakte Routen-Typ erhalten:
  - `binance` bleibt `binance`
  - `binance_native` bleibt `binance_native`

### 5.6 Liste aktiver physischer Subscriptions

```json
{
  "action": "list_subscriptions"
}
```

Optional ist bereits ein `scope`-Feld vorgesehen, die erste produktive Version antwortet aber global:

```json
{
  "action": "list_subscriptions",
  "scope": "global"
}
```

Antwort:

```json
{
  "type": "subscriptions_snapshot",
  "scope": "global",
  "ts": 1772300000000,
  "items": [
    {
      "exchange": "bybit",
      "market_type": "spot",
      "symbol": "BTC/USDT",
      "data_type": "trades",
      "adapter": "ccxt",
      "encoding": "msgpack",
      "running": true,
      "owners": 3,
      "clients": 3
    }
  ]
}
```

Semantik:

- ein Eintrag pro aktiver physischer Subscription auf Broker-Ebene
- `adapter` ist explizit `ccxt` oder `native`
- `encoding` zeigt die effektiv aggregierte Client-Encoding-Sicht:
  - leer = nicht gesetzt
  - ein Wert = konsistent
  - `mixed` = mehrere Clients mit unterschiedlichem Encoding
- `depth` wird nur bei Orderbooks gesetzt
- `owners` und `clients` sind in der aktuellen Version identisch und entsprechen der Anzahl abonnierter Clients auf diesen Join-Key

### 5.7 Health-Snapshot pro Subscription

```json
{
  "action": "subscription_health_snapshot"
}
```

Antwort:

```json
{
  "type": "subscription_health_snapshot",
  "ts": 1772300000000,
  "items": [
    {
      "exchange": "bybit",
      "market_type": "spot",
      "symbol": "BTC/USDT",
      "data_type": "trades",
      "status": "running",
      "last_message_age_ms": 120,
      "last_message_ts": 1772299999880,
      "reconnects_1h": 0,
      "messages_per_sec": 45.2,
      "latency_ms": 12.0,
      "broker_latency_ms": 1.0,
      "last_error": "",
      "last_error_ts": 0,
      "last_reconnect_ts": 1772299999000,
      "stale_threshold_ms": 5000,
      "sample_window_sec": 60
    }
  ]
}
```

Normierte Statuswerte:

- `running`
- `degraded`
- `reconnecting`
- `failed`
- `stopped`

Latenz-Semantik:

- `latency_ms`:
  - echte Exchange-Latenz
  - Zeit vom Exchange-Timestamp des Trades/Orderbooks bis zum Eintreffen im Broker
- `broker_latency_ms`:
  - interne Broker-Latenz
  - Zeit vom Broker-Ingest bis zum Snapshot-/Dispatch-Pfad
- `last_message_ts`:
  - Broker-Eingangszeit der letzten Nachricht
- `last_message_age_ms`:
  - Alter bezogen auf diese Broker-Eingangszeit
- `sample_window_sec`:
  - Broker-seitiges Messfenster fuer `messages_per_sec`
- `stale_threshold_ms`:
  - Broker-seitiger Schwellwert, ab dem `running -> degraded` kippt

### 5.8 Kombinierter Runtime-Snapshot

Das ist der empfohlene UI-Endpunkt fuer eine Command Bridge:

```json
{
  "action": "get_runtime_snapshot"
}
```

Antwort:

```json
{
  "type": "runtime_snapshot",
  "request_id": "deploy-123",
  "ts": 1772300000000,
  "subscriptions": [],
  "health": [],
  "totals": {
    "active_subscriptions": 1045,
    "messages_per_sec": 14200,
    "reconnects_24h": 42
  }
}
```

Warum dieser Endpunkt bevorzugt werden sollte:

- beide Snapshots kommen aus demselben Broker-Zeitpunkt
- das UI muss keine zwei getrennten Requests koordinieren
- `subscriptions` und `health` sind ueber denselben Join-Key kombinierbar:
  - `exchange`
  - `market_type`
  - `symbol`
  - `data_type`

### 5.9 Capabilities-Snapshot

```json
{
  "action": "get_capabilities",
  "request_id": "caps-1"
}
```

Antwort:

```json
{
  "type": "capabilities_snapshot",
  "request_id": "caps-1",
  "ts": 1772300000000,
  "items": [
    {
      "exchange": "bybit_native",
      "adapter": "native",
      "market_types": ["spot", "swap"],
      "data_types": ["trades", "orderbooks"],
      "orderbook_depths": [1, 50, 200, 500],
      "uses_batch_symbols": false,
      "supports_trade_unwatch": false,
      "supports_trade_batch_unwatch": false,
      "supports_orderbook_unwatch": false,
      "supports_orderbook_batch_unwatch": false,
      "supports_cache_n": false,
      "supports_request_id": true,
      "supports_deploy_queue": true
    }
  ]
}
```

Der Endpunkt ist fuer das UI die Quelle fuer:

- unterstuetzte `market_type`-/`data_type`-Kombinationen
- erlaubte Orderbook-Tiefen
- Deploy-/Correlation-Faehigkeiten pro Route
- konkrete CCXT-/Adapter-Capabilities fuer Lifecycle-Steuerung:
  - `uses_batch_symbols`
  - `supports_trade_unwatch`
  - `supports_trade_batch_unwatch`
  - `supports_orderbook_unwatch`
  - `supports_orderbook_batch_unwatch`

### 5.10 Deploy-Batch-Summary

Bulk-Operationen (`subscribe_bulk`, `unsubscribe_bulk`) erzeugen nach Abschluss ein Summary-Event:

```json
{
  "type": "deploy_batch_summary",
  "request_id": "deploy-123",
  "ts": 1772300001000,
  "sent": 17,
  "acked": 15,
  "failed": 2
}
```

## 6. Symbolformat

Es gibt zwei wichtige Faelle:

### Native Adapter

Typischerweise boersenspezifische native Symbole:

- Binance native: `BTCUSDT`
- Bybit native: `BTCUSDT`
- Bitget native: `BTCUSDT`

### CCXT

CCXT-style Symbole:

- Spot: `BTC/USDT`
- Perp/Swap: `BTC/USDT:USDT`

Wichtig:

- bei CCXT sollte `--exchanges` oder `exchange` **ohne** `_native` verwendet werden
- bei nativen Adaptern immer mit `_native`
- Snapshot, Health und Lifecycle-Events verwenden dieselbe Symbolform wie der jeweilige Adapterpfad:
  - native Requests/Events bleiben in nativer Symbolform
  - CCXT Requests/Events bleiben in CCXT-Symbolform

## 7. Welche Antworten der Broker liefert

Der Broker sendet drei Hauptarten von Nutzdaten:

- Trade-Micro-Batches
- Orderbook-Batches
- Lifecycle-/Status-Events

Zusatz:

- Ping/Pong fuer Client-Liveness
- Fehler-JSON bei ungueltigen Requests
- Runtime-Snapshots als JSON-Control-Responses

## 8. Datenformate des Brokers

### 8.1 JSON

Bei `encoding=json` erhaelt der Client JSON-Payloads.

Trades kommen im aktuellen Stand als kleine Arrays aus dem Trade-Micro-Batch-Fenster.
Orderbooks kommen ebenfalls als Arrays von normalisierten Objekten.

### 8.2 Msgpack/Binary

Bei `encoding=msgpack` oder `encoding=binary` verwendet der Broker zwei Frames:

- Header-Frame
- Payload-Frame

Header:

- `T` = Trade-Batch
- `O` = Orderbook-Batch

Das wird in [internal/broker/client_manager.go](./internal/broker/client_manager.go) und [clients/smoke_client.go](./clients/smoke_client.go) sichtbar.

### 8.3 Fehlerantworten

Fehler kommen als JSON:

```json
{
  "type": "error",
  "request_id": "deploy-123",
  "code": "invalid_request",
  "message": "subscribe/unsubscribe requires exchange, symbol and market_type"
}
```

Typische Fehlercodes:

- `invalid_json`
- `invalid_encoding`
- `missing_action`
- `invalid_request`
- `unknown_action`

### 8.4 Runtime-Snapshot-Antworten

Die Runtime-Snapshot-Endpunkte werden immer als JSON-Control-Payload gesendet.

Wichtig:

- sie sind bewusst lesbare Read-API-Antworten
- sie umgehen kein bestehendes Subscribe-/Unsubscribe-Verhalten
- sie dienen nur dazu, dem UI einen belastbaren Istzustand zu geben
- wenn der Request eine `request_id` gesetzt hat, spiegelt der Broker diese in der Control-Response

## 9. Normalisierte Datenmodelle

### TradeUpdate

Felder:

- `exchange`
- `symbol`
- `market_type`
- `timestamp`
- `go_timestamp`
- `price`
- `amount`
- `side`
- `trade_id`
- `data_type`

Definition: [internal/shared_types/types.go](./internal/shared_types/types.go)

### OrderBookUpdate

Felder:

- `exchange`
- `symbol`
- `market_type`
- `timestamp`
- `go_timestamp`
- `bids`
- `asks`
- `data_type`

`bids` und `asks` bestehen aus `price` und `amount`.

## 10. Lifecycle- und Status-Events

Seit `P7` verteilt der Broker explizite Stream-Status-Events an betroffene Clients.

Typen:

- `stream_subscribe_acked`
- `stream_subscribe_active`
- `stream_subscribe_failed`
- `stream_unsubscribe_acked`
- `stream_update_acked`
- `stream_update_failed`
- `stream_stop_requested`
- `stream_reconnecting`
- `stream_restored`
- `stream_unsubscribe_failed`
- `stream_force_closed`

Format:

```json
{
  "type": "stream_subscribe_acked",
  "exchange": "bybit_native",
  "market_type": "spot",
  "data_type": "trades",
  "symbol": "BTCUSDT",
  "adapter": "native",
  "request_id": "deploy-123",
  "status": "acked",
  "ts": 1772300000000
}
```

Wenn ein Event mehrere Symbole betrifft, kann statt `symbol` ein `symbols`-Array gesetzt sein.

Der Smoke-Client loggt diese Events in lesbarer Form.

Operatorisch wichtige Semantik:

- `stream_subscribe_acked`:
  - der Broker hat den Request angenommen und in den Adapterpfad ueberfuehrt
- `stream_subscribe_active`:
  - die Subscription hat echte Laufzeitdaten produziert
- `stream_update_acked`:
  - eine Spec-Aenderung, z. B. Orderbook-Depth, wurde brokerseitig akzeptiert
- `stream_*_failed`:
  - der Request wurde abgelehnt oder spaeter technisch als fehlgeschlagen markiert
- `stream_stop_requested`:
  - Operator/UI hat eine aktive Subscription explizit zum Stop markiert

Zusatz fuer Dashboards:

- `runtime_totals_tick`

Beispiel:

```json
{
  "type": "runtime_totals_tick",
  "ts": 1772300000000,
  "active_subscriptions": 1045,
  "messages_per_sec": 14200,
  "reconnects_24h": 42
}
```

Der Tick wird periodisch brokerseitig broadcastet und eignet sich fuer UI-KPIs ohne permanenten Poll auf `get_runtime_snapshot`.

## 11. Ping/Pong und Client-Timeout

Der Broker erwartet, dass Clients auf Ping reagieren.

Verhalten:

- Broker sendet Ping periodisch
- Client antwortet:

```json
{
  "message": "pong"
}
```

Wenn ein Client zu lange keinen Pong liefert:

- wird er im `ClientManager` entfernt
- der Broker triggert intern einen `disconnect`
- Subscriptions werden aufgeraeumt

## 12. Native vs. CCXT

### Native Adapter

Vorteile:

- beste Kontrolle ueber Subscribe-/Unsubscribe-Protokolle
- exchange-spezifische Lifecycle-Haertung
- Replay- und Baseline-Pfad ist fuer native Adapter die Primarreferenz

Aktuell relevant:

- `binance_native`
- `bybit_native`
- `bitget_native`

### CCXT

CCXT ist der generische Forschungs-/Abdeckungspfad fuer viele Exchanges.

Wichtige Regeln:

- fuer unbekannte Exchanges greift ein konservativer Slow-Default
- wenn `Describe().has["unWatch*"] == true`, versucht der Broker den echten `UnWatch*`-Pfad zu nutzen
- wenn `UnWatch*` zur Laufzeit fehlschlaegt, wird der Fehler geloggt und es folgt automatisch der shard-scope Fallback mit Wiederaufbau nur der `desired`-Streams
- Market-Metadaten werden pro `exchange/marketType` gecacht, um REST-Weight-Spikes zu vermeiden

## 13. Exchange-spezifische Besonderheiten

### Binance native

- native Lifecycle-Haertung verifiziert
- selektiver Recycle greift nur auf `desired`-Streams zurueck

### Bybit native

- marktspezifisches Command-Chunking
- Spot-Subscribe nach Doku limitiert

### Bitget native

- besonders empfindlich bei Subscribe-/Unsubscribe-Taktung
- deshalb globale Sendetaktung
- Empty-Shard-Cleanup erst nach finalem Unsubscribe-Flush
- aktuell trade-only:
  - `ob/s = 0` by design

### CCXT generic

- nicht jede Exchange hat dieselben `UnWatch*`-Faehigkeiten
- deshalb gilt jetzt: `Describe().has["unWatch*"] == true` aktiviert den Primaerpfad, Laufzeitfehler fallen auf Recycle zurueck
- `mexc` ist aktuell ein expliziter Fall fuer `recycle-on-unsubscribe`, weil `UnWatchTrades` in der verwendeten CCXT-Pro-Version reproduzierbar fehlschlaegt
- `kucoin` Spot-Trades verwenden echten Batch-Unwatch; Orderbook-Unwatch bleibt bis zu einem eigenen Verifikationslauf konservativ
- `htx`/`huobi` Trades und `woo` Trades laufen auf echtem `UnWatchTrades`; Laufzeitfehler werden abgefangen und fallen auf Fallback zurueck

## 14. Baseline- und Replay-Betrieb

Fuer reproduzierbare Laeufe gibt es:

- [scripts/README_baseline.md](./scripts/README_baseline.md)
- `scripts/baseline_ingest.sh`
- `cmd/wsreplay`

Artefakte je Run:

- `cpu.pprof`
- `heap.pprof`
- `allocs.pprof`
- `metrics_pre.txt`
- `metrics_post.txt`
- `smoke.log`
- `meta.json`
- `bundle.tar.gz`

Gate:

- `PASS`
- `FAIL_DROPS_DETECTED`

Entscheidend:

- `gate_status=PASS`
- `drops_delta_total=0`
- `SUBSCRIBE_DONE`
- `DISCONNECT_SENT`

## 15. Metrics

Prometheus Endpoint:

- `http://127.0.0.1:6060/metrics`

Wichtige Lifecycle-Metriken:

- `zmq_unsubscribe_attempts_total`
- `zmq_unsubscribe_failures_total`
- `zmq_forced_shard_recycles_total`
- `zmq_stream_reconnects_total`
- `zmq_stream_restore_success_total`

Wichtige Betriebsmetriken:

- Drops
- Queue-Samples
- Processing-Histogramme

## 15.1 Health-Snapshot-Herkunft

Der Runtime-Health-Snapshot wird broker-seitig aus drei Quellen aufgebaut:

- aktuelle deduplizierte aktive Subscription-Maps
- beobachtete eingehende Trade-/Orderbook-Nachrichten
- Lifecycle-Events wie `stream_reconnecting` und `stream_restored`

Das bedeutet:

- die Read-API ist ein echter Laufzeit-Istzustand auf Broker-Ebene
- es wird keine neue Dedupe-Logik eingefuehrt
- die bestehende Subscribe-/Unsubscribe-Logik bleibt unveraendert
- Health und `STATS` verwenden fuer die Nachrichtenerkennung dieselbe Broker-Eingangsseite
- die Exchange-Latenz wird getrennt von der Broker-internen Latenz ausgewiesen

## 16. pprof und Diagnose

Der Broker startet pprof auf `localhost:6060`.

Typische Abrufe:

```bash
curl -sS http://127.0.0.1:6060/debug/pprof/profile?seconds=30 > cpu.pprof
curl -sS http://127.0.0.1:6060/debug/pprof/heap > heap.pprof
curl -sS http://127.0.0.1:6060/debug/pprof/allocs > allocs.pprof
curl -sS http://127.0.0.1:6060/debug/pprof/block > block.pprof
curl -sS http://127.0.0.1:6060/debug/pprof/mutex > mutex.pprof
```

Wichtige Regel:

- `alloc_space` ist fuer lange Prozesslaeufe kumulativ
- fuer Leak-/Bestandsbewertung ist `inuse_space` nach Idle der richtige Blick

## 17. Typischer Betriebsablauf

1. Broker starten
2. Client verbindet sich
3. Client setzt optional Encoding
4. Client subscribed Trades/Orderbooks
5. Broker routed Requests an passenden Adapter
6. Adapter liefert normalisierte Daten
7. Broker verteilt Daten an abonnierte Clients
8. Client sendet `disconnect`
9. Broker raeumt Subscriptions auf und routed exaktes Unsubscribe

Hinweis zur Trade-Latenz:

- Trades werden mit einem begrenzten Micro-Batch-Fenster gesammelt
- dadurch liegt die zusaetzliche Broker-Verzoegerung typischerweise bei hoechstens `10ms`
- bei geringer Last koennen auch Einzeltrades als 1er-Array gesendet werden

## 18. Troubleshooting

### Es kommen keine Daten

Pruefen:

- Broker laeuft?
- richtiger Endpoint (`ipc://` vs `tcp://`)?
- korrektes Symbolformat fuer nativen oder CCXT-Pfad?
- `exchange` richtig gesetzt (`binance` vs `binance_native`)?

### Client bekommt Fehler `invalid_request`

Pflichtfelder je Aktion pruefen:

- `subscribe/unsubscribe`:
  - `exchange`
  - `symbol`
  - `market_type`
- `subscribe_bulk`:
  - `exchange`
  - `symbols`
  - `market_type`

### CCXT laeuft in REST-Limits

Sollte nach Market-Cache-Haertung deutlich reduziert sein.

Pruefen:

- Broker wirklich mit aktuellem Commit gestartet?
- alter Prozess noch aktiv?
- unbekannte Exchange faellt auf Slow-Default zurueck?

### Nach Disconnect laufen Streams weiter

Pruefen:

- nativer vs. CCXT-Pfad korrekt?
- exakte Route wurde beibehalten?
- `DISCONNECT_SENT` im Smoke-Log?
- `STATS` faellt auf `Trades: 0 | OrderBooks: 0`?

### Bitget reagiert empfindlich

Bitget ist bei Command-Pacing strikter als andere Boersen.

Wichtig:

- globale Sendetaktung nicht umgehen
- keine parallelen Retry-Stuerme einfuehren
- Empty-Shards erst nach finalem Unsubscribe retire'n

## 19. Wichtige Dateien

- Broker Entry:
  - [cmd/broker/main.go](./cmd/broker/main.go)
- Kern-Broker:
  - [internal/broker/subscription_manager.go](./internal/broker/subscription_manager.go)
  - [internal/broker/client_manager.go](./internal/broker/client_manager.go)
- Shared Types:
  - [internal/shared_types/types.go](./internal/shared_types/types.go)
- Smoke-Client:
  - [clients/smoke_client.go](./clients/smoke_client.go)
- Einfacher Python-Rate-Client fuer native Trades:
  - [clients/native_rate_client.py](./clients/native_rate_client.py)
- Baseline:
  - [scripts/baseline_ingest.sh](./scripts/baseline_ingest.sh)
  - [scripts/README_baseline.md](./scripts/README_baseline.md)
- Replay:
  - [cmd/wsreplay/main.go](./cmd/wsreplay/main.go)

## 20. Aktueller technischer Status

Stand heute:

- Baseline-v2 Pfad ist abgeschlossen
- P7 Lifecycle-Hardening ist abgeschlossen
- Beta-Deployment soll weiterhin Reconnect-/Status-Verhalten beobachten
- Bitget-Orderbook bleibt bewusst ein spaeterer Doku-/Implementierungspfad

## 21. Einfacher Python-Rate-Client

Fuer einen schnellen manuellen Test der nativen Trade-Pfade gibt es einen kleinen Python-Client:

- [clients/native_rate_client.py](./clients/native_rate_client.py)

Voraussetzung:

```bash
pip install pyzmq
```

Standardaufruf fuer native Binance- und Bybit-Trades auf `BTCUSDT` und `ETHUSDT`:

```bash
python3 clients/native_rate_client.py
```

Der Client:

- verbindet sich unter Linux standardmaessig mit `ipc:///tmp/feed_broker.ipc`
- sendet `subscribe_bulk` fuer:
  - `binance_native`
  - `bybit_native`
- abonniert `trades`
- gibt fortlaufend `msg/s` und `trades/s` aus
- sendet bei `Ctrl+C` ein `disconnect`

Beispiele:

```bash
python3 clients/native_rate_client.py --market-type swap
python3 clients/native_rate_client.py --symbols BTCUSDT,ETHUSDT,SOLUSDT
python3 clients/native_rate_client.py --broker tcp://127.0.0.1:5555
```
