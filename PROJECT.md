# PROJECT

## Vision

Unverhandelbares Kernziel:

- Low-latency ZMQ Market Data Broker in Go
- Trades im Hot Path priorisiert
- Orderbooks sekundär
- deterministisches Verhalten
- minimale GC- und Lock-Last
- keine silent drops
- keine unbounded queues
- Trade p99 < 2ms als Zielwert

## Aktuelle Architektur (Kurz)

Pipeline:

WS ingest -> decode/normalize -> SubscriptionManager -> ClientManager -> ZMQ ROUTER/DEALER -> clients

Module:

- `internal/exchanges/*`: native ingest (bybit/binance/bitget) + ccxt research ingest
- `internal/broker/subscription_manager.go`: subscribe state + fanout decision
- `internal/broker/client_manager.go`: single-writer send path + encode (msgpack/json)
- `internal/metrics/metrics.go`: ingest/publish/drop counters + processing histogram

IPC Endpoint:

- Linux default: `ipc:///tmp/feed_broker.ipc`
- Windows fallback: `tcp://127.0.0.1:5555`

## Daten-Prioritaet & Regeln

- Prio 1: Trades und Orderbook ToB/L1 gleich priorisiert (Happy-Path drop-frei)
- Prio 2: Orderbook Level >1 bis max 20 ist sekundär (darf deterministisch degradiert werden, immer gezählt)
- Trades: sofort senden, keine Aggregation
- Orderbooks: Default Level 1-5
- Orderbooks max depth: 20
- Drop nur explizit zaehlen, nie still

## Baseline-Vertrag (Team)

- Happy Path Gate: `ingest500` + smoke client + `drops_delta` fuer Prio 1 = `0`
- Prio 2 Degradation ist erlaubt, aber als separates Gate auszuweisen
- `RecordPublish` muss einer erfolgreichen Send-Weitergabe entsprechen (keine Fake-Publishes)

## Observer Gates (Blocker)

Blocker fuer Performance-Merge:

- `zmq_dropped_messages_total > 0` im Happy Path
- unbounded queue/buffer neu eingefuehrt
- pprof zeigt klaren neuen Hotspot ohne Gegenmassnahme
- nicht reproduzierbarer Baseline-Run

## Baseline/Profiling Policy

- Erst messen, dann optimieren
- Jeder Performance-Schritt braucht:
1. gepinnten Commit
2. skriptbasierten Run (`baseline_ingest.sh`)
3. Gate-Report (`PASS` oder `FAIL_DROPS_DETECTED`)
- Keine Heuristik-Automation im Messpfad
