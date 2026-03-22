# Performance Guide

Dieses Dokument ist der aktuelle Leitfaden fuer die Performance-Arbeit im `zmq_manager`.
Es soll die naechsten Schritte stabil halten, damit wir nicht zwischen Symptomen und
verschiedenen Baustellen hin- und herspringen.

## Aktueller Befund

Die bisherigen Messungen zeigen:

- Logging-/Write-Last wurde bereits deutlich reduziert
- der Hauptengpass sitzt weiterhin in Synchronisation und Scheduling
- `pprof` zeigt klar:
  - Mutex-Contention
  - viele blockierende `select`-/Channel-Loops
  - besonders auffaellig in den Exchange-Workern

## Aktuelle Priorisierung

### 1. Bybit ConnectionManager

Erster Fokus nach dem Bitget-Umbau:

- `internal/exchanges/bybit/connection_manager.go`
- `internal/exchanges/bybit/ob_connection_manager.go`
- besonders `addSubscription()`

Ziel:

- Hot-Path in `addSubscription()` so klein wie moeglich machen
- Logging aus dem Subscription-Pfad entfernen
- keine unnoetigen Channel-Sends / Nebenwirkungen im unmittelbaren Entscheidungsweg

Begruendung:

- aktuelles Mutex-Profil zeigt Bybit nun als dominanten kumulativen Treiber
- `log.Printf` haengt direkt am Bybit-`addSubscription()`-Pfad
- geringer Eingriffsbereich bei guter Messbarkeit

### 2. Bybit Session-/Worker-Pfade

Danach:

- `internal/exchanges/bybit/shard_worker.go`
- `internal/exchanges/bybit/ob_shard_worker.go`
- besonders `runSession()` / Eventloop-Pfade

Zu pruefen bzw. umzubauen:

- grosse `select`-Schleifen
- unnoetige Wakeups
- Einzelverarbeitung statt Batching
- mehrere Ticker auf gemeinsame Maintenance-Takte reduzieren

### 3. Broker-Hot-Path

Danach:

- `internal/broker/runtime_tracker.go`
- `internal/metrics/metrics.go`

Ziele:

- globale Locks auf dem Nachrichten-Hot-Path reduzieren
- Lock-Sharding, Sampling oder Owner-Goroutine-Modell pruefen

### 4. Weitere Eventloop-/Ticker-Themen

Spaeter:

- `internal/exchanges/ccxt/worker_lifecycle.go`
- weitere kurze Ticker-/Sleep-Loops

Das ist wichtig, aber nicht der erste Hebel.

## Arbeitsregeln

- erst die klarsten Hotspots aus `pprof` angehen
- keine grossen Refactorings ohne Messbeleg
- erst nach jedem Schritt neu messen
- wenn ein Fix keine erkennbare Wirkung zeigt:
  - nicht ausweiten
  - naechsten Hotspot angehen

## Naechster konkreter Schritt

Als naechstes umzusetzen:

- Bybit-`runSession()`-Pfade fuer Trades und Orderbooks vereinfachen
- zuerst Wakeups durch Ticker-/Maintenance-Logik reduzieren
- danach lokaler Build + gezielte Tests
- danach erneut `pprof` auf Vultr
