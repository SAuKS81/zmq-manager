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

### 1. Bitget Send-Limiter

Erster Fokus:

- `internal/exchanges/bitget/send_limiter.go`
- `bitgetSendLimiter.Wait()`

Ziel:

- kein Warten mehr unter `sync.Mutex`
- Lock nur fuer kurzen State-Zugriff verwenden
- Wartephase ausserhalb des Locks

Begruendung:

- Mutex-Profil zeigt dort den aktuell klarsten Hotspot
- geringes Risiko
- klar abgrenzbarer erster Fix

### 2. Bitget ConnectionManager

Danach:

- `internal/exchanges/bitget/connection_manager.go`
- besonders `addSubscription()`

Zu pruefen bzw. umzubauen:

- kein `Sleep` unter Lock
- Timer-/AfterFunc-Handling entkoppeln
- moeglichst kurze kritische Abschnitte

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

## Nächster konkreter Schritt

Als naechstes umzusetzen:

- Bitget Send-Limiter ohne Warten unter Lock
- danach lokaler Build + gezielte Tests
- danach erneut `pprof` auf Vultr
