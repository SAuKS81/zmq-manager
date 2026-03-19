## MEXC Protobuf Source

Dieser Ordner enthaelt den MEXC-Protobuf-Stand aus dem produktiven CCXT-Go-Modul.

Quelle:
- Modul: `github.com/ccxt/ccxt/go/v4`
- Version: `v4.5.44`
- Unterordner im Modul: `protoc/`

Wichtige Versionsbindung:
- `protoc`: `v5.29.3`
- `protoc-gen-go`: `v1.36.10`

Warum dieser Stand:
- Der native MEXC-Adapter soll denselben Wire-Schema-Stand nutzen wie der produktive CCXT-Pfad.
- Es soll bewusst nicht automatisch der neueste externe MEXC-Proto-Stand verwendet werden.

Hinweis:
- Fuer Spot-Trades und Spot-Orderbooks werden fachlich nur `PushDataV3ApiWrapper`, `PublicAggreDealsV3Api` und `PublicAggreDepthsV3Api` benoetigt.
- Da `PushDataV3ApiWrapper` jedoch alle MEXC-Body-Typen im `oneof` referenziert, wird hier der vollstaendige, compile-sichere Dateisatz aus dem CCXT-Stand abgelegt.
