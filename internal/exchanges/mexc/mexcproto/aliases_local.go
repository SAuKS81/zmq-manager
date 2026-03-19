//go:build !ccxt

package mexcproto

import localprotoc "bybit-watcher/internal/exchanges/mexc/protoc"

type PushDataV3ApiWrapper = localprotoc.PushDataV3ApiWrapper
type PublicAggreDealsV3ApiItem = localprotoc.PublicAggreDealsV3ApiItem
type PublicAggreDepthV3ApiItem = localprotoc.PublicAggreDepthV3ApiItem
