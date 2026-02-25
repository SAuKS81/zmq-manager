//go:build !ccxt
// +build !ccxt

package broker

func registerCCXT(sm *SubscriptionManager) {
	// CCXT is intentionally disabled in default local builds.
}
