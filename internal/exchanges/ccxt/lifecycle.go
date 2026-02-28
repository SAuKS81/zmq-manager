//go:build ccxt
// +build ccxt

package ccxt

import (
	"log"

	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
)

type closeableExchange interface {
	Close() []error
}

func createCCXTExchange(exchangeName, marketType string) ccxtpro.IExchange {
	return ccxtpro.CreateExchange(exchangeName, makeExchangeOptions(exchangeName, marketType))
}

func closeCCXTExchange(exchangeName, marketType string, exchange ccxtpro.IExchange) {
	if exchange == nil {
		return
	}
	if c, ok := exchange.(closeableExchange); ok {
		if errs := c.Close(); len(errs) > 0 {
			for _, err := range errs {
				if err != nil {
					log.Printf("[CCXT-LIFECYCLE-WARN] Close exchange failed (%s/%s): %v", exchangeName, marketType, err)
				}
			}
		}
	}
}
