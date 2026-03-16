//go:build ccxt
// +build ccxt

package ccxt

import (
	"fmt"
	"reflect"
	"sync"

	ccxtcore "github.com/ccxt/ccxt/go/v4"
	ccxtpro "github.com/ccxt/ccxt/go/v4/pro"
)

type tradeCacheSnapshot struct {
	Found      bool
	MaxSize    int
	CurrentLen int
	CacheType  string
}

func inspectTradeCache(exchange ccxtpro.IExchange, symbol string) (tradeCacheSnapshot, error) {
	if exchange == nil {
		return tradeCacheSnapshot{}, fmt.Errorf("exchange is nil")
	}

	tradesValue, ok := findExchangeTradesField(reflect.ValueOf(exchange))
	if !ok || !tradesValue.IsValid() || tradesValue.IsNil() {
		return tradeCacheSnapshot{}, fmt.Errorf("trades field unavailable")
	}

	cacheValue, ok := lookupTradeCacheValue(tradesValue, symbol)
	if !ok || !cacheValue.IsValid() {
		return tradeCacheSnapshot{}, fmt.Errorf("trade cache missing for symbol %s", symbol)
	}

	if cacheValue.Kind() == reflect.Interface && !cacheValue.IsNil() {
		cacheValue = cacheValue.Elem()
	}

	if cacheValue.Kind() == reflect.Ptr && !cacheValue.IsNil() {
		if cache, ok := cacheValue.Interface().(*ccxtcore.ArrayCache); ok {
			return tradeCacheSnapshot{
				Found:      true,
				MaxSize:    cache.MaxSize,
				CurrentLen: len(cache.ToArray()),
				CacheType:  reflect.TypeOf(cache).String(),
			}, nil
		}
	}

	baseValue := cacheValue
	if baseValue.Kind() == reflect.Ptr && !baseValue.IsNil() {
		baseValue = baseValue.Elem()
	}

	if !baseValue.IsValid() {
		return tradeCacheSnapshot{}, fmt.Errorf("invalid cache value for symbol %s", symbol)
	}

	baseCacheField := baseValue.FieldByName("BaseCache")
	if baseCacheField.IsValid() {
		if baseCacheField.Kind() == reflect.Ptr && !baseCacheField.IsNil() {
			baseCacheField = baseCacheField.Elem()
		}
		if baseCacheField.IsValid() {
			maxSizeField := baseCacheField.FieldByName("MaxSize")
			dataField := baseCacheField.FieldByName("Data")
			if maxSizeField.IsValid() && dataField.IsValid() && dataField.Kind() == reflect.Slice {
				return tradeCacheSnapshot{
					Found:      true,
					MaxSize:    int(maxSizeField.Int()),
					CurrentLen: dataField.Len(),
					CacheType:  cacheValue.Type().String(),
				}, nil
			}
		}
	}

	return tradeCacheSnapshot{}, fmt.Errorf("unsupported cache type %s", cacheValue.Type().String())
}

func findExchangeTradesField(value reflect.Value) (reflect.Value, bool) {
	if !value.IsValid() {
		return reflect.Value{}, false
	}
	if value.Kind() == reflect.Interface && !value.IsNil() {
		value = value.Elem()
	}
	if value.Kind() == reflect.Ptr {
		if value.IsNil() {
			return reflect.Value{}, false
		}
		value = value.Elem()
	}
	if !value.IsValid() || value.Kind() != reflect.Struct {
		return reflect.Value{}, false
	}

	if field := value.FieldByName("Trades"); field.IsValid() {
		return field, true
	}
	if embedded := value.FieldByName("Exchange"); embedded.IsValid() {
		return findExchangeTradesField(embedded)
	}
	return reflect.Value{}, false
}

func lookupTradeCacheValue(tradesValue reflect.Value, symbol string) (reflect.Value, bool) {
	if tradesValue.Kind() == reflect.Interface && !tradesValue.IsNil() {
		tradesValue = tradesValue.Elem()
	}
	if !tradesValue.IsValid() {
		return reflect.Value{}, false
	}

	if tradesValue.Kind() == reflect.Ptr && !tradesValue.IsNil() {
		if syncMap, ok := tradesValue.Interface().(*sync.Map); ok {
			cache, found := syncMap.Load(symbol)
			if !found {
				return reflect.Value{}, false
			}
			return reflect.ValueOf(cache), true
		}
	}

	if tradesValue.Kind() == reflect.Map {
		result := tradesValue.MapIndex(reflect.ValueOf(symbol))
		if result.IsValid() {
			return result, true
		}
		return reflect.Value{}, false
	}

	switch trades := tradesValue.Interface().(type) {
	case map[string]interface{}:
		cache, ok := trades[symbol]
		if !ok {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(cache), true
	case map[string]*ccxtcore.ArrayCache:
		cache, ok := trades[symbol]
		if !ok {
			return reflect.Value{}, false
		}
		return reflect.ValueOf(cache), true
	default:
		return reflect.Value{}, false
	}
}
