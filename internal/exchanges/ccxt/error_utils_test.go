//go:build ccxt
// +build ccxt

package ccxt

import (
	"errors"
	"reflect"
	"testing"
)

func TestExtractMissingMarketSymbol(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want string
		ok   bool
	}{
		{
			name: "standard does not have market symbol",
			err:  errors.New("binance does not have market symbol BTC/USDT:USDT"),
			want: "BTC/USDT:USDT",
			ok:   true,
		},
		{
			name: "BadSymbol variant",
			err:  errors.New("BadSymbol: kucoin bad symbol `ETH/USDT:USDT`"),
			want: "ETH/USDT:USDT",
			ok:   true,
		},
		{
			name: "no symbol",
			err:  errors.New("temporary network error"),
			want: "",
			ok:   false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := extractMissingMarketSymbol(tc.err)
			if ok != tc.ok || got != tc.want {
				t.Fatalf("extractMissingMarketSymbol() = (%q,%v), want (%q,%v)", got, ok, tc.want, tc.ok)
			}
		})
	}
}

func TestRemoveSymbolFromBatch(t *testing.T) {
	in := []string{"BTC/USDT", "ETH/USDT", "BTC/USDT", "XRP/USDT"}
	got := removeSymbolFromBatch(in, "BTC/USDT")
	want := []string{"ETH/USDT", "XRP/USDT"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("removeSymbolFromBatch() = %v, want %v", got, want)
	}
}

