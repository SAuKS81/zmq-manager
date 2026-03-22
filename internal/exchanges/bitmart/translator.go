package bitmart

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"fmt"
	"io"
	"strconv"
	"strings"

	"bybit-watcher/internal/shared_types"
)

func TranslateSymbolToExchange(symbol string) string {
	return strings.ReplaceAll(symbol, "/", "_")
}

func TranslateSymbolFromExchange(symbol string) string {
	return strings.ReplaceAll(symbol, "_", "/")
}

func NormalizeOrderBookDepth(depth int) int {
	switch depth {
	case 100:
		return depth
	default:
		return defaultDepthLevel
	}
}

func NormalizeOrderBookMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case modeAll:
		return modeAll
	default:
		return modeLevel100
	}
}

func IsValidOrderBookMode(mode string) bool {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "", modeLevel100, modeAll:
		return true
	default:
		return false
	}
}

func orderBookChannel(_ int, _ string) string {
	return "depth/increase100"
}

func decodePayload(payload []byte) ([]byte, error) {
	if len(payload) == 0 {
		return payload, nil
	}
	if decoded, err := tryGzip(payload); err == nil {
		return decoded, nil
	}
	if decoded, err := tryZlib(payload); err == nil {
		return decoded, nil
	}
	return payload, nil
}

func tryGzip(payload []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func tryZlib(payload []byte) ([]byte, error) {
	reader, err := zlib.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

func NormalizeTrade(trade wsSpotTrade, goTimestamp int64, ingestUnixNano int64) (*shared_types.TradeUpdate, error) {
	price, err := strconv.ParseFloat(trade.Price, 64)
	if err != nil {
		return nil, fmt.Errorf("parse price: %w", err)
	}
	amount, err := strconv.ParseFloat(trade.Size, 64)
	if err != nil {
		return nil, fmt.Errorf("parse size: %w", err)
	}

	ts := trade.MST
	if ts == 0 && trade.ST > 0 {
		ts = trade.ST * 1000
	}

	return &shared_types.TradeUpdate{
		Exchange:       "bitmart",
		Symbol:         TranslateSymbolFromExchange(trade.Symbol),
		MarketType:     "spot",
		Timestamp:      ts,
		GoTimestamp:    goTimestamp,
		IngestUnixNano: ingestUnixNano,
		Price:          price,
		Amount:         amount,
		Side:           strings.ToLower(trade.Side),
	}, nil
}

func normalizeLevels(raw [][]string, existing map[string]shared_types.OrderBookLevel) error {
	for _, level := range raw {
		if len(level) < 2 {
			continue
		}
		priceStr := level[0]
		amountStr := level[1]
		amount, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			return err
		}
		if amount == 0 {
			delete(existing, priceStr)
			continue
		}
		price, err := strconv.ParseFloat(priceStr, 64)
		if err != nil {
			return err
		}
		existing[priceStr] = shared_types.OrderBookLevel{Price: price, Amount: amount}
	}
	return nil
}
