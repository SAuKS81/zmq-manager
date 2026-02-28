package binance

import (
	"bytes"
	"sync"

	"github.com/gorilla/websocket"
)

var binanceReadBufPool = sync.Pool{
	New: func() any {
		buf := &bytes.Buffer{}
		// Binance combined stream payloads are often above 16KB; pre-grow to cut growSlice churn.
		buf.Grow(32 * 1024)
		return buf
	},
}

func readWSMessagePooled(conn *websocket.Conn) (int, []byte, error) {
	msgType, r, err := conn.NextReader()
	if err != nil {
		return 0, nil, err
	}

	buf := binanceReadBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	_, err = buf.ReadFrom(r)
	if err != nil {
		binanceReadBufPool.Put(buf)
		return 0, nil, err
	}

	msg := append([]byte(nil), buf.Bytes()...)
	binanceReadBufPool.Put(buf)
	return msgType, msg, nil
}
