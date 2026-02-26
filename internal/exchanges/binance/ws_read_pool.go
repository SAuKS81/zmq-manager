package binance

import (
	"bytes"
	"sync"

	"github.com/gorilla/websocket"
)

var binanceReadBufPool = sync.Pool{
	New: func() any {
		buf := &bytes.Buffer{}
		buf.Grow(16 * 1024)
		return buf
	},
}

type borrowedWSMessage struct {
	msgType int
	payload []byte
	buf     *bytes.Buffer
}

func readWSMessageBorrowed(conn *websocket.Conn) (borrowedWSMessage, error) {
	msgType, r, err := conn.NextReader()
	if err != nil {
		return borrowedWSMessage{}, err
	}

	buf := binanceReadBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	if _, err := buf.ReadFrom(r); err != nil {
		binanceReadBufPool.Put(buf)
		return borrowedWSMessage{}, err
	}

	return borrowedWSMessage{
		msgType: msgType,
		payload: buf.Bytes(),
		buf:     buf,
	}, nil
}

func releaseWSMessage(msg borrowedWSMessage) {
	if msg.buf != nil {
		binanceReadBufPool.Put(msg.buf)
	}
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
