package bybit

import (
	"bytes"
	"sync"
)

func recyclePooledBuffer(pool *sync.Pool, buf *bytes.Buffer) {
	if buf == nil {
		return
	}
	if buf.Cap() > maxPooledReadBufCap {
		return
	}
	buf.Reset()
	pool.Put(buf)
}
