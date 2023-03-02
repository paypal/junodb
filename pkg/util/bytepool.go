package util

import (
     "sync"
     _"fmt"
)

type BytePool interface {
	Get() ([]byte)
	Put([]byte)
}

// sync.Pool based buffer pool
type SyncBytePool struct {
	pool sync.Pool
	size int
}

	
func NewSyncBytePool(size int) (BytePool) {
	p := sync.Pool {
		New: func() interface{} { return make([]byte, size) },
	}
	
	return &SyncBytePool{pool : p, size : size}
}

func (p *SyncBytePool) Get() ([]byte) {
	item := p.pool.Get()
	buf, ok := item.([]byte)
	if !ok {
		buf = make([]byte, p.size)
	}
	return buf
}

func (p *SyncBytePool) Put(buf []byte) {
	p.pool.Put(buf[:cap(buf)])
}

// channel based buffer pool
type ChanBytePool struct {
	poolCh chan []byte
	size int
}

func NewChanBytePool(chansize int, bytesize int) (BytePool) {
	p := & ChanBytePool {
		poolCh : make(chan []byte, chansize),
		size : bytesize,
	}
	
	return p
}

func (p *ChanBytePool) Get() (b []byte) {
	select {
		case b = <- p.poolCh:
			//fmt.Printf("byte returned from channel\n")

		default:
			b = make([]byte, p.size)
			//fmt.Printf("new []byte\n")
	}
	
	return b
}

func (p *ChanBytePool) Put(b []byte) {
	select {
		case p.poolCh <- b[:cap(b)]:
		default:
			// do nothing, will be gc
	}
}