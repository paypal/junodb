package util

import (
	"sync"
)

type BufferPool interface {
	Get() *PPBuffer
	Put(buf *PPBuffer)
}

// sync.Pool based buffer pool
type SyncBufferPool struct {
	pool sync.Pool
	size int
}

func NewSyncBufferPool(size int) BufferPool {
	p := sync.Pool{
		New: func() interface{} {
			buf := new(PPBuffer)
			buf.Grow(size)
			return buf
		},
	}

	return &SyncBufferPool{pool: p}
}

func (p *SyncBufferPool) Get() *PPBuffer {
	item := p.pool.Get()
	buf, ok := item.(*PPBuffer)
	if !ok {
		buf = new(PPBuffer)
		buf.Grow(p.size)
	}
	return buf
}

func (p *SyncBufferPool) Put(buf *PPBuffer) {
	buf.Reset()
	p.pool.Put(buf)
}

// channel based buffer pool
type ChanBufferPool struct {
	poolCh chan *PPBuffer
	size   int
}

func NewChanBufferPool(chansize int, bufsize int) BufferPool {
	p := &ChanBufferPool{
		poolCh: make(chan *PPBuffer, chansize),
		size:   bufsize,
	}

	return p
}

func (p *ChanBufferPool) Get() (buf *PPBuffer) {
	select {
	case buf = <-p.poolCh:
	default:
		//			fmt.Println("New Buffer @ ", time.Now())
		//		debug.PrintStack()
		buf = new(PPBuffer)
		buf.Grow(p.size)
	}

	return buf
}

func (p *ChanBufferPool) Put(buf *PPBuffer) {
	buf.Reset()
	select {
	case p.poolCh <- buf:
	default:
		// do nothing, will be gc
	}
}
