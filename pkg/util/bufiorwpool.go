package util

import (
	"bufio"
	"io"
	"sync"
)

var (
	bufioReaderPool sync.Pool
	bufioWriterPool sync.Pool
)

func NewBufioReader(r io.Reader, bufSize int) *bufio.Reader {
	if v := bufioReaderPool.Get(); v != nil {
		br := v.(*bufio.Reader)
		br.Reset(r)
		return br
	}
	return bufio.NewReaderSize(r, bufSize)
}

func PutBufioReader(br *bufio.Reader) {
	br.Reset(nil)
	bufioReaderPool.Put(br)
}

func NewBufioWriter(w io.Writer, bufSize int) *bufio.Writer {
	if v := bufioWriterPool.Get(); v != nil {
		bw := v.(*bufio.Writer)
		bw.Reset(w)
		return bw
	}
	return bufio.NewWriterSize(w, bufSize)
}

func PutBufioWriter(bw *bufio.Writer) {
	bw.Reset(nil)
	bufioWriterPool.Put(bw)
}
