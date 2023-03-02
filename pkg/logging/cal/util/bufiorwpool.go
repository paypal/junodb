package util

import (
     "sync"
     "bufio"
     "io"
)

var (
	bufioReaderPool   sync.Pool
	bufioWriterPool	  sync.Pool
)

func NewBufioReader(r io.Reader) *bufio.Reader {
        if v := bufioReaderPool.Get(); v != nil {
                br := v.(*bufio.Reader)
                br.Reset(r)
                return br
        }
        return bufio.NewReader(r)
}

func PutBufioReader(br *bufio.Reader) {
        br.Reset(nil)
        bufioReaderPool.Put(br)
}

func NewBufioWriter(w io.Writer) *bufio.Writer {
        if v := bufioWriterPool.Get(); v != nil {
                bw := v.(*bufio.Writer)
                bw.Reset(w)
                return bw
        }
        return bufio.NewWriter(w)
}

func PutBufioWriter(bw *bufio.Writer) {
        bw.Reset(nil)
        bufioWriterPool.Put(bw)
}