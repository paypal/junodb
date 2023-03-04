//  
//  Copyright 2023 PayPal Inc.
//  
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  
//  Package utility provides the utility interfaces for mux package
//  
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
