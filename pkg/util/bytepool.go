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