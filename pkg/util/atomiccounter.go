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
	"sync/atomic"
)

type AtomicCounter struct {
	cnt int32
}

func (c *AtomicCounter) Get() int32 {
	return atomic.LoadInt32(&c.cnt)
}

func (c *AtomicCounter) Add(delta int32) {
	atomic.AddInt32(&c.cnt, delta)
}

func (c *AtomicCounter) Reset() {
	atomic.StoreInt32(&c.cnt, 0)
}

func (c *AtomicCounter) Set(cnt int32) {
	atomic.StoreInt32(&c.cnt, cnt)
}

type AtomicUint64Counter struct {
	cnt uint64
}

func (c *AtomicUint64Counter) Get() uint64 {
	return atomic.LoadUint64(&c.cnt)
}

func (c *AtomicUint64Counter) Add(delta uint64) {
	atomic.AddUint64(&c.cnt, delta)
}

func (c *AtomicUint64Counter) Reset() {
	atomic.StoreUint64(&c.cnt, 0)
}

func (c *AtomicUint64Counter) Set(cnt uint64) {
	atomic.StoreUint64(&c.cnt, cnt)
}

type AtomicShareCounter struct {
	cnt *uint64
}

func (c *AtomicShareCounter) Get() uint64 {
	return atomic.LoadUint64(c.cnt)
}

func (c *AtomicShareCounter) Add(delta uint64) {
	atomic.AddUint64(c.cnt, delta)
}

func (c *AtomicShareCounter) Reset() {
	atomic.StoreUint64(c.cnt, 0)
}

func NewAtomicShareCounter(cnt *uint64) *AtomicShareCounter {
	return &AtomicShareCounter{cnt: cnt}
}
