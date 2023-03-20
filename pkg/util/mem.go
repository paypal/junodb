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

var (
	bufferpool128  BufferPool
	bufferpool512  BufferPool
	bufferpool1k   BufferPool
	bufferpool4k   BufferPool
	bufferpool8k   BufferPool
	bufferpool16k  BufferPool
	bufferpool32k  BufferPool
	bufferpool64k  BufferPool
	bufferpool128k BufferPool
	bufferpool     BufferPool // anything greater than 128k, use this pool
)

func init() {
	bufferpool128 = NewChanBufferPool(5000, 128)
	bufferpool512 = NewChanBufferPool(5000, 512)
	bufferpool1k = NewChanBufferPool(5000, 1024)
	bufferpool4k = NewChanBufferPool(2000, 4*1024)
	bufferpool8k = NewChanBufferPool(2000, 8*1024)
	bufferpool16k = NewChanBufferPool(500, 16*1024)
	bufferpool32k = NewChanBufferPool(500, 32*1024)
	bufferpool64k = NewChanBufferPool(200, 64*1024)
	bufferpool128k = NewChanBufferPool(100, 128*1024)
	bufferpool = NewChanBufferPool(10, 128*1024)
}

func GetBufferPool(size int) BufferPool {

	if size > 128*1024 {
		return bufferpool
	}

	if size > 64*1024 {
		return bufferpool128k
	}

	if size > 32*1024 {
		return bufferpool64k
	}

	if size > 16*1024 {
		return bufferpool32k
	}

	if size > 8*1024 {
		return bufferpool16k
	}

	if size > 4*1024 {
		return bufferpool8k
	}

	if size > 1024 {
		return bufferpool4k
	}

	if size > 512 {
		return bufferpool1k
	}

	if size > 128 {
		return bufferpool512
	}

	return bufferpool128
}
