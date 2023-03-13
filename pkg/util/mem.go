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

//"bytes"
//"sync"

//128, 512, 1024, 4*1024, 16*1024, 64*1024, 128*1024, 256*1024
var (
	bpool128  BytePool
	bpool512  BytePool
	bpool1k   BytePool
	bpool4k   BytePool
	bpool16k  BytePool
	bpool64k  BytePool
	bpool128k BytePool
	bpool256k BytePool
)

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

	bpool128 = NewChanBytePool(20000, 128)
	bpool512 = NewChanBytePool(20000, 512)
	bpool1k = NewChanBytePool(20000, 1024)
	bpool4k = NewChanBytePool(20000, 4*1024)
	bpool16k = NewChanBytePool(500, 16*1024)
	bpool64k = NewChanBytePool(200, 64*1024)
	bpool128k = NewChanBytePool(100, 128*1024)
	bpool256k = NewChanBytePool(50, 256*1024)

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

func GetBytePool(size int) BytePool {
	if size > 256*1024 {
		return nil
	}

	if size > 128*1024 {
		return bpool256k
	}

	if size > 64*1024 {
		return bpool128k
	}

	if size > 16*1024 {
		return bpool64k
	}

	if size > 4*1024 {
		return bpool16k
	}

	if size > 1024 {
		return bpool4k
	}

	if size > 512 {
		return bpool1k
	}

	if size > 128 {
		return bpool512
	}
	return bpool128
}
