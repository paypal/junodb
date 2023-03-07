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
  
package gld

import (
	"encoding/binary"
	"math/rand"
)

var (
	GLD_SIG      []byte = []byte("GLD")
	BULKLOAD_SIG []byte = []byte("BULK")
	REDIST_SIG   []byte = []byte("RDIS")
)

// with same seed and signature, key can be generated repeatably
func NewRandomKey(seed int, signature []byte) []byte {

	off := 0
	key := make([]byte, 16+off)
	r := uint32(((int64(seed+1)*25214903917 + 11) >> 5) & 0x7fffffff)
	binary.BigEndian.PutUint32(key[0+off:], r)
	binary.BigEndian.PutUint32(key[4+off:], uint32(seed))
	copy(key[12:], signature[0:3])

	return key
}

func NewPayload(len int) []byte {

	payload := make([]byte, len)
	for i := 0; i < len; i++ {
		val := byte((int64(i)*1103515245 + 12345) & 0xff)
		payload[i] = val
	}

	return payload
}

func RandomLenForShard(originalLen int, shardId int) int {
	min := originalLen / 2
	if min == 0 {
		return 1
	}

	len := min + shardId
	if originalLen < len {
		len = min + shardId%(min)
	}

	return len
}

func RandomLen(original_len int) int {
	min := original_len / 3
	if min == 0 {
		return 1
	}
	len := min + rand.Intn(2*min)
	return len
}
