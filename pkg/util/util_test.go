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
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

func RandomKey(s int) []byte {
	off := 0

	key := make([]byte, 16+off)
	r := uint32(((int64(s+1)*25214903917 + 11) >> 5) & 0x7fffffff)
	binary.BigEndian.PutUint32(key[0+off:], r)
	binary.BigEndian.PutUint32(key[4+off:], uint32(s))
	return key
}

func TestMicroShardIdDistribution(t *testing.T) {

	const numShards uint32 = 1024
	const numMicroShards uint32 = 128
	var COUNTS_MSHARDS [numShards][numMicroShards]uint32
	var COUNTS_SHARDS [numShards]uint32

	var total int = 50000000
	for i := 0; i < total; i++ {
		key := RandomKey(i)
		shardId, microShardId := GetShardIds([]byte(key), numShards, numMicroShards)
		//fmt.Printf("shardId=%d, microShardId=%d \n", shardId, microShardId)
		COUNTS_MSHARDS[shardId][microShardId]++
		COUNTS_SHARDS[shardId]++
	}

	avg := float64(total) / float64(numShards)
	for i := 0; i < int(numShards); i++ {
		pct := math.Abs(float64(COUNTS_SHARDS[i])-avg) / avg
		if pct >= 0.02 {
			fmt.Printf("shard_id=%d, count=%d, pct=%v\n", i, COUNTS_SHARDS[i], pct)
		}

		avg2 := float64(COUNTS_SHARDS[i]) / float64(numMicroShards)
		for j := 0; j < int(numMicroShards); j++ {
			pct2 := math.Abs(float64(COUNTS_MSHARDS[i][j])-avg2) / avg2
			if pct2 >= 0.15 {
				fmt.Printf("shard_id=%d, micro_id=%d, count=%d, pct=%v, avg=%v\n",
					i, j, COUNTS_MSHARDS[i][j], pct2, avg2)
			}

		}
	}
}
