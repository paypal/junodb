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
	"fmt"
	"strconv"
	"strings"

	"github.com/paypal/junodb/pkg/util"
)

type RedistSet struct {
	numShards       int
	numMShardGroups int
	startId         int
	endId           int
	payloadLen      int
	payload         []byte

	shardCount      []uint32
	microShardCount [][256]uint32

	excludeMShards [256]int
}

func NewRedistSet(numShards int, numMShardGroups int, startId int, endId int, payloadLen int, excludesmshards string) IDataSet {

	r := &RedistSet{
		numShards:       numShards,
		numMShardGroups: numMShardGroups,
		startId:         startId,
		endId:           endId,
		payloadLen:      payloadLen,
		shardCount:      make([]uint32, numShards),
		microShardCount: make([][256]uint32, numShards),
	}

	r.payload = NewPayload(r.payloadLen)
	// parsing exclude micro shards list

	var err error
	list := strings.Split(excludesmshards, ",")
	for _, item := range list {
		rg := strings.Split(item, "-")
		var s, e int

		s, err = strconv.Atoi(rg[0])
		if err != nil {
			continue
		}

		if len(rg) < 2 {
			e = s
		} else {
			e, err = strconv.Atoi(rg[1])
			if err != nil {
				e = s
			}
		}
		for i := s; i <= e; i++ {
			r.excludeMShards[i] = 1
		}
	}

	return r
}

func (r *RedistSet) getKey(seed int) (KEY, int) {
	key := NewRandomKey(seed, GLD_SIG)
	shardid, mshardid := util.GetShardIds(key, uint32(r.numShards), uint32(256))
	if r.excludeMShards[mshardid] == 1 {
		fmt.Printf("excluded mshard, shardid=%d, mshardid=%d\n", shardid, mshardid)
		return nil, -1
	}
	r.shardCount[shardid]++
	r.microShardCount[shardid][mshardid]++
	return key, int(shardid)
}

func (r *RedistSet) Dump() {
	r.DumpStats()
}

func (r *RedistSet) DumpStats() {
	for i := 0; i < r.numShards; i++ {
		fmt.Printf("shard id %d, cnt=%d\n", i, r.shardCount[i])
		numMShardsPerGroup := 256 / r.numMShardGroups

		for j := 0; j < r.numMShardGroups; j++ {
			start_mshard := j * numMShardsPerGroup
			end_mshard := start_mshard + numMShardsPerGroup - 1
			if j == r.numMShardGroups-1 { // last group, may have extras
				end_mshard = 255
			}
			count := 0
			for k := start_mshard; k <= end_mshard; k++ {
				count += int(r.microShardCount[i][k])
			}
			fmt.Printf("shard id %d, micro shards (%d - %d), cnt=%d\n", i, start_mshard, end_mshard, count)
		}
	}
}

func (r *RedistSet) Insert(cli *JunoClient) bool {

	err_cnt := 0
	for i := r.startId; i < r.endId; i++ {
		key, shardid := r.getKey(i)
		if key == nil {
			continue
		}
		fmt.Printf("%x\n", key)
		plen := RandomLen(r.payloadLen)
		if !cli.AddKey(shardid, key, r.payload[0:plen]) {
			err_cnt++
		}
	}
	fmt.Printf("error count: %d\n", err_cnt)
	return err_cnt == 0
}

func (r *RedistSet) Delete(cli *JunoClient) bool {

	err_cnt := 0
	for i := r.startId; i < r.endId; i++ {
		key, shardid := r.getKey(i)
		if key == nil {
			continue
		}
		if !cli.DelKey(shardid, key) {
			err_cnt++
		}
	}
	fmt.Printf("error count: %d\n", err_cnt)
	return err_cnt == 0
}

func (r *RedistSet) Get(cli *JunoClient) bool {

	err_cnt := 0
	for i := r.startId; i < r.endId; i++ {
		key, shardid := r.getKey(i)
		if key == nil {
			continue
		}
		res, _ := cli.GetKey(shardid, key)
		if !res {
			err_cnt++
		}
	}
	fmt.Printf("error count: %d\n", err_cnt)
	return err_cnt == 0
}
