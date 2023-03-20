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
	"juno/pkg/util"
)

type KEY []byte

type IDataSet interface {
	Insert(cli *JunoClient) bool
	Delete(cli *JunoClient) bool
	Get(cli *JunoClient) bool
	Dump()
}

type GoldenSet struct {
	shardCollection []KEY
	numShards       int
	startId         int
	populated       bool
	payloadLen      int
	payload         []byte
}

func NewGoldenSet(numShards int, startId int, payploadLen int) IDataSet {

	gs := &GoldenSet{
		numShards:       numShards,
		shardCollection: make([]KEY, numShards),
		startId:         startId,
		populated:       false,
		payloadLen:      payploadLen,
	}

	gs.payload = NewPayload(gs.payloadLen)
	return gs
}

func (gs *GoldenSet) PopulateKeys() {

	seed := gs.startId

	for cnt := 0; cnt < gs.numShards; {
		key := NewRandomKey(seed, GLD_SIG)
		seed++
		shardid, _ := util.GetShardIds(key, uint32(gs.numShards), 256)
		if len(gs.shardCollection[shardid]) > 0 {
			//fmt.Printf("key existed, seed %d, continue\n", seed-1)
			continue
		}

		// note: we only increment count if there is no dupe.
		gs.shardCollection[shardid] = make(KEY, len(key))
		copy(gs.shardCollection[shardid], key)
		//fmt.Printf("count: %d, seed %d\n", cnt, seed)
		cnt++
	}

	gs.populated = true
}

func (gs *GoldenSet) Dump() {
	if !gs.populated {
		fmt.Print("Empty set\n")
		return
	}

	for i := 0; i < gs.numShards; i++ {
		payloadLen := RandomLenForShard(gs.payloadLen, i)
		fmt.Printf("shard:%d, key: %#x, payload len: %d\n",
			i, gs.shardCollection[i], payloadLen)
	}
}

func (gs *GoldenSet) Insert(cli *JunoClient) bool {
	if !gs.populated {
		gs.PopulateKeys()
	}

	err_cnt := 0
	for i := 0; i < gs.numShards; i++ {
		len := RandomLenForShard(gs.payloadLen, i)
		//fmt.Printf("Shard id: %d, key: %#x\n", i, gs.shardCollection[i])
		if !cli.AddKey(i, gs.shardCollection[i], gs.payload[0:len]) {
			err_cnt++
		}
	}
	fmt.Printf("error count: %d\n", err_cnt)
	return err_cnt == 0
}

func (gs *GoldenSet) Delete(cli *JunoClient) bool {
	if !gs.populated {
		gs.PopulateKeys()
	}

	err_cnt := 0
	for i := 0; i < gs.numShards; i++ {
		if !cli.DelKey(i, gs.shardCollection[i]) {
			err_cnt++
		}
	}
	fmt.Printf("error count: %d\n", err_cnt)
	return err_cnt == 0
}

func (gs *GoldenSet) Get(cli *JunoClient) bool {
	if !gs.populated {
		gs.PopulateKeys()
	}

	err_cnt := 0
	for i := 0; i < gs.numShards; i++ {
		payloadlen := RandomLenForShard(gs.payloadLen, i)
		//fmt.Printf("Shard id: %d, key: %#x\n", i, gs.shardCollection[i])
		res, value := cli.GetKey(i, gs.shardCollection[i])
		if !res || len(value) != payloadlen {
			// todo compare payload content
			err_cnt++
		}
	}
	fmt.Printf("error count: %d\n", err_cnt)
	return err_cnt == 0
}
