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

/*
Package util implements some utility functions.
*/
package util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/spaolacci/murmur3"
)

// http://blog.sgmansfield.com/2015/12/goroutine-ids/
// Goroutine Id, used for debugging purpose
func GetGID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

func Murmur3Hash(data []byte) uint32 {
	return murmur3.Sum32(data)
}

func GetPartitionId(key []byte, numShards uint32) uint16 {
	hashcode := Murmur3Hash(key)
	return uint16(hashcode % numShards)
}

func GetShardInfoByKey(key []byte, numShards uint32, numZones uint32, AlgVersion uint32) (shardId uint16, start_zoneid uint32) {

	hashcode := Murmur3Hash(key)
	shardId = uint16(hashcode % numShards)

	//hashCode, shardId := util.GetHashAndShardId(key, uint32(p.shardMap.cluster.NumShards))

	if AlgVersion == 1 {
		start_zoneid = uint32(shardId+1) % numZones
	} else {
		// for new algorithm, starting zone id is based on hash
		start_zoneid = (hashcode >> 16) % numZones
	}

	return shardId, start_zoneid
}

// lower two bytes are used for Shard Id, and higher two bytes are used for Micro Shard Id
func GetShardIds(key []byte, numShards uint32, numMicroShards uint32) (shardId uint16, microShardId uint8) {
	hashcode := Murmur3Hash(key)
	if numMicroShards == 0 {
		shardId = GetPartitionId(key, numShards)
		return
	}
	shardId = uint16(hashcode % numShards)
	microShardId = uint8((hashcode >> 16) % uint32(numMicroShards))
	return
}

func GetMicroShardId(key []byte, numMicroShards uint32) (microShardId uint8) {
	if numMicroShards == 0 {
		microShardId = 0
		return
	}

	hashcode := Murmur3Hash(key)

	if numMicroShards > 256 {
		numMicroShards = 256
	}
	microShardId = uint8((hashcode >> 16) % numMicroShards)
	return
}

type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

func (d Duration) MarshalText() (text []byte, err error) {
	text = []byte(d.Duration.String())
	return
}

func GetTimeToLive(expirationTime uint32) uint32 {
	return GetTimeToLiveFrom(expirationTime, time.Now())
}

func GetTimeToLiveFrom(expirationTime uint32, now time.Time) uint32 {
	nowUnix := now.Unix()
	exp := int64(expirationTime)

	if exp > nowUnix {
		return uint32(exp - nowUnix)
	}
	return 0
}

func GetExpirationTimeFrom(now time.Time, ttl uint32) (expirationTime uint32) {
	exp := now.Unix() + int64(ttl)
	if exp > math.MaxUint32 {
		expirationTime = math.MaxUint32
	} else {
		expirationTime = uint32(exp)
	}
	return
}

func GetExpirationTime(ttl uint32) uint32 {
	return GetExpirationTimeFrom(time.Now(), ttl)
}

func Now() uint32 {
	return uint32(time.Now().Unix())
}

func Now64() uint64 {
	return uint64(time.Now().Unix())
}

const uuidEpoch = 122192928000000000 // UUID epoch (October 15, 1582)

func GetTimeFromUUIDv1(id uuid.UUID) (tm time.Time, err error) {
	if id[6]&0xF0 != 0x10 {
		err = fmt.Errorf("not v1 UUID")
		return
	}
	var buf [8]byte
	buf[0] = id[6] & 0xF
	buf[1] = id[7]
	buf[2] = id[4]
	buf[3] = id[5]
	copy(buf[4:], id[:4])

	timestamp := (binary.BigEndian.Uint64(buf[:]) - uuidEpoch) * 100
	tm = time.Unix(0, int64(timestamp))
	return
}

// in MB
func GetTotalMemMB() int {
	file, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0
	}

	var size int
	fmt.Fscanf(file, "MemTotal: %d kB", &size)
	size /= 1024

	return size
}
