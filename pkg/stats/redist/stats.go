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

package redist

import (
	"fmt"
	"juno/pkg/logging"
	"juno/pkg/util"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

var (
	StatsTotalCount   = []byte("total")
	StatsOkCount      = []byte("ok")
	StatsErrCount     = []byte("err")
	StatsDropCount    = []byte("drop")
	StatsExpiredCount = []byte("expired")
	StatsMicroShardId = []byte("mshd")
	StatsTagStatus    = []byte("st")
	StatsTagElapse    = []byte("et")

	StatsInProgress    = "P"
	StatsFinish        = "F"
	StatsBegin         = "B"
	StatsAbort         = "A"
	StatsPairDelimiter = "&"
	StatsDelimiter     = "="
)

type Stats struct {
	totalCnt util.AtomicCounter
	okCnt    util.AtomicCounter
	failCnt  util.AtomicCounter
	dropCnt  util.AtomicCounter
	expCnt   util.AtomicCounter
	mshardId int32
	status   string

	// for tracking last successful checkpoint
	lastTotalCnt int32
	lastOkCnt    int32
	lastFailCnt  int32
	lastDropCnt  int32
	lastExpCnt   int32
	lastMShardId int32
}

func (r *Stats) SaveCheckPoint() {
	// save the counts for current checkpoint
	r.lastTotalCnt = r.totalCnt.Get()
	r.lastOkCnt = r.okCnt.Get()
	r.lastFailCnt = r.failCnt.Get()
	r.lastDropCnt = r.dropCnt.Get()
	r.lastExpCnt = r.expCnt.Get()
	r.lastMShardId = r.mshardId
}

func (r *Stats) RestoreFromCheckPoint() {
	// restore the counts from the last sucessful checkpoint
	r.totalCnt.Set(r.lastTotalCnt)
	r.okCnt.Set(r.lastOkCnt)
	r.failCnt.Set(r.lastFailCnt)
	r.dropCnt.Set(r.lastDropCnt)
	r.expCnt.Set(r.lastExpCnt)
	r.mshardId = r.lastMShardId
}

func (r *Stats) Restore(ch *Stats) {
	r.lastTotalCnt = ch.lastTotalCnt
	r.lastOkCnt = ch.lastOkCnt
	r.lastFailCnt = ch.lastFailCnt
	r.lastDropCnt = ch.lastDropCnt
	r.lastExpCnt = ch.lastExpCnt
	r.lastMShardId = ch.lastMShardId
	r.RestoreFromCheckPoint()
}

func (r *Stats) IncreaseTotalCnt() {
	r.totalCnt.Add(1)
}

func (r *Stats) IncreaseOkCnt() {
	r.okCnt.Add(1)
}

func (r *Stats) IncreaseFailCnt() {
	r.failCnt.Add(1)
}

func (r *Stats) IncreaseDropCnt() {
	r.dropCnt.Add(1)
}

func (r *Stats) IncreaseExpireCnt() {
	r.expCnt.Add(1)
}

func (r *Stats) SetMShardId(id int32) {
	r.lastMShardId = r.mshardId
	atomic.StoreInt32(&r.mshardId, id)
}

func (r *Stats) GetMShardId() int32 {
	return atomic.LoadInt32(&r.lastMShardId)
}

func (r *Stats) HasOutstandingReqs() bool {
	if r.totalCnt.Get() == r.okCnt.Get()+r.failCnt.Get()+r.dropCnt.Get() {
		return true
	}

	return false
}

func (r *Stats) ShouldAbort(dropTh float64, errTh float64) bool {
	totalCnt := float64(r.totalCnt.Get())
	if totalCnt == 0 {
		return false
	}

	if (float64(r.dropCnt.Get())/totalCnt)*100 > dropTh ||
		(float64(r.failCnt.Get())/totalCnt)*100 > errTh {
		return true
	}
	return false
}

func (r *Stats) GetStatus() string {
	return r.status
}

func (r *Stats) SetStatus(s string) {
	r.status = s
}

func (r *Stats) GetStatsStr(start time.Time) (res string) {
	elapsed := fmt.Sprintf("%s", time.Since(start))
	buf := logging.NewKVBuffer()
	buf.Add(StatsTagStatus, r.status)
	buf.AddInt(StatsTotalCount, int(r.totalCnt.Get()))
	buf.AddInt(StatsOkCount, int(r.okCnt.Get()))
	buf.AddInt(StatsErrCount, int(r.failCnt.Get()))
	buf.AddInt(StatsDropCount, int(r.dropCnt.Get()))
	buf.AddInt(StatsExpiredCount, int(r.expCnt.Get()))
	buf.AddInt(StatsMicroShardId, int(r.GetMShardId()))
	buf.Add(StatsTagElapse, elapsed)

	return string(buf.Bytes())
}

func (r *Stats) Isfinished() bool {
	if r.status == StatsFinish {
		return true
	}

	return false
}

type KVPairs struct {
	kvpairs map[string]string
}

func NewKVPairs(str string) *KVPairs {
	p := &KVPairs{
		kvpairs: make(map[string]string),
	}
	pairs := strings.Split(str, StatsPairDelimiter)
	for _, kv := range pairs {
		v := strings.Split(kv, StatsDelimiter)
		if len(v) >= 2 {
			p.kvpairs[v[0]] = v[1]
		}
	}
	return p
}

func (p *KVPairs) GetInt(key string, defv int) int {
	value := p.kvpairs[key]
	if len(value) == 0 {
		return defv
	}
	s, err := strconv.Atoi(value)
	if err != nil {
		return defv
	}
	return s
}

func (p *KVPairs) GetValue(key string, defv string) string {
	value := p.kvpairs[key]
	if len(value) == 0 {
		return defv
	}
	return value
}

func NewStats(str string) *Stats {
	kvs := NewKVPairs(str)
	st := &Stats{}
	st.status = kvs.GetValue(string(StatsTagStatus), "")
	st.SetMShardId(int32(kvs.GetInt(string(StatsMicroShardId), 0)))
	st.totalCnt.Set(int32(kvs.GetInt(string(StatsTotalCount), 0)))
	st.okCnt.Set(int32(kvs.GetInt(string(StatsOkCount), 0)))
	st.failCnt.Set(int32(kvs.GetInt(string(StatsErrCount), 0)))
	st.dropCnt.Set(int32(kvs.GetInt(string(StatsDropCount), 0)))
	st.expCnt.Set(int32(kvs.GetInt(string(StatsExpiredCount), 0)))

	st.SaveCheckPoint()
	return st
}
