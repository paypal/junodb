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

package stats

import (
	"sync"
	"sync/atomic"

	"juno/pkg/proto"
)

const (
	kEmaMultiplier float32 = 0.05
)

var (
	statsByAppNamespace sync.Map
	needToRecordPayload [256]bool
	needToRecordTTL     [256]bool
	indexAppNamespace   uint32
)

func init() {
	needToRecordPayload[proto.OpCodeCreate] = true
	needToRecordPayload[proto.OpCodeUpdate] = true
	needToRecordPayload[proto.OpCodeSet] = true
	needToRecordTTL[proto.OpCodeUDFSet] = true

	needToRecordTTL[proto.OpCodeCreate] = true
	needToRecordTTL[proto.OpCodeGet] = true
	needToRecordTTL[proto.OpCodeUpdate] = true
	needToRecordTTL[proto.OpCodeSet] = true
	needToRecordTTL[proto.OpCodeUDFGet] = true
}

type (
	appNamespaceStatsMapT sync.Map

	AppNamespaceStats struct {
		AvgPayloadLen uint32
		MaxPayloadLen uint32
		AvgTimeToLive uint32
		MaxTimeToLive uint32
	}

	anStatsMapEntryT struct {
		mtx   sync.Mutex
		stats *AppNamespaceStats
		index uint32
	}
)

func RangeAppNamespaceStats(f func(index uint32, key []byte, st *AppNamespaceStats)) {
	statsByAppNamespace.Range(func(key, value interface{}) bool {
		if v, ok := value.(*anStatsMapEntryT); ok {
			v.mtx.Lock()
			f(v.index, []byte(key.(string)), v.stats)
			v.mtx.Unlock()
		}
		return true
	})
}

/*
map
	<string> <*anStatsMapEntryT>
*/

func CollectStatsByAppNamespace(pst *ProcStat) {
	if pst != nil {
		key := pst.GroupKey
		opcode := pst.Opcode
		pl := pst.RequestPayloadLen
		ttl := pst.RequestTimeToLive

		if value, ok := statsByAppNamespace.Load(string(key)); ok {
			if st, ok := value.(*anStatsMapEntryT); ok {
				st.mtx.Lock()
				if st.stats != nil {
					if needToRecordPayload[opcode] {
						if st.stats.MaxPayloadLen < pl {
							st.stats.MaxPayloadLen = pl
						}
						apl := uint32(float32(pl-st.stats.AvgPayloadLen)*kEmaMultiplier) + st.stats.AvgPayloadLen
						if apl > st.stats.MaxPayloadLen {
							st.stats.AvgPayloadLen = st.stats.MaxPayloadLen
						} else {
							st.stats.AvgPayloadLen = apl
						}
					}
					if needToRecordTTL[opcode] && ttl != 0 {
						if st.stats.MaxTimeToLive < ttl {
							st.stats.MaxTimeToLive = ttl
						}

						attl := uint32(float32(ttl-st.stats.AvgTimeToLive)*kEmaMultiplier) + st.stats.AvgTimeToLive
						if attl > st.stats.MaxTimeToLive {
							st.stats.AvgTimeToLive = st.stats.MaxTimeToLive
						} else {
							st.stats.AvgTimeToLive = attl
						}
					}
				}
				st.mtx.Unlock()
			}
		} else {
			value := &anStatsMapEntryT{
				stats: &AppNamespaceStats{MaxPayloadLen: pl, AvgPayloadLen: pl, MaxTimeToLive: ttl, AvgTimeToLive: ttl},
				index: atomic.LoadUint32(&indexAppNamespace),
			}
			if actual, loaded := statsByAppNamespace.LoadOrStore(string(key), value); loaded {
				if st, ok := actual.(*anStatsMapEntryT); ok {
					st.mtx.Lock()
					if st.stats != nil {
						if needToRecordPayload[opcode] {
							if st.stats.MaxPayloadLen < pl {
								st.stats.MaxPayloadLen = pl
							}
						}
						if needToRecordTTL[opcode] {
							if st.stats.MaxTimeToLive < ttl {
								st.stats.MaxTimeToLive = ttl
							}
						}

					}
					st.mtx.Unlock()
				}
			} else {
				atomic.AddUint32(&indexAppNamespace, 1)
			}
		}
	}
}
