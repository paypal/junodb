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
	"math"
	"sync/atomic"
	"time"
	"unsafe"

	"juno/cmd/proxy/stats/shmstats"
	"juno/pkg/logging/cal"
	"juno/pkg/stats"
)

var (
	_ stats.IStatesWriter = (*stateLogShmWriter)(nil)
)

type (
	stateLogShmWriter struct { // Use it for now. Might use the shm directly in StateLog IState
	}
)

type IShmStatsWriterCallBack interface {
	Call()
}

var repStatsCB IShmStatsWriterCallBack

func SetRepStatsCallBack(cb IShmStatsWriterCallBack) {
	repStatsCB = cb
}

func (w *stateLogShmWriter) Write(now time.Time) error {
	if mgr := shmstats.GetCurrentWorkerStatsManager(); mgr != nil {
		if enabled {
			stat := &shmstats.ReqProcStats{
				NumRequests:          atomic.LoadUint64(&statsNumReqProcessed),
				RequestsPerSecond:    atomic.LoadUint32(&statsTPS),
				AvgReqProcTime:       atomic.LoadUint32(&statsEMA),
				ReqProcErrsPerSecond: atomic.LoadUint32(&statsEPS),
				NumReads:             atomic.LoadUint32(&statsNumRead), ///may change theirs types the same
				NumWrites:            atomic.LoadUint32(&statsNumWrite),
				NumBadShards:         uint16(atomic.LoadUint32(&statsNumBadShards)),
				NumWarnShards:        uint16(atomic.LoadUint32(&statsNumWarnShards)),
				NumAlertShards:       uint16(atomic.LoadUint32(&statsNumAlertShards)),
				ProcCpuUsage:         math.Float32frombits(atomic.LoadUint32((*uint32)(unsafe.Pointer(&statsProcCpuUsage)))),
				MachCpuUsage:         math.Float32frombits(atomic.LoadUint32((*uint32)(unsafe.Pointer(&statsMachCpuUsage)))),
			}
			mgr.SetReqProcStats(stat)
			stats.RangeAppNamespaceStats(func(index uint32, appNsKey []byte, st *stats.AppNamespaceStats) {
				mgr.SetAppNsStats(index, appNsKey, st)
			})

		}
		mgr.SetInboundConnStats(listeners)

		// use callback to avoid circular dependancy with Replicator
		if repStatsCB != nil {
			repStatsCB.Call()
		}

		if cal.IsEnabled() {
			mgr.SetCalDropCount(cal.GetCalDropCount())
		}
	}
	return nil
}

func (w *stateLogShmWriter) Close() error {
	return nil
}
