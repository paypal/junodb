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
//  Package utility provides the utility interfaces for mux package
//  
package stats

import (
	"math"
	"sync/atomic"
	"time"
	"unsafe"

	"juno/cmd/storageserv/stats/shmstats"
	"juno/pkg/stats"
)

var (
	_ stats.IStatesWriter = (*stateLogShmWriter)(nil)
)

type (
	stateLogShmWriter struct { // Use it for now. Might use the shm directly in StateLog IState
	}
)

func (w *stateLogShmWriter) Write(now time.Time) error {
	if mgr := shmstats.GetCurrentWorkerStatsManager(); mgr != nil {
		mgr.SetReqProcStats(&shmstats.ReqProcStats{
			NumRequests:       atomic.LoadUint64(&statsNumRequests),
			RequestsPerSecond: atomic.LoadUint32(&statsRequestsPerSecond),
			AvgReqProcTime:    atomic.LoadUint32(&statsReqProcEMA),
			NumReads:          atomic.LoadUint64(&statsNumRequestsByType[kRead]),
			NumDeletes:        atomic.LoadUint64(&statsNumRequestsByType[kDelete]),
			NumCommits:        atomic.LoadUint64(&statsNumRequestsByType[kCommit]),
			NumAborts:         atomic.LoadUint64(&statsNumRequestsByType[kAbort]),
			NumRepairs:        atomic.LoadUint64(&statsNumRequestsByType[kRepair]),
			NumMarkDeletes:    atomic.LoadUint64(&statsNumRequestsByType[kMarkDelete]),
			ProcCpuUsage:      math.Float32frombits(atomic.LoadUint32(((*uint32)(unsafe.Pointer(&statsProcCpuUsage))))),
			MachCpuUsage:      math.Float32frombits(atomic.LoadUint32(((*uint32)(unsafe.Pointer(&statsMachCpuUsage))))),
		})
		mgr.SetStorageStats(&shmstats.StorageStats{
			Free:                atomic.LoadUint64(&statsFreeStorageSpace),
			Used:                atomic.LoadUint64(&statsUsedStorageSpace),
			NumKeys:             atomic.LoadUint64(&statsNumKeys),
			MaxDBLevel:          atomic.LoadUint32(&statsMaxDBLevel),
			CompSecByInterval:   atomic.LoadUint32(&statsCompSecByInterval),
			CompCountByInterval: atomic.LoadUint32(&statsCompCountByInterval),
			PendingCompKBytes:   atomic.LoadUint64(&statsPendingCompKBytes),
			DelayedWriteRate:    atomic.LoadUint64(&statsDelayedWriteRate),
		})
	}

	return nil
}

func (w *stateLogShmWriter) Close() error {
	return nil
}
