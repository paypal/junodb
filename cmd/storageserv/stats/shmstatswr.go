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
