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
