package redist

import (
	"juno/pkg/io"
	"juno/pkg/util"
	"time"
)

type Config struct {
	// throttle the request forward rate for snapshot
	SnapshotRateLimit int64 // KBps

	RedistRespTimeout  util.Duration
	MaxWaitTime        int // in second
	ConcurrentSnapshot uint16

	// when forwarding failed, limit the number of retries allowed
	MaxRetry uint16

	ErrThreshold  float64 // in pct, e.g. 1 means 1pct
	DropThreshold float64 // in pct

	ErrThresholdRealtime  float64
	DropThresholdRealtime float64

	Outbound io.OutboundConfig
}

var DefRedistConfig = Config{
	SnapshotRateLimit:  10000, // default: 10MBps
	RedistRespTimeout:  util.Duration{5000 * time.Millisecond},
	MaxWaitTime:        3 * 60, // 3 minutes
	ConcurrentSnapshot: 1,
	MaxRetry:           3,
	ErrThreshold:       0.01,
	DropThreshold:      0,

	ErrThresholdRealtime:  0.01,
	DropThresholdRealtime: 0,

	Outbound: io.OutboundConfig{
		ConnectTimeout:        util.Duration{1 * time.Second},
		ReqChanBufSize:        80000,
		MaxPendingQueSize:     8092,
		PendingQueExtra:       50,
		MaxBufferedWriteSize:  64 * 1024, // default 64k
		ReconnectIntervalBase: 100,       // 100 ms
		ReconnectIntervalMax:  10000,     // 10 seconds
		NumConnsPerTarget:     1,
		IOBufSize:             64 * 1024,
	},
}

var RedistConfig = DefRedistConfig
