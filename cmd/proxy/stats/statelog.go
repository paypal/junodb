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
	"bufio"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	//	"juno/third_party/forked/golang/glog"

	"juno/pkg/cluster"
	"juno/pkg/io"
	"juno/pkg/proto"
	"juno/pkg/stats"
	"juno/pkg/util"
)

// counters
const (
	ActiveCreate = iota
	ActiveGet
	ActiveUpdate
	ActiveSet
	ActiveDestroy
	ActiveUDFGet
	ActiveUDFSet
	NumErrors
	LastCounter // insert new counter before this
)

const (
	kDefaultStateFieldWidth = 6
)

var (
	statelog StateLog = StateLog{}
	initOnce sync.Once
	enabled  bool = false
)

var (
	// states
	statsNumOkShards    uint32
	statsNumBadShards   uint32
	statsNumWarnShards  uint32
	statsNumAlertShards uint32
	statsTPS            uint32
	statsEPS            uint32
	statsEMA            uint32
	statsNumRead        uint32
	statsNumWrite       uint32
	statsProcCpuUsage   float32
	statsMachCpuUsage   float32

	statsNumReqProcessed uint64

	listeners []io.IListener

	rusage     *syscall.Rusage
	rusageTime time.Time

	machTime    time.Time
	machCpuTick uint16
	machUser    uint64
	machSystem  uint64

	isErrorResponse [256]bool
)

func init() {
	isErrorResponse[proto.OpStatusBadMsg] = true
	isErrorResponse[proto.OpStatusBadParam] = true
	isErrorResponse[proto.OpStatusReqProcTimeout] = true
	isErrorResponse[proto.OpStatusNoStorageServer] = true
}

type (
	StateLog struct {
		stats.StateLog

		cnts      [LastCounter]util.AtomicCounter
		curNumReq uint64 // snapshot
		curNumErr int32  // snapshot
		respTime  int32

		// for calculating moving average processing time
		emaProctime   [proto.OpCodeLastProxyOp]int32
		emaWindowSize uint32
	}
)

func Enabled() bool {
	return enabled
}

func RunCollector() {
	Init(0, "")
}

func Init(id int32, logfilepath string) {
	initOnce.Do(func() {
		enabled = true
		if enabled {
			statelog.Init(fmt.Sprintf("%d", id), logfilepath, (id == 0), &statelog, []stats.IState{})
			statelog.emaWindowSize = 39 // multipler = 2/(39+1) => 0.2
			for i := 0; i <= int(proto.OpCodeDestroy); i++ {
				statelog.emaProctime[i] = 0
			}
			InitProcCpuUsage()
			InitMachCpuUsage()

			statelog.AddStateWriter(&stateLogShmWriter{})
			statelog.Run()
		}
	})
}

func Quit() {
	statelog.Quit()
}

func GetActiveCreateCounter() *util.AtomicCounter {
	if !enabled {
		return nil
	}
	return &statelog.cnts[ActiveCreate]
}

func GetActiveGetCounter() *util.AtomicCounter {
	if !enabled {
		return nil
	}
	return &statelog.cnts[ActiveGet]
}

func GetActiveSetCounter() *util.AtomicCounter {
	if !enabled {
		return nil
	}
	return &statelog.cnts[ActiveSet]
}

func GetActiveUpdateCounter() *util.AtomicCounter {
	if !enabled {
		return nil
	}
	return &statelog.cnts[ActiveUpdate]
}

func GetActiveDestroyCounter() *util.AtomicCounter {
	if !enabled {
		return nil
	}
	return &statelog.cnts[ActiveDestroy]
}

func GetActiveUDFGetCounter() *util.AtomicCounter {
	if !enabled {
		return nil
	}
	return &statelog.cnts[ActiveUDFGet]
}

func GetActiveUDFSetCounter() *util.AtomicCounter {
	if !enabled {
		return nil
	}
	return &statelog.cnts[ActiveUDFSet]
}

func SendProcState(st stats.ProcStat) {
	statelog.SendProcState(st)
}

// http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_averages

// collect stats
func (l *StateLog) ProcessStateChange(stat stats.ProcStat) {
	if enabled {
		atomic.AddUint64(&statsNumReqProcessed, 1)
		if isErrorResponse[stat.ResponseStatus] {
			l.cnts[NumErrors].Add(1)
		}
		stats.CollectStatsByAppNamespace(&stat)
	}

	//EMA: {Close - EMA(previous day)} x multiplier + EMA(previous day).
	// Multipler = 2/(window_size + 1)
	prevEMA := int32(l.emaProctime[stat.Opcode])
	curEMA := (int32(stat.ProcTime)-prevEMA)*2.0/(int32(l.emaWindowSize)+1) + prevEMA
	atomic.StoreInt32(&l.emaProctime[stat.Opcode], curEMA)

	// overall
	prevEMA = l.emaProctime[0]
	curEMA = (int32(stat.ProcTime)-prevEMA)*2.0/int32(l.emaWindowSize+1) + prevEMA
	atomic.StoreInt32(&l.emaProctime[0], curEMA)
}

// called before write to the state log file
func (l *StateLog) ProcessWrite(cnt int) {

	statsTPS, statsEPS = l.getThroughputAndErrorRate()
	statsNumRead = uint32(statelog.cnts[ActiveGet].Get())
	statsNumRead += uint32(statelog.cnts[ActiveUDFGet].Get())

	statsNumWrite = uint32(statelog.cnts[ActiveCreate].Get())
	statsNumWrite += uint32(statelog.cnts[ActiveSet].Get())
	statsNumWrite += uint32(statelog.cnts[ActiveUpdate].Get())
	statsNumWrite += uint32(statelog.cnts[ActiveDestroy].Get())
	statsNumWrite += uint32(statelog.cnts[ActiveUDFSet].Get())

	ema := uint32(atomic.LoadInt32(&l.emaProctime[0]))
	if statsTPS <= 0 {
		ema = 0
	}

	atomic.StoreUint32(&statsEMA, ema)

	// we check SS connectivity 30 seconds, but on lead worker only
	if cnt%30 == 0 {
		statsNumOkShards, statsNumBadShards, statsNumWarnShards, statsNumAlertShards = cluster.GetShardMgr().GetSSConnectivityStats()
	}

	// get process's/machine's cpu usage every 10 seconds
	if cnt%10 == 0 {
		statsProcCpuUsage = ProcCpuUsage()
		statsMachCpuUsage = MachCpuUsage()
	}
}

func (l *StateLog) getThroughputAndErrorRate() (tps uint32, eps uint32) {
	// take a snap shot of errcnt and numreqs
	numErrs := l.cnts[NumErrors].Get()
	numReqs := atomic.LoadUint64(&statsNumReqProcessed)

	tps = uint32(numReqs - l.curNumReq)

	eps = 0
	if tps > 0 {
		eps = uint32(numErrs - l.curNumErr)
	}

	l.curNumErr = numErrs
	l.curNumReq = numReqs

	return
}

func SetListeners(lsnrs []io.IListener) {
	listeners = lsnrs
	var tmp []stats.IState

	for _, lsnr := range lsnrs {
		var name string
		var width int = 8
		if lsnr.GetType() == io.ListenerTypeTCPwSSL {
			name = "ssl_conns"
			width = 12
		} else {
			name = "conns"
		}
		l := lsnr
		st := stats.NewGenState(name, name,
			func() string {
				return strconv.Itoa(int(l.GetNumActiveConnections()))
			},
			width)
		tmp = append(tmp, st)
	}
	sz := len(tmp)
	for i := sz - 1; i >= 0; i-- {
		AddState(tmp[i], false)
	}

}

func AddState(st stats.IState, append bool) {
	if append {
		statelog.AddState(st)
	} else {
		statelog.AddStatePrepend(st)
	}
}

func GetThroughputEmaErrorRate() (tps uint32, ema uint32, eps uint32) {
	tps = atomic.LoadUint32(&statsTPS)
	ema = atomic.LoadUint32(&statsEMA)
	eps = atomic.LoadUint32(&statsEPS)
	if int32(tps) <= 0 { ///TODO
		tps = 0
		ema = 0
	}
	return
}

func InitProcCpuUsage() {
	rusage = new(syscall.Rusage)
	rusageTime = time.Now()
	syscall.Getrusage(syscall.RUSAGE_SELF, rusage)
}

func ProcCpuUsage() (usage float32) {
	nextRusage := new(syscall.Rusage)
	nextTime := time.Now()
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, nextRusage); err == nil {
		secs := nextRusage.Stime.Sec + nextRusage.Utime.Sec - rusage.Stime.Sec - rusage.Utime.Sec
		usecs := nextRusage.Stime.Usec + nextRusage.Utime.Usec - rusage.Stime.Usec - rusage.Utime.Usec
		duration := float64(secs) + float64(usecs)*1.0e-6

		elapsed := nextTime.Sub(rusageTime)
		usage = float32((duration / elapsed.Seconds()) * 100)

		rusageTime = nextTime
		rusage = nextRusage
	}

	return
}

func readCPUUsage() (user, system uint64, cpus uint16) {
	file, err := os.Open("/proc/stat")
	if err != nil {
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if fields[0] == "cpu" {
			// cpu user nice system idle iowait irq softirq steal guest guest_nice
			user, _ = strconv.ParseUint(fields[1], 10, 64)
			system, _ = strconv.ParseUint(fields[3], 10, 64)
		} else if strings.HasPrefix(fields[0], "cpu") {
			cpus++
		}
	}

	return
}

func InitMachCpuUsage() {
	machTime = time.Now()
	machUser, machSystem, _ = readCPUUsage()

	machCpuTick = 100
	out, err := exec.Command("getconf", "CLK_TCK").Output()
	if err == nil {
		val, err := strconv.ParseUint(string(out), 10, 16)
		if err == nil && val > 0 {
			machCpuTick = uint16(val)
		}
	}
}

func MachCpuUsage() (usage float32) {
	nextTime := time.Now()
	nextUser, nextSystem, cpus := readCPUUsage()
	elapsed := nextTime.Sub(machTime)

	if cpus > 0 && elapsed.Seconds() > 0.0 {
		ticks := elapsed.Seconds() * float64(machCpuTick) * float64(cpus)
		usage = float32(float64((nextUser-machUser)+(nextSystem-machSystem))/ticks) * 100
	}

	machTime = nextTime
	machUser = nextUser
	machSystem = nextSystem

	return
}
