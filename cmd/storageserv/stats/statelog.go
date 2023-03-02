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
	"bufio"
	"bytes"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"juno/cmd/storageserv/storage/db"
	"juno/pkg/proto"
	"juno/pkg/stats"
)

const (
	kRead = iota
	kDelete
	kCommit
	kAbort
	kRepair
	kMarkDelete
	kNumRequestTypes
)

//Exponential Moving Average (EMA)
var (
	statsNumRequests       uint64
	statsNumRequestsPrev   uint64
	statsRequestsPerSecond uint32
	statsReqProcEMA        uint32 //in us
	statsNumRequestsByType [kNumRequestTypes]uint64
	statsFreeStorageSpace  uint64 // in Megabytes
	statsUsedStorageSpace  uint64 // in Megabytes
	statsNumKeys           uint64
	statsMaxDBLevel        uint32 // 0 to 6 (L0 to L6)
	statsProcCpuUsage      float32
	statsMachCpuUsage      float32

	statsCompSecPrev         uint64
	statsCompCountPrev       uint64
	statsCompSecByInterval   uint32
	statsCompCountByInterval uint32
	statsPendingCompKBytes   uint64
	statsDelayedWriteRate    uint64

	theDbPaths  []string
	rusage      *syscall.Rusage
	rusageTime  time.Time
	machTime    time.Time
	machCpuTick uint16
	machUser    uint64
	machSystem  uint64
)

type DiskStatus struct {
	All   uint64
	Used  uint64
	Free  uint64
	Avail uint64
}

// disk usage of path/disk
func DiskUsage(path string) (disk DiskStatus) {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &fs)
	if err != nil {
		return
	}
	disk.All = fs.Blocks * uint64(fs.Bsize)
	disk.Free = fs.Bfree * uint64(fs.Bsize)
	disk.Avail = fs.Bavail * uint64(fs.Bsize)
	disk.Used = disk.All - disk.Free
	return
}

func SumDiskUsage(paths []string) (disk DiskStatus) {

	for _, path := range paths {
		status := DiskUsage(path)
		disk.All += status.All
		disk.Free += status.Free
		disk.Avail += status.Avail
		disk.Used += status.Used
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
		usage = float32((duration / elapsed.Seconds()) * 100) // converted to tenths to store as uint

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

const (
	B  = 1
	KB = 1024 * B
	MB = 1024 * KB
	GB = 1024 * MB
)

type StateLog struct {
	stats.StateLog
}

var ( //Variables only being set in initialization
	initOnce                sync.Once
	enabled                 bool
	emaWindowSize           uint32                   = 39 // multipler = 2/(39+1) => 0.2
	statsMapNumRequestTypes map[proto.OpCode]*uint64 = map[proto.OpCode]*uint64{
		proto.OpCodeRead:       &statsNumRequestsByType[kRead],
		proto.OpCodeDelete:     &statsNumRequestsByType[kDelete],
		proto.OpCodeCommit:     &statsNumRequestsByType[kCommit],
		proto.OpCodeAbort:      &statsNumRequestsByType[kAbort],
		proto.OpCodeRepair:     &statsNumRequestsByType[kRepair],
		proto.OpCodeMarkDelete: &statsNumRequestsByType[kMarkDelete],
	}

	statelog StateLog = StateLog{}
)

func SendProcState(st stats.ProcStat) {
	statelog.SendProcState(st)
}

func GetStates() []stats.IState {
	return statelog.GetStates()
}

func Enabled() bool {
	return enabled
}

func RunCollector(dbPaths []string) {
	Init("", "", dbPaths, false)
}

func Init(id string, logfilepath string, dbPaths []string, isLeader bool) {
	initOnce.Do(func() {
		enabled = true
		if enabled {
			statelog.Init(id, logfilepath, isLeader, &statelog, []stats.IState{})
			theDbPaths = dbPaths
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

// http://stockcharts.com/school/doku.php?id=chart_school:technical_indicators:moving_averages

// collect stats
func (l *StateLog) ProcessStateChange(stat stats.ProcStat) {
	atomic.AddUint64(&statsNumRequests, 1)
	if addr, ok := statsMapNumRequestTypes[stat.Opcode]; ok {
		atomic.AddUint64(addr, 1)
	}

	prevEMA := int32(atomic.LoadUint32(&statsReqProcEMA))
	curEMA := (int32(stat.ProcTime)-prevEMA)*2.0/(int32(emaWindowSize)+1) + prevEMA
	atomic.StoreUint32(&statsReqProcEMA, uint32(curEMA))
}

func getCompSecCount() (uint32, uint64, uint64) {

	w := new(bytes.Buffer)
	db.GetDB().WriteProperty("cfstats-no-file-histogram", w)

	scanner := bufio.NewScanner(w)
	scanner.Split(bufio.ScanLines)

	var line string
	var compSec, compCount uint64
	var maxLevel uint32 = 0

	for scanner.Scan() {
		line = scanner.Text()
		if maxLevel == 0 && strings.HasPrefix(line, "  L1 ") {
			maxLevel = 1
		} else if maxLevel < 2 && strings.HasPrefix(line, "  L2 ") {
			maxLevel = 2
		} else if maxLevel < 3 && strings.HasPrefix(line, "  L3 ") {
			maxLevel = 3
		} else if maxLevel < 4 && strings.HasPrefix(line, "  L4 ") {
			maxLevel = 4
		} else if maxLevel < 5 && strings.HasPrefix(line, "  L5 ") {
			maxLevel = 5
		} else if maxLevel < 6 && strings.HasPrefix(line, "  L6 ") {
			maxLevel = 6
		}

		if !strings.HasPrefix(line, " Sum ") {
			continue
		}

		list := strings.Fields(line)
		if len(list) < 16 {
			continue
		}

		// list[14] and list[15] are Comp(sec) and Comp(cnt)
		val, _ := strconv.ParseInt(list[14], 10, 64)
		compSec += uint64(val)
		val, _ = strconv.ParseInt(list[15], 10, 64)
		compCount += uint64(val)
	}

	return maxLevel, compSec, compCount
}

// write to the state log file
func (l *StateLog) ProcessWrite(cnt int) {
	n := atomic.LoadUint64(&statsNumRequests)
	nPrev := atomic.LoadUint64(&statsNumRequestsPrev)
	var delta uint32
	if n > nPrev {
		delta = uint32(n - nPrev)
	}
	if stats.KWriteIntervalSecond != 1 {
		delta = uint32(float32(delta) / (float32(stats.KWriteIntervalSecond)))
	}
	atomic.StoreUint32(&statsRequestsPerSecond, delta)
	atomic.StoreUint64(&statsNumRequestsPrev, n)

	n = db.GetDB().GetIntProperty("estimate-num-keys")
	atomic.StoreUint64(&statsNumKeys, n)

	maxLevel, compSec, compCount := getCompSecCount()
	atomic.StoreUint32(&statsMaxDBLevel, maxLevel)

	n = db.GetDB().GetIntProperty("estimate-pending-compaction-bytes")
	atomic.StoreUint64(&statsPendingCompKBytes, n/1000)

	n = db.GetDB().GetIntProperty("actual-delayed-write-rate")
	atomic.StoreUint64(&statsDelayedWriteRate, n)

	if statsCompSecPrev == 0 || compSec < statsCompSecPrev {
		statsCompSecPrev = compSec
	}

	delta = uint32(compSec - statsCompSecPrev)
	atomic.StoreUint32(&statsCompSecByInterval, delta)

	if statsCompCountPrev == 0 || compCount < statsCompCountPrev {
		statsCompCountPrev = compCount
	}

	delta = uint32(compCount - statsCompCountPrev)
	atomic.StoreUint32(&statsCompCountByInterval, delta)

	// Start a new interval for stats
	if cnt%60 == 0 {
		statsCompSecPrev = compSec
		statsCompCountPrev = compCount
	}

	// recalcuate storage usage every 5 seconds
	if cnt%5 == 0 {
		status := SumDiskUsage(theDbPaths)
		statsFreeStorageSpace = status.Avail / MB
		statsUsedStorageSpace = status.Used / MB
	}

	// Get process's cpu usage every 10 seconds
	if cnt%10 == 0 {
		statsProcCpuUsage = ProcCpuUsage()
	}

	// Get machine's cpu usage every 10 seconds
	if cnt%10 == 0 {
		statsMachCpuUsage = MachCpuUsage()
	}
}
