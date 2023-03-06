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
  
package app

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/dbscanserv/config"
	"juno/cmd/dbscanserv/prime"
	"juno/pkg/net/netutil"
)

type Remote int

type Request struct {
	Zoneid  int
	Shardid int
	Rangeid int
	prime.KeyList
}

type CmdRequest struct {
	Cmd          int
	ServerZoneid int

	IncExpireKeys bool
	ModTimeBegin  int64
	ProxyAddr     string

	RangeidBitlen int
	StartShardid  int
	StopShardid   int

	Key            []byte
	NamespaceNames string
	Trace          bool
}

type CmdReply struct {
	Cmd          int
	ServerZoneid int

	FirstDbid       int
	LastDbid        int
	NumOpenDbs      int
	ReplicateKeys   int64
	ReplicateErrors int64
	Status          string

	Val    []byte
	ValLen int
	Config config.DbScan
	Error  string
}

type KeyPrefix [][]byte
type NSMap map[string]KeyPrefix

type CmdNamespaceRequest struct {
	Cmd            int
	ServerZoneid   int
	LockServerAddr string

	NamespaceConfig []byte
}

type CmdNamespaceReply struct {
	Cmd          int
	ServerZoneid int
	Locked       bool
	Disabled     bool
	Failed       bool
}

const (
	cmdInit = iota + 1
	cmdRun
	cmdStatus
	cmdStop
	cmdGet
	cmdPatch

	cmdAcquireLock
	cmdRefreshLock
	cmdReleaseLock
	cmdClearNamespace
)

var (
	listenPort     = 62200
	dbWatchEnabled bool

	cmdQueue = make(chan CmdRequest, 5)
	initOnce sync.Once

	// For namespace service
	sessionLockExpireTime int64
	token                 uint64
	mutex                 sync.Mutex
)

func (r *CmdReply) DbRange() string {
	return fmt.Sprintf("firstDb=(%d,%d) lastDb=(%d,%d)", r.FirstDbid>>16,
		r.FirstDbid&0xffff, r.LastDbid>>16, r.LastDbid&0xffff)
}

func SetListenPort(port int, b bool) {
	listenPort = port
	dbWatchEnabled = b
}

func getPeerPort(zoneid int) int {
	return listenPort
}

func IsLocalAddress(ip string, zoneid int) bool {
	return netutil.IsLocalAddress(ip)
}

func GenRemoteAddr(ip string, zoneid int) string {
	return fmt.Sprintf("%s:%d", ip, getPeerPort(zoneid))
}

// Start listener
// Return cmdQueue
func StartListener() chan CmdRequest {

	initOnce.Do(func() {
		// Thread to close DB if idle too long
		prime.CloseIdleDb()

		// Remove left-over event files
		pattern := "events/zone_?"
		files, _ := filepath.Glob(pattern)
		for _, f := range files {
			os.Remove(f)
		}

		// Listener thread
		go func() {

			str := fmt.Sprintf(":%d", listenPort)
			glog.Infof("Listen on %s", str)

			listener, err := net.Listen("tcp", str)
			for i := 0; i < 5; i++ {
				if err == nil {
					break
				}
				glog.Errorf("[WARN] Failed to start listener on %d, %s",
					listenPort, err)
				if i == 4 {
					return
				}
				time.Sleep(2 * time.Second)
				listener, err = net.Listen("tcp", str)
			}

			rpc.Register(new(Remote))
			rpc.Accept(listener) // blocked unless Accept failed.
			listener.Close()
		}()
	})

	return cmdQueue
}

func (r *Remote) GetNext(req Request, reply *prime.MessageBlock) error {

	scanner := GetScanner(req.Zoneid, int(req.Shardid))

	// Out parameter
	*reply = *scanner.GetNext(req.Rangeid, req.KeyList)

	return nil
}

func (r *Remote) Ping(req CmdRequest, reply *CmdReply) error {

	switch req.Cmd {
	case cmdRun, cmdStop, cmdPatch:
		cmdQueue <- req
		reply.Cmd = req.Cmd
		reply.ServerZoneid = req.ServerZoneid

		first, last := prime.GetDbMapRange()
		reply.FirstDbid = first
		reply.LastDbid = last
		reply.NumOpenDbs = prime.GetNumOpenDbs()
		reply.Status = prime.GetScanStatus()

		if req.Cmd == cmdRun {
			prime.SetProxyAddr(req.ProxyAddr)
		}

		if req.Cmd == cmdStop {
			SetCancel()
		}
		prime.LogMsg("Ping response: recv=%v reply=%v", req, *reply)

	case cmdInit:
		prime.ResetReplicateCount()
		if !ResetScanners(req.StartShardid, req.StopShardid) ||
			ShardRangesOverlap(req.StartShardid, req.StopShardid) {
			reply.Error = "Server is not idle."
		} else {
			nsCopy := len(req.NamespaceNames) > 0
			prime.SetBeginEndTimes(req.IncExpireKeys, req.ModTimeBegin, nsCopy)
			prime.SetRangeidBits(req.RangeidBitlen)
			trace := (req.ServerZoneid == 0) && req.Trace
			prime.SetNamespaceNames(req.NamespaceNames, trace)
		}

		reply.Cmd = req.Cmd
		reply.ServerZoneid = req.ServerZoneid

		first, last := prime.GetDbMapRange()
		reply.FirstDbid = first
		reply.LastDbid = last
		reply.NumOpenDbs = prime.GetNumOpenDbs()
		reply.Config = dbscanConfig

		prime.LogMsg("Ping response: recv=%v reply=%v", req, *reply)

	case cmdStatus:
		reply.Cmd = req.Cmd
		reply.ServerZoneid = req.ServerZoneid

		first, last := prime.GetDbMapRange()
		reply.FirstDbid = first
		reply.LastDbid = last
		reply.NumOpenDbs = prime.GetNumOpenDbs()
		reply.ReplicateKeys = prime.GetReplicateCount()
		reply.ReplicateErrors = prime.GetReplicateErrors()

		scanStatus := prime.GetScanStatus()
		errorStatus := prime.GetErrorStatus()
		reply.Status = scanStatus
		if len(scanStatus) == 0 {
			if len(errorStatus) == 0 {
				reply.Status = "ready"
			} else {
				reply.Status = errorStatus
			}
		}
		prime.LogMsg("Ping response: recv=%v zone=%d "+
			" repKeys=%d repErrors=%d status=%s", req,
			reply.ServerZoneid, reply.ReplicateKeys,
			reply.ReplicateErrors, reply.Status)

	case cmdGet:
		reply.Cmd = req.Cmd

		reply.Val, reply.ValLen, reply.Error =
			GetVersionFromDb(req.Key, req.ServerZoneid)
	}
	return nil
}

func (r *Remote) NamespaceService(req CmdNamespaceRequest,
	reply *CmdNamespaceReply) error {

	reply.Cmd = req.Cmd
	reply.ServerZoneid = req.ServerZoneid
	now := time.Now().Unix()

	switch req.Cmd {
	case cmdAcquireLock:
		if !dbWatchEnabled {
			reply.Disabled = true
			return nil
		}
		t := atomic.LoadInt64(&sessionLockExpireTime)
		if t > now {
			reply.Locked = true
			return nil
		}
		atomic.StoreInt64(&sessionLockExpireTime, now+15)
		return nil
	case cmdRefreshLock:
		atomic.StoreInt64(&sessionLockExpireTime, now+15)
		return nil
	case cmdReleaseLock:
		atomic.StoreInt64(&sessionLockExpireTime, 0)
		return nil
	}

	// Starts a session
	glog.Infof("Start a new session for zone %d", req.ServerZoneid)
	glog.Infof("event: %s", string(req.NamespaceConfig))

	// Create an event file.
	event := fmt.Sprintf("events/zone_%d", req.ServerZoneid)
	tmp := fmt.Sprintf("events/tmp")
	err := os.WriteFile(tmp, req.NamespaceConfig, 0644)

	if err != nil {
		glog.Errorf("%s", err)
		reply.Failed = true
		return nil
	}

	id := beginWork(func() {
		err := os.Rename(tmp, event)
		if err != nil {
			glog.Errorf("%s", err)
			reply.Failed = true
		}
	})

	if reply.Failed {
		return nil
	}

	// Wait for finish
	RefreshLock(req.LockServerAddr)
	time.Sleep(2 * time.Second)
	for !isDeleteTaskDone(req.ServerZoneid, reply) {
		RefreshLock(req.LockServerAddr)
		time.Sleep(2 * time.Second)
	}

	// Remove event files
	pattern := fmt.Sprintf("events/zone_%d*", req.ServerZoneid)
	files, _ := filepath.Glob(pattern)

	endWork(id, func() {
		for _, f := range files {
			os.Remove(f)
		}
		CloseConnect()
	})

	return nil
}

func beginWork(f func()) uint64 {
	mutex.Lock()
	defer mutex.Unlock()

	token++
	f()
	return token
}

func endWork(id uint64, f func()) {
	mutex.Lock()
	defer mutex.Unlock()

	if id != token {
		return
	}
	f()
}

func fileExist(name string) bool {
	files, _ := filepath.Glob(name)
	return len(files) > 0
}

func isDeleteTaskDone(zoneid int, reply *CmdNamespaceReply) bool {

	event := fmt.Sprintf("events/zone_%d", zoneid)
	start := fmt.Sprintf("events/zone_%d_start_*", zoneid)
	done := fmt.Sprintf("events/zone_%d_done_*", zoneid)

	if fileExist(start) || (fileExist(event) && !fileExist(done)) {
		return false
	}

	fail := fmt.Sprintf("events/zone_%d_done_*_fail", zoneid)
	if fileExist(fail) {
		reply.Failed = true
	}

	return true
}
