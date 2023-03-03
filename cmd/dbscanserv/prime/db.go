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
package prime

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"
	"juno/third_party/forked/tecbot/gorocksdb"

	"juno/cmd/storageserv/storage/db"
)

// Map from zoneid, nodeid to db handle
type DBMap map[int]*gorocksdb.DB

const (
	maxIdleSec = 120
	countLimt  = 86400 * 4 / maxIdleSec
)

var (
	dbMap  = make(DBMap)
	dbRoot string

	lastAccessTime int64
	mutex          sync.Mutex

	scanStatus  = ""
	errorStatus = ""
)

func SetDBRoot(path string) {
	dbRoot = path
}

func GenMapKey(a int, b int) int {
	return int(a<<16) + b
}

func GetDbMapRange() (first, last int) {

	first = GenMapKey(10, 0)
	last = 0

	for k := range dbMap {
		if k < first {
			first = k
		}
		if k > last {
			last = k
		}
	}

	return first, last
}

// One db handle per zoneid, nodeid.
func GetDbHandle(zoneid, nodeid int) *gorocksdb.DB {
	key := GenMapKey(zoneid, nodeid)

	mutex.Lock()
	defer mutex.Unlock()
	atomic.StoreInt64(&lastAccessTime, time.Now().Unix())

	handle := dbMap[key]
	if handle == nil {
		dbpath := getDbPath(dbRoot, zoneid, nodeid)
		handle = NewDbHandle(zoneid, dbpath, true /* readonly */)
		dbMap[key] = handle
	}
	return handle
}

func AddDbHandle(zoneid, nodeid int) bool {
	key := GenMapKey(zoneid, nodeid)

	dbpath := getDbPath(dbRoot, zoneid, nodeid)
	dbMap[key] = NewDbHandle(zoneid, dbpath, true /* readonly */)
	if dbMap[key] == nil {
		return false
	}
	return true
}

func GetNumOpenDbs() (count int) {
	mutex.Lock()
	defer mutex.Unlock()

	count = 0
	for _, handle := range dbMap {
		if handle == nil {
			continue
		}
		count++
	}

	return count
}

func UpdateDbAccessTime() {
	atomic.StoreInt64(&lastAccessTime, time.Now().Unix())
}

func CloseIdleDb() {

	go func() {
		atomic.StoreInt64(&lastAccessTime, time.Now().Unix())
		var count = 0
		for {
			t := atomic.LoadInt64(&lastAccessTime)
			if t > 0 && time.Now().Unix() > (t+maxIdleSec) {
				if CloseDb() {
					RunGC()
				}
			}
			time.Sleep(maxIdleSec * time.Second)

			if t > 0 { // Db in use
				count = 0
				continue
			}

			// Db has been closed
			count++
			if count >= countLimt {
				TruncateLog(false)
				count = 0
			}
		}
	}()
}

func CloseDb() bool {

	mutex.Lock()
	defer mutex.Unlock()

	t := atomic.LoadInt64(&lastAccessTime)
	if t == 0 || time.Now().Unix() < (t+3) {
		// already closed or accessed moment ago
		return false
	}
	atomic.StoreInt64(&lastAccessTime, 0)

	for key, handle := range dbMap {
		if handle == nil {
			continue
		}
		glog.Infof("Close DB zoneid=%d nodeid=%d", key>>16, key&0xffff)
		handle.Close()
		dbMap[key] = nil
	}

	return true
}

func RunGC() {
	var s runtime.MemStats
	count := uint32(0)

	runtime.ReadMemStats(&s)
	glog.Debugf("Before GC: alloc=%d heapInuse=%d heapObjects=%d numGC=%d",
		s.Alloc/1000, s.HeapInuse/1000, s.HeapObjects, s.NumGC)

	for i := 0; i < 3; i++ {
		runtime.GC()

		runtime.ReadMemStats(&s)
		glog.Debugf("alloc=%d heapInuse=%d heapObjects=%d numGC=%d",
			s.Alloc/1000, s.HeapInuse/1000, s.HeapObjects, s.NumGC)

		if count == s.NumGC {
			time.Sleep(time.Second)
		}
		count = s.NumGC
	}

}

func getDbPath(rootPath string, zoneid int, nodeid int) string {
	path := fmt.Sprintf("%s/%d-%d-0.db", rootPath, zoneid, nodeid)
	return path
}

func NewDbHandle(zoneid int, dbpath string, readOnly bool) (handle *gorocksdb.DB) {

	blockOpts := db.ConfigBlockCache()

	opts := db.NewRocksDBptions()
	opts.SetBlockBasedTableFactory(blockOpts)
	if !readOnly {
		var sf db.ShardFilter
		sf.SetCompactionFilter(opts, false)
	}
	opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(3))
	LogMsg("== zoneid=%d dbpath=%s", zoneid, dbpath)

	var err error
	if readOnly {
		handle, err = gorocksdb.OpenDbForReadOnly(opts, dbpath, true)
	} else {
		handle, err = gorocksdb.OpenDb(opts, dbpath)
	}
	if err != nil {
		msg := fmt.Sprintf("dbpath=%s, %s", dbpath, err)
		SetErrorStatus(msg)
		glog.Errorf("[ERROR] %s", msg)
		return nil
	}

	return handle
}

func GetScanStatus() string {
	return scanStatus
}

func SetScanStatus(msg string) {
	scanStatus = msg
}

func GetErrorStatus() string {
	return errorStatus
}

func SetErrorStatus(msg string) {
	errorStatus = fmt.Sprintf("%s; %s", errorStatus, msg)
}
