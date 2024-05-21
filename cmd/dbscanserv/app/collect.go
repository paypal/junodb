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
	"math"
	"sync/atomic"
	"time"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/paypal/junodb/cmd/dbscanserv/prime"
	"github.com/paypal/junodb/pkg/logging"
)

type Collector struct {
	numZones     int
	numShards    int
	shardScanned int

	totalKeys   int
	failKeys    int
	okRepairs   int
	failRepairs int
	numPauses   int
	ch          chan prime.Result
}

var (
	cancelFlag   int32
	beginShardid int32
	endShardid   int32
)

func SetCancel() {
	atomic.StoreInt32(&cancelFlag, 1)
}

func IsCancel() bool {
	return atomic.LoadInt32(&cancelFlag) > 0
}

func setShardRange(startid, stopid int) {
	atomic.StoreInt32(&beginShardid, int32(startid))
	atomic.StoreInt32(&endShardid, int32(stopid))
}

// Overlap with the shard range being scanned.
func ShardRangesOverlap(startid, stopid int) bool {
	a := atomic.LoadInt32(&beginShardid)
	b := atomic.LoadInt32(&endShardid)

	return math.Max(float64(a), float64(startid)) <
		math.Min(float64(b), float64(stopid))
}

func Collect(file string, startShardid int, stopShardid int,
	skip int, driver bool) {

	clusterMap := InitScanners(file, false)
	cmdQueue := StartListener()
	if !driver {
		logging.LogWorkerStart(-2)
		defer logging.LogWorkerExit(-2)
		time.Sleep(4 * time.Second)
		prime.CloseDb()
	}

	for {
		setShardRange(0, 0)
		prime.TruncateLog(true /*checkSize*/) // Truncate if too big
		if driver {
			time.Sleep(1 * time.Second)
			PingServers(cmdInit, startShardid, stopShardid)
			if stopShardid == 0 {
				return
			}
		} else {
			req := <-cmdQueue
			switch req.Cmd {
			case cmdRun:
				time.Sleep(1 * time.Second)
				startShardid = req.StartShardid
				stopShardid = req.StopShardid

				prime.LogMsg("%s Run: startid=%d stopid=%d",
					fmt.Sprintf("%s", time.Now())[0:19],
					startShardid, stopShardid)
				setShardRange(startShardid, stopShardid)
				prime.ResetDisplayCount()

			case cmdStop:
				glog.Infof("stopping dbscanserv ...")
				CloseConnect()
				prime.CloseProxyConnect()
				time.Sleep(4 * time.Second)
				prime.CloseDb()
				ClosePatchDb()
				return
			case cmdPatch:
				DoPatch("")
				continue
			}
		}

		numZones, numShards, shardsByNode :=
			clusterMap.GetLocalShardList(startShardid, stopShardid, skip)
		collector := Collector{
			numZones:  numZones,
			numShards: numShards,
			ch:        make(chan prime.Result, len(shardsByNode)),
		}
		seq := true

		start := time.Now()
		for _, shards := range shardsByNode {

			if seq {
				if !collector.collectByNode(shards, start) {
					glog.Infof("Scan cancelled.")
					prime.LogMsg("Scan cancelled.")
					close(collector.ch)
					break
				}
			} else {
				go collector.collectByNode(shards, start)
			}
		}

		var report prime.Report
		for i := 0; i < len(shardsByNode); i++ {
			result, valid := <-collector.ch
			if !valid {
				break
			}
			report.AddResult(i, result)
		}
		report.Summary(driver)

		CloseConnect()
		prime.CloseProxyConnect()
		prime.RunGC()

		if driver {
			return
		}
	}
}

func (c *Collector) collectByNode(shards []uint32, start time.Time) bool {

	var result prime.Result
	defer func() {

		c.ch <- result
	}()

	for _, shardid := range shards {

		s := int(shardid)
		var currShard prime.Report
		c.shardScanned++

		skipZones := 0
		var rs prime.Result
		rs.Init(c.numZones)

		for i := 0; i < prime.GetRangeCount()+1; i++ { // all ranges

			jm := prime.NewJoinMap()

			if skipZones > 0 {
				jm = nil
			}

			for k := 0; k < c.numZones; k++ { // all zones

				if IsCancel() {
					return false
				}

				if (skipZones & (1 << k)) != 0 {
					continue
				}

				if err := GetScanner(k, s).ScanAndMerge(i, jm, &rs); err != nil {
					skipZones |= (1 << k)
					continue
				} // all zones
			}
			rs.Init(c.numZones)

			if skipZones > 0 || jm.IsEmpty() {
				continue
			}

			jm.Filter(c.numZones, i, &rs)
			result.AddResult(rs)
			currShard.AddCount(rs)

			step := (c.totalKeys + currShard.TotalKeys) / 750000
			if step > c.numPauses { // pause
				time.Sleep(time.Second)
				c.numPauses = step
			}
		} // One shard

		if skipZones > 0 {
			return false
		}

		prime.LogMsg("   shardid=%d keys=%d fails=%d okRepairs=%d failRepairs=%d",
			s, currShard.TotalKeys, currShard.TotalFails,
			currShard.OkRepairs, currShard.FailRepairs)

		c.totalKeys += currShard.TotalKeys
		c.failKeys += currShard.TotalFails
		c.okRepairs += currShard.OkRepairs
		c.failRepairs += currShard.FailRepairs
		elapsed := time.Since(start).Seconds()

		msg := fmt.Sprintf("Collect shardid=%d (%d out of %d) "+
			"totalKeys=%d failKeys=%d okRepairs=%d failRepairs=%d "+
			"t=%s elapsed=%.2fs",

			s, c.shardScanned, c.numShards,
			c.totalKeys, c.failKeys, c.okRepairs, c.failRepairs,
			fmt.Sprintf("%s", time.Now())[0:19], elapsed)

		glog.Infof(msg)
		prime.SetScanStatus(msg)

		if IsCancel() {
			return false
		}
	}

	return true
}
