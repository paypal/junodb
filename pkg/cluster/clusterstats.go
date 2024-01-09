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

package cluster

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"juno/pkg/logging/otel"
	"juno/pkg/util"
	"juno/third_party/forked/golang/glog"
)

type ProcStat struct {
	zoneid   uint32
	nodeid   uint32
	timeout  bool
	procTime int32 // in Microsecond
}

type NodeStat struct {
	zoneid      uint32
	nodeid      uint32
	timeoutCnt  []uint32
	emaProcTime util.AtomicCounter // in Microsecond
}

type ClusterStats struct {
	numZones        uint32
	maxNodesPerZone uint32
	nodes           []NodeStat

	curWindowId uint32
	quitOnce    sync.Once
	chQuit      chan bool
	chNodeStat  chan *ProcStat
	conf        *StatsConfig

	MarkdownTable      []bool  // 1 writer: stats go routine, M readers: any request proc
	MarkdownExpiration []int64 // only accessed by stats go routine
}

func NewClusterStats(numZones uint32, maxNodesPerZone uint32, conf *StatsConfig) *ClusterStats {
	st := &ClusterStats{
		numZones:        numZones,
		maxNodesPerZone: maxNodesPerZone,
		conf:            conf,
		curWindowId:     conf.TimeoutWindowSize,
		nodes:           make([]NodeStat, numZones*maxNodesPerZone),
		chQuit:          make(chan bool),
		chNodeStat:      make(chan *ProcStat, 5000),
	}

	st.MarkdownTable = make([]bool, numZones*maxNodesPerZone)
	st.MarkdownExpiration = make([]int64, numZones*maxNodesPerZone)
	for i := 0; i < int(numZones*maxNodesPerZone); i++ {
		st.nodes[i].timeoutCnt = make([]uint32, st.conf.TimeoutWindowSize)
	}
	return st
}

func (c *ClusterStats) Run() {
	go c.collect()
}

func (c *ClusterStats) Quit() {
	c.quitOnce.Do(func() {
		close(c.chQuit)
	})
}

// called by processors, in different go routines/threads
func (c *ClusterStats) SendNodeProcState(st *ProcStat) {
	select {
	case c.chNodeStat <- st:
	default:
		// drop if Channel full
	}
}

func (c *ClusterStats) PrintMarkDown() {
	var i uint32
	var j uint32
	fmt.Printf("=== current markdown nodes ===\n")
	for i = 0; i < c.numZones; i++ {
		for j = 0; j < c.maxNodesPerZone; j++ {
			if c.MarkdownTable[i*c.maxNodesPerZone+j] {
				fmt.Printf("%d, %d marked down: %v, %d\n",
					i, j, c.MarkdownTable[i*c.maxNodesPerZone+j], c.MarkdownExpiration[i*c.maxNodesPerZone+j])
			}
		}
	}
	fmt.Printf("==============================\n")
}

func (c *ClusterStats) PrintStats() {
	var i uint32
	var j uint32
	fmt.Printf("=== cluster node stats ===\n")
	for i = 0; i < c.numZones; i++ {
		for j = 0; j < c.maxNodesPerZone; j++ {
			fmt.Printf("%d, %d, %d\n", i, j, c.nodes[i*c.maxNodesPerZone+j].emaProcTime.Get())
		}
	}
	fmt.Printf("==============================\n")
}

func (c *ClusterStats) IsMarkeddown(zoneid uint32, nodeid uint32) bool {
	idx := zoneid*c.maxNodesPerZone + nodeid
	return c.MarkdownTable[idx]
}

// go routine that collects the stats and marks down/up the nodes
func (c *ClusterStats) collect() {
	ticker := time.NewTicker(time.Duration(c.conf.TimeoutWindowUint) * time.Second)
	defer func() {
		ticker.Stop()
	}()

	for {
		select {
		case <-c.chQuit:
			glog.Infof("Cluster Stats collector quit")
			return

		case stat := <-c.chNodeStat:
			c.processStateChange(stat)

		case <-ticker.C:
			c.markup()
		}
	}
}

func (c *ClusterStats) processStateChange(st *ProcStat) {

	// validate zoneid/node id
	if st == nil || st.zoneid > c.numZones-1 || st.nodeid > c.maxNodesPerZone-1 {
		return
	}

	idx := st.zoneid*c.maxNodesPerZone + st.nodeid

	if st.timeout {
		now := time.Now().Unix()

		if c.curWindowId >= c.conf.TimeoutWindowSize {
			// initial case
			curTime := now / int64(c.conf.TimeoutWindowUint)
			c.curWindowId = uint32(int64(curTime) % int64(c.conf.TimeoutWindowSize))
		}
		c.nodes[idx].timeoutCnt[c.curWindowId]++

		var timeoutCnt uint32 = 0
		for i := 0; i < int(c.conf.TimeoutWindowSize); i++ {
			timeoutCnt += c.nodes[idx].timeoutCnt[i]
		}

		// if timeout cnt exceeds the threashold and the node hasn't been marked down yet
		if timeoutCnt >= c.conf.MarkdownThreashold && !c.MarkdownTable[idx] {
			// only one writer, multiple readers, no need to be automic in our use case
			c.MarkdownExpiration[idx] = now + int64(c.conf.MarkdownExpirationBase)
			c.MarkdownTable[idx] = true
			glog.Infof("markdown: node %d-%d, exp:%d, timout ct: %d",
				st.zoneid, st.nodeid, c.MarkdownExpiration[idx], timeoutCnt)
			targetSS := getIP(st.zoneid, st.nodeid)
			otel.RecordCount(otel.SoftMark, []otel.Tags{{otel.Target, targetSS}, {otel.Status, otel.SSMarkDown}})
		}
	}

	// calculate exponential moving average (EMA)
	if st.procTime > 0 {
		// EMA: {close - EMA(previous day)} x multiplier + EMA(previous day).
		// Multipler = 2/(window_size + 1)

		prevEMA := float32(c.nodes[idx].emaProcTime.Get())
		curEMA := ((float32(st.procTime)-prevEMA)*2.0/float32((c.conf.EMARespTimeWindowSize+1)) + prevEMA + 0.5)
		c.nodes[idx].emaProcTime.Set(int32(curEMA))
	}
}

func getIP(zoneid uint32, nodeid uint32) string {
	shardMgr := GetShardMgr()
	// First check if the connInfo has the element.
	if len(shardMgr.connInfo) < int(zoneid+1) || len(shardMgr.connInfo[zoneid]) < int(nodeid+1) {
		return ""
	}
	i := strings.Index(shardMgr.connInfo[zoneid][nodeid], ":")

	if i < 0 {
		return shardMgr.connInfo[zoneid][nodeid]
	}
	return shardMgr.connInfo[zoneid][nodeid][:i]
}

// Naive way of doing markup: markdown expired.
// Potentially can add probe logic so that we markup only if it's in good state.
func (c *ClusterStats) markup() {
	now := time.Now().Unix()
	curTime := now / int64(c.conf.TimeoutWindowUint)
	c.curWindowId = uint32(curTime % int64(c.conf.TimeoutWindowSize))
	lastWindowIdx := (c.curWindowId + 1) % c.conf.TimeoutWindowSize

	for i := uint32(0); i < c.numZones; i++ {
		for j := uint32(0); j < c.maxNodesPerZone; j++ {
			idx := i*c.maxNodesPerZone + j

			// clear the timeout count on the last timeout window slot
			c.nodes[idx].timeoutCnt[lastWindowIdx] = 0

			// markup
			if c.MarkdownTable[idx] && now > c.MarkdownExpiration[idx] {
				// TODO: add probe logic here
				glog.Infof("markup: host %d-%d", i, j)
				c.MarkdownExpiration[idx] = 0
				c.MarkdownTable[idx] = false
				targetSS := getIP(i, j)
				otel.RecordCount(otel.SoftMark, []otel.Tags{{otel.Target, targetSS}, {otel.Status, otel.SSMarkUp}})
			}
		}
	}
}
