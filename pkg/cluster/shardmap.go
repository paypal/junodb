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
package cluster

import (
	"errors"
	"fmt"
	"time"
)

const (
	MaxShards = 1024
	MaxZone   = 5
)

type ShardMapEntry struct {
	nodeid    uint32
	isPrimary bool
}

type ShardMap struct {
	cluster *Cluster
	shards  [MaxShards][MaxZone]ShardMapEntry
}

func NewShardMap(c *Cluster) *ShardMap {
	m := &ShardMap{
		cluster: c,
	}

	m.populate()
	return m
}

func (m *ShardMap) Populate(c *Cluster) {
	m.cluster = c
	m.populate()
}

func (m *ShardMap) populate() {
	for zoneid := uint32(0); zoneid < m.cluster.NumZones; zoneid++ {
		for nodeid := uint32(0); nodeid < m.cluster.Zones[zoneid].NumNodes; nodeid++ {
			node := m.cluster.Zones[zoneid].Nodes[nodeid]
			for _, shardid := range node.PrimaryShards {
				m.shards[shardid][zoneid].nodeid = nodeid
				m.shards[shardid][zoneid].isPrimary = true
			}

			for _, shardid := range node.SecondaryShards {
				m.shards[shardid][zoneid].nodeid = nodeid
				m.shards[shardid][zoneid].isPrimary = false
			}
		}
	}
}

func (m *ShardMap) GetNodes(id uint32, start_zoneid uint32) ([]uint32, []uint32, error) {

	zones := make([]uint32, m.cluster.NumZones)
	nodes := make([]uint32, m.cluster.NumZones)

	k := 0

	// find primary (order perserved)
	for i := uint32(0); i < m.cluster.NumZones; i++ {
		zoneid := (start_zoneid + i) % m.cluster.NumZones
		if m.shards[id][zoneid].isPrimary {
			zones[k] = uint32(zoneid)
			nodes[k] = m.shards[id][zoneid].nodeid
			k++
		}
	}

	// find secondary (order perserved)
	for i := uint32(0); i < m.cluster.NumZones; i++ {
		zoneid := (start_zoneid + i) % m.cluster.NumZones
		if !m.shards[id][zoneid].isPrimary {
			zones[k] = uint32(zoneid)
			nodes[k] = m.shards[id][zoneid].nodeid
			k++
		}
	}
	return zones, nodes, nil
}

func (m *ShardMap) GetNodeId(shardid uint32, zoneid uint32) (nodeid uint32, err error) {
	if shardid >= m.cluster.NumShards || zoneid >= m.cluster.NumZones {
		err = errors.New("shardid or zoneid out of range")
	}

	nodeid = m.shards[shardid][zoneid].nodeid
	return
}

///FIXME this function needs to be change because of the shard Map change
func (m *ShardMap) LogConnectivity(connState [][]int, id int32, log bool) bool {

	numZones := m.cluster.NumZones
	numWrite := (numZones + 1) / 2

	now := time.Now()

	prefix := fmt.Sprintf("%s  %2d ",
		now.Format("01-02 15:04:05"), id)

	var ok bool = true

	for i := uint32(0); i < numZones; i++ {
		var ok_cnt uint32 = 0
		var line string = prefix
		line = fmt.Sprintf("%s %4d ", line, i)
		for zoneid := uint32(0); zoneid < numZones; zoneid++ {
			nodeid := int(m.shards[i][zoneid].nodeid)
			ok_cnt += uint32(connState[zoneid][nodeid])
			if !log {
				continue
			}
			line = fmt.Sprintf("%s %3d[%d]", line, nodeid, connState[zoneid][nodeid])
		}

		if ok_cnt < numWrite {
			// todo CAL Logging?
			ok = false
		}

		if log {
			if ok_cnt < numWrite {
				line = fmt.Sprintf("%s  xx", line)
			} else {
				line = fmt.Sprintf("%s  ok", line)
			}
			line = fmt.Sprintf("%s\n", line)
		}
	}

	return ok
}

func (m *ShardMap) Dump() {

	fmt.Printf("\n zone\t")
	for zoneid := uint32(0); zoneid < m.cluster.NumZones; zoneid++ {
		fmt.Printf("\t%d", zoneid)
	}
	fmt.Printf("\n----------------------------------------------------")

	for shardid := uint32(0); shardid < m.cluster.NumShards; shardid++ {
		fmt.Printf("\n shard %d: \t", shardid)
		for zoneid := uint32(0); zoneid < m.cluster.NumZones; zoneid++ {
			fmt.Printf("%d", m.shards[shardid][zoneid].nodeid)
			if m.shards[shardid][zoneid].isPrimary {
				fmt.Printf("*")
			}
			fmt.Printf("\t")
		}
	}
	fmt.Printf("\n")
}
