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
	"fmt"

	"juno/third_party/forked/golang/glog"
)

func DisplayZones(zones []*Zone, header string) {
	fmt.Printf("%s\n%s\n", header,
		" zoneid\tnodeid\tshard_count\tratio [primary][backup]")
	for i := 0; i < len(zones); i++ {
		if zones[i] == nil {
			continue
		}
		zones[i].Display()
	}
}

// Validate shardids are unique and consecutive.
func ValidateZones(zones []*Zone) bool {
	ok := true
	dupCount := 0
	numZones := uint32(len(zones))

	for zoneid := uint32(0); zoneid < numZones; zoneid++ {
		totalShards := 0

		if zones[zoneid] == nil {
			glog.Errorf("[ERROR] Shardmap does not exist for zone %d.\n", zoneid)
			return false
		}

		nodeList := zones[zoneid].Nodes
		numNodes := len(nodeList)

		if numNodes == 0 {
			glog.Errorf("[ERROR] NumNodes is zero in zone %d.\n", zoneid)
			return false
		}

		if int(zones[zoneid].NumNodes) != numNodes {
			glog.Errorf("[ERROR] Number of nodes (%d, %d) mismatch\n.",
				zones[zoneid].NumNodes, numNodes)
			return false
		}

		for nodeid := 0; nodeid < numNodes; nodeid++ {
			totalShards += nodeList[nodeid].totalLength()
		}

		unique := make([]uint32, totalShards)
		for nodeid := 0; nodeid < numNodes; nodeid++ {

			for _, shardid := range nodeList[nodeid].PrimaryShards {
				if !IsPrimary(shardid, zoneid, numZones) {
					glog.Errorf("[ERROR] zoneid=%d nodeid=%d shardid=%d is not a primary shard.\n",
						zoneid, nodeid, shardid)
					ok = false
				}
				if shardid >= uint32(totalShards) {
					glog.Errorf("[ERROR] zoneid=%d nodeid=%d shardid=%d is out of range.\n",
						zoneid, nodeid, shardid)
					ok = false
					continue
				}
				if unique[shardid] != 0 {
					glog.Errorf("[ERROR] zoneid=%d nodeid=%d shardid=%d is a duplicate.\n",
						zoneid, nodeid, shardid)
					dupCount++
					ok = false
				}
				unique[shardid] = 1
			}
			for _, shardid := range nodeList[nodeid].SecondaryShards {
				if IsPrimary(shardid, zoneid, numZones) {
					glog.Errorf("[ERROR] zoneid=%d nodeid=%d shardid=%d is not a secondary shard.\n",
						zoneid, nodeid, shardid)
					ok = false
				}
				if shardid >= uint32(totalShards) {
					glog.Errorf("[ERROR] zoneid=%d nodeid=%d shardid=%d is out of range.\n",
						zoneid, nodeid, shardid)
					ok = false
					continue
				}
				if unique[shardid] != 0 {
					glog.Errorf("[ERROR] zoneid=%d nodeid=%d shardid=%d is a duplicate.\n",
						zoneid, nodeid, shardid)
					dupCount++
					ok = false
				}
				unique[shardid] = 1
			}
		}
	}
	return ok
}
