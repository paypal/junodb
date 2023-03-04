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

type Filter struct {
	shardPos []map[uint32]int
	base     int
}

func NewFilter(numZones int) (filter *Filter) {
	filter = &Filter{
		shardPos: make([]map[uint32]int, numZones),
		base:     0,
	}

	return filter
}

func (f *Filter) InitZone(zone int) {
	f.shardPos[zone] = make(map[uint32]int)
}

func (f *Filter) SetBase(val int) {
	f.base = val
}

func (f *Filter) inRange(nodeid int) bool {
	if f.base > 0 && nodeid >= f.base {
		return true
	}

	return false
}

func (f *Filter) NumZones() int {
	return len(f.shardPos)
}

func (f *Filter) set(zoneid int, shardid uint32, nodeid int) {
	f.shardPos[zoneid][shardid] = nodeid
}

func (f *Filter) Get(zoneid int, shardid uint32) (nodeid int) {
	nodeid = f.shardPos[zoneid][shardid]
	return nodeid
}

func cloneZones(other []*Zone) (workZones []*Zone, filter *Filter) {

	numZones := len(other)
	workZones = make([]*Zone, numZones)
	filter = NewFilter(numZones)

	base := 1000

	for i := 0; i < numZones; i++ {
		numNodes := len(other[i].Nodes)
		workZones[i] = &Zone{
			Zoneid:   uint32(i),
			NumNodes: uint32(numNodes),
			Nodes:    make([]Node, numNodes, 100),
		}

		if numNodes < base {
			base = numNodes
		}

		filter.InitZone(i)

		for j := 0; j < numNodes; j++ {
			workZones[i].Nodes[j].copy(other[i].Nodes[j])

			for _, shardid := range other[i].Nodes[j].PrimaryShards {
				filter.set(i, shardid, j)
			}

			for _, shardid := range other[i].Nodes[j].SecondaryShards {
				filter.set(i, shardid, j)
			}
		}
	}

	filter.SetBase(base)

	return workZones, filter
}

func (filter *Filter) ExpandNodes(workZones []*Zone, cutoff []int, numShards uint32) (zones []*Zone) {

	numZones := len(workZones)
	zones = make([]*Zone, numZones)

	maxNodes := 0
	for i := 0; i < numZones; i++ {
		if cutoff[i]+1 > maxNodes {
			maxNodes = cutoff[i] + 1
		}
		zones[i] = &Zone{
			Zoneid:   uint32(i),
			NumNodes: uint32(cutoff[i] + 1),
			Nodes:    make([]Node, cutoff[i]+1),
		}

		if len(workZones[i].Nodes) > 0 {
			extra := len(workZones[i].Nodes) - cutoff[i] - 1
			if extra == 0 {
				// No expansion is needed
				for k := 0; k <= cutoff[i]; k++ {
					zones[i].Nodes[k].copy(workZones[i].Nodes[k])
				}
			}

			if extra > 0 {
				// Shrink nodes
				for k := 0; k < extra; k++ {
					workZones[i].removeOneNode()
				}
				for k := 0; k <= cutoff[i]; k++ {
					zones[i].Nodes[k].copy(workZones[i].Nodes[k])
				}
			}
			continue
		}

		workZones[i] = &Zone{
			Zoneid:   uint32(i),
			NumNodes: 1,
			Nodes:    make([]Node, 1, 100),
		}

		filter.InitZone(i)
		for k := uint32(0); k < numShards; k++ {
			filter.set(i, k, 0)
		}

		workZones[i].Nodes[0].InitNode(uint32(i), 0)
		workZones[i].Nodes[0].initShards(uint32(i), uint32(numZones), numShards)
		if 0 == cutoff[i] {
			zones[i].Nodes[0].copy(workZones[i].Nodes[0])
		}
	}

	for j := 1; j < maxNodes; j++ {

		for i := 0; i < numZones; i++ {

			if j <= len(workZones[i].Nodes)-1 {
				continue // skip
			}

			workZones[i].addOneNode(filter)

			if j == cutoff[i] {
				for k := 0; k <= j; k++ {
					zones[i].Nodes[k].copy(workZones[i].Nodes[k])
				}
			}
		}
	}

	return
}

// Select a shard to move to target node.
// Return the index in the shards.
func (filter *Filter) selectShardForMove(shards []uint32, target Node) int {
	var ix = 0
	const scale = 1000
	var min = 1000 * scale

	if len(shards) == 1 {
		filter.set(int(target.Zoneid), shards[0], int(target.Nodeid))
		return 0
	}

	numZones := filter.NumZones()
	for i, shardid := range shards {

		var max = -1
		var count = 0

		for j := 0; j < numZones; j++ {

			if j == int(target.Zoneid) { // Skip my zone
				continue
			}

			nodeid := filter.Get(j, shardid)

			if nodeid > max {
				max = nodeid
			}

			// Track count
			if filter.inRange(nodeid) {
				if IsPrimary(shardid, uint32(j), uint32(numZones)) {
					count += 10
				} else {
					count += 9
				}
			}

		}

		score := count*scale + max
		if score >= 0 && score < min {
			min = score
			ix = i
		}
	}

	filter.set(int(target.Zoneid), shards[ix], int(target.Nodeid))

	return ix
}
