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
	"sort"

	"juno/third_party/forked/golang/glog"
)

// Node class represent a logic node
type Zone struct {
	Zoneid   uint32
	NumNodes uint32
	Nodes    []Node
}

var newMappingAlg = false

func IsNewMappingAlg() bool {
	return newMappingAlg
}

func SetMappingAlg(algVersion uint32) {
	glog.Infof("algver=%d", algVersion)
	if algVersion < 2 {
		newMappingAlg = false
		return
	}

	newMappingAlg = true
}

func NewZoneFromConfig(zoneid uint32, numNodes uint32, numZones uint32, numShards uint32) *Zone {
	zone := Zone{
		Zoneid:   zoneid,
		NumNodes: numNodes,
		Nodes:    make([]Node, 1, numNodes),
	}

	// Populate Nodes
	zone.initShardsAsssignment(numZones, numShards)
	return &zone
}

func IsPrimary(shardid uint32, zoneid uint32, numZones uint32) bool {

	if IsNewMappingAlg() {
		return true
	}
	m := shardid % numZones

	if m >= zoneid && m < zoneid+(numZones-1)/2 {
		return false
	}

	if m+numZones < zoneid+(numZones-1)/2 {
		return false
	}

	return true
}

func (z *Zone) initShardsAsssignment(numZones uint32, numShards uint32) {

	z.Nodes[0].InitNode(z.Zoneid, 0)
	z.Nodes[0].initShards(z.Zoneid, numZones, numShards)

	for k := uint32(1); k < z.NumNodes; k++ {
		z.addOneNode(nil)
	}
}

func NewZones(numZones uint32, numShards uint32, cutoff []int) (zones []*Zone) {

	if !IsNewMappingAlg() {
		glog.Exitf("Wrong call: only allowed for new mapping algorithm.")
	}

	zones = make([]*Zone, numZones)

	// Add shard ids mapped to each node.
	for i := 0; i < int(numZones); i++ {

		numNodes := cutoff[i]
		zones[i] = NewZoneFromConfig(uint32(i), uint32(numNodes), numZones, numShards)
	}
	return zones
}

// zones: expected
// curr:  current cluster info in etcd.
func MatchZones(zones []*Zone, curr []*Zone) bool {

	numZones := len(zones)
	if numZones != len(curr) {
		return false
	}

	for i := 0; i < numZones; i++ {

		nodeList := zones[i].Nodes
		numNodes := len(nodeList)
		currNodeList := curr[i].Nodes

		if numNodes != len(currNodeList) {
			glog.Errorf("[ERROR] Number of nodes: (curr=%d, expected=%d) mismatch in zone %d.",
				len(currNodeList), numNodes, i)
			return false
		}

		for j := 0; j < numNodes; j++ {

			msg := fmt.Sprintf("[ERROR] Inconsistent shardmap at zone %d node %d", i, j)
			if len(nodeList[j].PrimaryShards) != len(currNodeList[j].PrimaryShards) ||
				len(nodeList[j].SecondaryShards) != len(currNodeList[j].SecondaryShards) {
				glog.Error(msg)
				return false
			}

			for k := range nodeList[j].PrimaryShards {
				if nodeList[j].PrimaryShards[k] != currNodeList[j].PrimaryShards[k] {
					glog.Error(msg)
					return false
				}
			}

			for k := range nodeList[j].SecondaryShards {
				if nodeList[j].SecondaryShards[k] != currNodeList[j].SecondaryShards[k] {
					glog.Error(msg)
					return false
				}
			}
		}
	}

	return true
}

func (z *Zone) addOneNode(filter *Filter) (err error) {
	var target Node
	target.InitNode(z.Zoneid, uint32(len(z.Nodes)))

	var curr int = 0
	var numNodes = len(z.Nodes)
	var reorder []indexElem = make([]indexElem, numNodes)
	for k := 0; k < numNodes; k++ {
		reorder[k].nodeid = k
		reorder[k].weight = z.Nodes[k].primaryLength()
	}

	if !IsNewMappingAlg() {
		sort.Sort(byWeight(reorder))
	}

	for {
		nodeid := reorder[curr].nodeid

		if IsNewMappingAlg() {
			max := 0
			nodeid = 0
			for j := 0; j < numNodes; j++ {
				if z.Nodes[j].primaryLength() >= max {
					max = z.Nodes[j].primaryLength()
					nodeid = j
				}
			}
		}

		source := z.Nodes[nodeid]

		// Check exit condition
		if target.primaryLength() >= source.primaryLength()-1 {
			break
		}

		shardid := z.Nodes[nodeid].deleteFromPrimary(filter, target)
		target.appendToPrimary(shardid)

		if IsNewMappingAlg() {
			continue
		}

		next := curr + 1

		if next >= numNodes {
			curr = 0
			continue
		}

		// Rewind if the current node has more shards than next.
		nextNode := reorder[next].nodeid
		if z.Nodes[nodeid].primaryLength() >= z.Nodes[nextNode].primaryLength() {
			curr = 0 // rewind
		} else { // move to the next
			curr = next
		}
	}

	if IsNewMappingAlg() {
		sort.Sort(byValue(target.PrimaryShards))
		z.Nodes = append(z.Nodes, target)

		return nil
	}

	// TODO reuse code
	for k := 0; k < numNodes; k++ {
		reorder[k].nodeid = k
		reorder[k].weight = z.Nodes[k].secondaryLength()
	}

	sort.Sort(byWeight(reorder))

	curr = 0
	for {
		nodeid := reorder[curr].nodeid
		source := z.Nodes[nodeid]

		// Check exit condition
		if target.secondaryLength() >= source.secondaryLength()-1 {
			break
		}

		shardid := z.Nodes[nodeid].deleteFromSecondary(filter, target)
		target.appendToSecondary(shardid)

		next := curr + 1
		if next >= numNodes {
			curr = 0
			continue
		}

		// Rewind if the current node has more shards than next.
		nextNode := reorder[next].nodeid
		if z.Nodes[nodeid].secondaryLength() >= z.Nodes[nextNode].secondaryLength() {
			curr = 0 // rewind
		} else { // move to the next
			curr = next
		}
	}

	z.Nodes = append(z.Nodes, target)
	return nil
}

func (z *Zone) removeOneNode() {

	var curr int = 0
	var last = len(z.Nodes) - 1
	if last <= 0 {
		return // done
	}

	var reorder []indexElem = make([]indexElem, last)
	for k := 0; k < last; k++ {
		reorder[k].nodeid = k
		reorder[k].weight = -z.Nodes[k].primaryLength()
	}
	sort.Sort(byWeight(reorder))

	for {
		// Target nodeid
		nodeid := reorder[curr].nodeid

		// Check exit condition
		if z.Nodes[last].primaryLength() == 0 {
			break
		}

		shardid := z.Nodes[last].deleteFromPrimary(nil, z.Nodes[nodeid])
		z.Nodes[nodeid].appendToPrimary(shardid)

		next := curr + 1
		if next >= last {
			curr = 0
			continue
		}

		// Rewind if the current node has fewer shards than next.
		nextNode := reorder[next].nodeid
		if z.Nodes[nodeid].primaryLength() <= z.Nodes[nextNode].primaryLength() {
			curr = 0 // rewind
		} else { // move to the next
			curr = next
		}
	}

	for k := 0; k < last; k++ {
		reorder[k].nodeid = k
		reorder[k].weight = -z.Nodes[k].secondaryLength()
	}

	sort.Sort(byWeight(reorder))

	curr = 0
	for {
		// Target nodeid
		nodeid := reorder[curr].nodeid

		// Check exit condition
		if z.Nodes[last].secondaryLength() == 0 {
			break
		}

		shardid := z.Nodes[last].deleteFromSecondary(nil, z.Nodes[nodeid])
		z.Nodes[nodeid].appendToSecondary(shardid)

		next := curr + 1
		if next >= last {
			curr = 0
			continue
		}

		// Rewind if the current node has fewer shards than next.
		nextNode := reorder[next].nodeid
		if z.Nodes[nodeid].secondaryLength() <= z.Nodes[nextNode].secondaryLength() {
			curr = 0 // rewind
		} else { // move to the next
			curr = next
		}
	}

	// Remove last node
	z.Nodes = z.Nodes[:last]
}

func (z *Zone) Log() {
	glog.Verbosef("zoneid=%d, numNodes=%d", z.Zoneid, z.NumNodes)
	for i := uint32(0); i < z.NumNodes; i++ {
		z.Nodes[i].Log()
	}
}

func (z *Zone) Print() {
	fmt.Printf("-------------------\n")
	fmt.Printf("zoneid=%d, numNode=%d\n", z.Zoneid, z.NumNodes)
	for i := uint32(0); i < z.NumNodes; i++ {
		z.Nodes[i].Print(false)
	}
}

func (z *Zone) Display() {
	//fmt.Printf(" zoneid\tnodeid\tshard_count\tratio [primary][backup]\n")
	for i := uint32(0); i < z.NumNodes; i++ {
		z.Nodes[i].Print(false)
	}
}

// helper
type indexElem struct {
	nodeid int
	weight int
}

type byWeight []indexElem

func (list byWeight) Len() int {
	return len(list)
}

func (list byWeight) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}

// Decreasing order by weight
func (list byWeight) Less(i, j int) bool {
	return list[i].weight > list[j].weight
}
