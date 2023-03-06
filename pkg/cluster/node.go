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
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"juno/third_party/forked/golang/glog"
)

// Node class represent a logic node
type Node struct {
	Zoneid          uint32
	Nodeid          uint32
	PrimaryShards   []uint32
	SecondaryShards []uint32
}

func NewNode(zoneid uint32, nodeid uint32) *Node {
	node := &Node{
		Zoneid: zoneid,
		Nodeid: nodeid,
	}
	return node
}

func (n *Node) InitNode(zoneid uint32, nodeid uint32) {
	n.Zoneid = zoneid
	n.Nodeid = nodeid
}

// First node only
func (n *Node) initShards(zoneid uint32, numZones uint32, numShards uint32) {

	var primary []uint32 = make([]uint32, 0, numShards)
	var secondary []uint32 = make([]uint32, 0, numShards)

	for k := uint32(0); k < numShards; k++ {

		if IsPrimary(k, zoneid, numZones) {
			primary = append(primary, k)
		} else {
			secondary = append(secondary, k)
		}
	}

	// intializing with all shards assigned to the first node in the zone
	n.allocate(len(primary), len(secondary))
	n.fillPrimary(0, len(primary), primary)
	n.fillSecondary(0, len(secondary), secondary)
}

func (n *Node) GetShards() (shards []uint32) {
	shards = make([]uint32, len(n.PrimaryShards)+len(n.SecondaryShards))
	copy(shards, n.PrimaryShards)
	copy(shards[len(n.PrimaryShards):], n.SecondaryShards)
	return
}

func (n *Node) NodeToString(priSecDelimiter string, shardDelimiter string) string {

	var shards_str []string = make([]string, 2)

	var list []string = make([]string, 0, len(n.PrimaryShards))
	for _, shardid := range n.PrimaryShards {
		list = append(list, strconv.Itoa(int(shardid)))
	}
	shards_str[0] = strings.Join(list, shardDelimiter)

	list = make([]string, 0, len(n.SecondaryShards))
	for _, shardid := range n.SecondaryShards {
		list = append(list, strconv.Itoa(int(shardid)))
	}
	shards_str[1] = strings.Join(list, shardDelimiter)

	return strings.Join(shards_str, priSecDelimiter)
}

func (n *Node) StringToNode(zoneid uint32, nodeid uint32, val string,
	priSecDelimiter string, shardDelimiter string) error {

	// get shards from value
	shard_tokens := strings.Split(val, priSecDelimiter)
	if len(shard_tokens) < 2 {
		err := fmt.Sprintf("etcd: bad format in %s", val)
		glog.Errorf("[ERROR]: %s\n", err)
		return errors.New(err)
	}

	priShards := strings.Split(shard_tokens[0], shardDelimiter)
	secShards := strings.Split(shard_tokens[1], shardDelimiter)

	priLen := len(priShards)
	secLen := len(secShards)
	if shard_tokens[0] == "" {
		priLen = 0
	}
	if shard_tokens[1] == "" {
		secLen = 0
	}

	n.InitNode(zoneid, nodeid)
	n.PrimaryShards = make([]uint32, priLen)
	n.SecondaryShards = make([]uint32, secLen)

	for k := 0; k < priLen; k++ {
		shard, _ := strconv.Atoi(priShards[k])
		n.PrimaryShards[k] = uint32(shard)
	}

	for k := 0; k < secLen; k++ {
		shard, _ := strconv.Atoi(secShards[k])
		n.SecondaryShards[k] = uint32(shard)
	}

	return nil
}

func (n *Node) allocate(primaryCount int, secondaryCount int) {
	n.PrimaryShards = make([]uint32, 0, primaryCount)
	n.SecondaryShards = make([]uint32, 0, secondaryCount)
}

func (n *Node) fillPrimary(start int, count int, shards []uint32) {
	n.PrimaryShards = append(n.PrimaryShards, shards[start:start+count]...)
}

func (n *Node) fillSecondary(start int, count int, shards []uint32) {
	n.SecondaryShards = append(n.SecondaryShards, shards[start:start+count]...)
}

func (n *Node) totalLength() int {
	return len(n.PrimaryShards) + len(n.SecondaryShards)
}

func (n *Node) primaryLength() int {
	return len(n.PrimaryShards)
}

func (n *Node) secondaryLength() int {
	return len(n.SecondaryShards)
}

func (n *Node) ratio() float32 {
	if n.totalLength() == 0 {
		return 0
	}
	return float32(n.primaryLength()) / float32(n.totalLength())
}

// remove i-th entry from primary
// Return the shardid
func (n *Node) deleteFromPrimary(filter *Filter, target Node) uint32 {
	var i int

	if filter == nil {
		i = 0
	} else {
		i = filter.selectShardForMove(n.PrimaryShards, target)
	}

	last := n.primaryLength() - 1
	if IsNewMappingAlg() {
		i = last
	}
	shardid := n.PrimaryShards[i]

	n.PrimaryShards[i] = n.PrimaryShards[last]
	n.PrimaryShards = n.PrimaryShards[:last]

	return shardid
}

// add one to tail
func (n *Node) appendToPrimary(shardid uint32) {
	n.PrimaryShards = append(n.PrimaryShards, shardid)
}

// remove i-th entry from secondary
// Return the shardid
func (n *Node) deleteFromSecondary(filter *Filter, target Node) uint32 {
	var i int

	if filter == nil {
		i = 0
	} else {
		i = filter.selectShardForMove(n.SecondaryShards, target)
	}
	shardid := n.SecondaryShards[i]
	last := n.secondaryLength() - 1

	n.SecondaryShards[i] = n.SecondaryShards[last]
	n.SecondaryShards = n.SecondaryShards[:last]

	return shardid
}

// add one to tail
func (n *Node) appendToSecondary(shardid uint32) {
	n.SecondaryShards = append(n.SecondaryShards, shardid)
}

func (n *Node) copy(other Node) {
	n.allocate(other.primaryLength(), other.secondaryLength())
	n.Zoneid = other.Zoneid
	n.Nodeid = other.Nodeid
	n.PrimaryShards = append(n.PrimaryShards, other.PrimaryShards...)
	n.SecondaryShards = append(n.SecondaryShards, other.SecondaryShards...)
}

func (n *Node) copyAndSort(other Node) {
	n.copy(other)
	sort.Sort(byValue(n.PrimaryShards))
	sort.Sort(byValue(n.SecondaryShards))
}

func (n *Node) Log() {
	glog.Verbosef("zoneid=%d, nodeid=%d, prim_shards=%#v, second_shards=%#v",
		n.Zoneid, n.Nodeid, n.PrimaryShards, n.SecondaryShards)
}

func (n *Node) Print(short bool) {
	var tmp Node
	tmp.copyAndSort(*n)

	if short {
		fmt.Printf("%3d\t%4d\t%2d=(%3d %3d)\t%.2f\n", n.Zoneid, n.Nodeid,
			tmp.totalLength(), tmp.primaryLength(),
			tmp.secondaryLength(), tmp.ratio())
	} else {
		fmt.Printf("%3d\t%4d\t%2d=(%3d %3d)\t%.2f  {%v %v}\n", n.Zoneid, n.Nodeid,
			tmp.totalLength(), tmp.primaryLength(),
			tmp.secondaryLength(), tmp.ratio(), tmp.PrimaryShards, tmp.SecondaryShards)
	}
}

// helper
type byValue []uint32

func (a byValue) Len() int {
	return len(a)
}
func (a byValue) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}

// Increasing order
func (a byValue) Less(i, j int) bool {
	return a[i] < a[j]
}
