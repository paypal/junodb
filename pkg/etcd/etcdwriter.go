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

package etcd

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/cluster"
)

type IKVWriter interface {
	PutValue(key string, value string) (err error)
	DeleteKeyWithPrefix(key string, isPrefix bool) (err error)
	PutValuesWithTxn(op OpList) (err error)
}

// Base implemenation for cluster.IWriter
type ClusterWriter struct {
	kvwriter IKVWriter
}

// Write a new cluster info to etcd
func (cw *ClusterWriter) Write(c *cluster.Cluster, version ...uint32) (err error) {

	newver := 1
	if len(version) > 0 && version[0] > 1 {
		newver = int(version[0])
	}

	algver := c.AlgVersion

	var op OpList = make(OpList, 0, c.NumZones*200)

	// Delete key range [beginKey, endKey)
	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {
		if !c.IsRedistZone(zoneid) {
			continue
		}
		beginKey := KeyNodeIpport(zoneid, int(c.Zones[zoneid].NumNodes))
		endKey := KeyNodeIpport(zoneid+1, 0)
		op.AddDeleteWithRange(beginKey, endKey)
	}

	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {
		if !c.IsRedistZone(zoneid) {
			continue
		}
		beginKey := KeyNodeShards(zoneid, int(c.Zones[zoneid].NumNodes))
		endKey := KeyNodeShards(zoneid+1, 0)
		op.AddDeleteWithRange(beginKey, endKey)
	}

	op.AddDeleteWithPrefix(TagRedistPrefix)

	cw.write(c, &op)
	op.AddPut(TagAlgVer, strconv.Itoa(int(algver)))
	op.AddPut(TagVersion, strconv.Itoa(newver))

	// Batch operation
	return cw.kvwriter.PutValuesWithTxn(op)
}

// for redistribution
func (cw *ClusterWriter) WriteRedistInfo(c *cluster.Cluster, nc *cluster.Cluster) (err error) {
	if c.NumZones != nc.NumZones || c.NumShards != nc.NumShards {
		glog.Errorf("[ERROR] cluster number of zones(%d,%d) or shards(%d,%d) do not match",
			c.NumZones, nc.NumZones, c.NumShards, nc.NumShards)
		return errors.New("cluster number of zones or shards do not match")
	}

	// cleanup first
	if err = cw.kvwriter.DeleteKeyWithPrefix(TagRedistPrefix, true); err != nil {
		return err
	}

	c.DumpChangeMap(nc)
	fmt.Println()

	shardmap := nc.CreateShardMap()
	targetNodeMap := make([]map[int]int, c.NumZones)

	// redist
	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {
		if !nc.IsRedistZone(zoneid) {
			continue
		}
		for nodeid := 0; nodeid < int(c.Zones[zoneid].NumNodes); nodeid++ {
			node := c.Zones[zoneid].Nodes[nodeid]
			var changelist []string = make([]string, 0, len(node.PrimaryShards)+len(node.SecondaryShards))

			targetNodeMap[zoneid] = make(map[int]int)
			for _, shardid := range node.PrimaryShards {
				new_nodeid, _ := shardmap.GetNodeId(uint32(shardid), uint32(zoneid))
				if nodeid != int(new_nodeid) {
					change := fmt.Sprintf("%s%s%s", strconv.Itoa(int(shardid)),
						TagCompDelimiter, strconv.Itoa(int(new_nodeid)))
					changelist = append(changelist, change)

					key := KeyRedistNodeState(zoneid, nodeid, int(shardid))
					err = cw.kvwriter.PutValue(key, TagRedistStateBegin)
					if err != nil {
						return err
					}

					targetNodeMap[zoneid][int(new_nodeid)] = 1
				}
			}

			for _, shardid := range node.SecondaryShards {
				new_nodeid, _ := shardmap.GetNodeId(uint32(shardid), uint32(zoneid))
				if nodeid != int(new_nodeid) {
					change := fmt.Sprintf("%s%s%s", strconv.Itoa(int(shardid)),
						TagCompDelimiter, strconv.Itoa(int(new_nodeid)))
					changelist = append(changelist, change)

					key := KeyRedistNodeState(zoneid, nodeid, int(shardid))
					err = cw.kvwriter.PutValue(key, TagRedistStateBegin)
					if err != nil {
						return err
					}

					targetNodeMap[zoneid][int(new_nodeid)] = 1
				}
			}

			if len(changelist) > 0 {
				key := KeyRedistFromNode(int(zoneid), int(nodeid))
				value := strings.Join(changelist, TagRedistShardMoveSeparator)
				err = cw.kvwriter.PutValue(key, value)
				if err != nil {
					return err
				}
			}
		}
	}

	var op OpList = make(OpList, 0, nc.NumZones*200)
	// redist node ip port
	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {
		if !nc.IsRedistZone(zoneid) {
			continue
		}
		for nodeid := len(c.Zones[zoneid].Nodes); nodeid < len(nc.Zones[zoneid].Nodes); nodeid++ {
			key := KeyRedistNodeIpport(zoneid, nodeid)
			op.AddPut(key, nc.ConnInfo[zoneid][nodeid])
		}
	}

	// redist node shards
	for zoneid := 0; zoneid < int(nc.NumZones); zoneid++ {
		if !nc.IsRedistZone(zoneid) {
			continue
		}
		for nodeid := 0; nodeid < len(nc.Zones[zoneid].Nodes); nodeid++ {
			key := KeyRedistNodeShards(zoneid, nodeid)
			op.AddPut(key,
				nc.Zones[zoneid].Nodes[nodeid].NodeToString(TagPrimSecondaryDelimiter,
					TagShardDelimiter))
		}
	}

	// Add redist enable state
	for zoneid := 0; zoneid < int(nc.NumZones); zoneid++ {
		if !nc.IsRedistZone(zoneid) {
			continue
		}
		op.AddPut(KeyRedistEnable(zoneid), TagRedistEnabledReady)
	}

	// Add redist target state
	for zoneid := 0; zoneid < int(nc.NumZones); zoneid++ {

		if !nc.IsRedistZone(zoneid) {
			continue
		}
		start := len(c.Zones[zoneid].Nodes)
		if start == len(nc.Zones[zoneid].Nodes) {
			continue
		} else if start > len(nc.Zones[zoneid].Nodes) {
			start = 0 // shrinking cluster
		}
		for nodeid := start; nodeid < len(nc.Zones[zoneid].Nodes); nodeid++ {
			if start == 0 && targetNodeMap[zoneid][nodeid] == 0 {
				continue // not a target node
			}
			key := KeyRedistTgtNodeState(zoneid, nodeid)
			op.AddPut(key, TagRedistTgtStateInit)
		}
	}

	return cw.kvwriter.PutValuesWithTxn(op)
}

func (cw *ClusterWriter) WriteRedistResume(zoneid int, ratelimit int) (err error) {
	value := TagRedistResume
	if ratelimit > 0 {
		value = fmt.Sprintf("%s%s%s%s%d", TagRedistResumeRL, TagFieldSeparator,
			TagRedistRateLimit, TagKeyValueSeparator, ratelimit)
	}

	key := KeyRedistEnable(zoneid)
	fmt.Printf("key=%s, value=%s\n", key, value)
	return cw.kvwriter.PutValue(key, value)
}

func (cw *ClusterWriter) WriteRedistResumeAll(c *cluster.Cluster, ratelimit int) (err error) {
	value := TagRedistResume
	if ratelimit > 0 {
		value = fmt.Sprintf("%s%s%s%s%d", TagRedistResumeRL, TagFieldSeparator,
			TagRedistRateLimit, TagKeyValueSeparator, ratelimit)
	}

	var op OpList = make(OpList, 0, c.NumZones)

	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {
		op.AddPut(KeyRedistEnable(zoneid), value)
	}

	return cw.kvwriter.PutValuesWithTxn(op)
}

func (cw *ClusterWriter) WriteRedistStart(c *cluster.Cluster, flag bool, zoneid int, src bool, ratelimit int) (err error) {
	// redist enable
	value := TagRedistEnabledTarget
	if src {
		value = TagRedistEnabledSource
		if ratelimit > 0 {
			value = fmt.Sprintf("%s%s%s%s%d", TagRedistEnabledSourceRL, TagFieldSeparator,
				TagRedistRateLimit, TagKeyValueSeparator, ratelimit)
		}
	}

	if !flag {
		value = TagRedistDisabled
	}

	key := KeyRedistEnable(zoneid)
	cw.kvwriter.PutValue(key, value)

	return nil
}

func ParseRedistRateLimit(value string) (ratelimit int) {
	ratelimit = 0 // default

	pairs := strings.Split(value, TagFieldSeparator)
	if len(pairs) < 2 { // no ratelimit
		return
	}

	v := strings.Split(pairs[1], TagKeyValueSeparator)
	if strings.Compare(v[0], string(TagRedistRateLimit)) == 0 {
		limit, err := strconv.Atoi(v[1])
		if err == nil {
			ratelimit = limit
		}
	}
	return
}

func (cw *ClusterWriter) WriteRedistAbort(c *cluster.Cluster) (err error) {

	var op OpList = make(OpList, 0, 20)

	// Delete key for redist
	key := Key(TagRedistFromNode)
	op.AddDeleteWithPrefix(key)

	key = Key(TagRedistNodePrefix)
	op.AddDeleteWithPrefix(key)

	key = Key(TagRedistStatePrefix)
	op.AddDeleteWithPrefix(key)

	key = Key(TagRedistTgtStatePrefix)
	op.AddDeleteWithPrefix(key)

	// Update redistenable key
	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {
		op.AddPut(KeyRedistEnable(zoneid), TagRedistAbortAll)
	}

	return cw.kvwriter.PutValuesWithTxn(op)
}

// Write a new cluster info to etcd.
func (cw *ClusterWriter) write(c *cluster.Cluster, op *OpList) (err error) {

	op.AddPut(TagNumZones, strconv.Itoa(int(c.NumZones)))
	op.AddPut(TagNumShards, strconv.Itoa(int(c.NumShards)))

	// node ip/port info (physical)
	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {
		if !c.IsRedistZone(zoneid) {
			continue
		}
		for nodeid := 0; nodeid < len(c.Zones[zoneid].Nodes); nodeid++ {
			key := KeyNodeIpport(zoneid, nodeid)
			op.AddPut(key, c.ConnInfo[zoneid][nodeid])
		}
	}

	// node shard info (logical)
	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {
		if !c.IsRedistZone(zoneid) {
			continue
		}
		for nodeid := 0; nodeid < len(c.Zones[zoneid].Nodes); nodeid++ {
			key := KeyNodeShards(zoneid, nodeid)
			op.AddPut(key,
				c.Zones[zoneid].Nodes[nodeid].NodeToString(TagPrimSecondaryDelimiter,
					TagShardDelimiter))
		}
	}

	return nil
}

// EtcdWriter
type EtcdWriter struct {
	ClusterWriter
	etcdcli *EtcdClient
}

func (w *EtcdWriter) PutValue(key string, value string) (err error) {
	return w.etcdcli.PutValue(key, value)
}

func (w *EtcdWriter) DeleteKeyWithPrefix(key string, isPrefix bool) (err error) {
	return w.etcdcli.DeleteKeyWithPrefix(key, isPrefix)
}

func (w *EtcdWriter) PutValuesWithTxn(op OpList) (err error) {
	return w.etcdcli.PutValuesWithTxn(op)
}

// StdoutWriter
type StdoutWriter struct {
	ClusterWriter
	keyPrefix string
}

func (w *StdoutWriter) PutValue(key string, value string) (err error) {
	fmt.Printf("%s%s=%s\n", w.keyPrefix, key, value)
	return nil
}

func (w *StdoutWriter) DeleteKeyWithPrefix(key string, isPrefix bool) (err error) {
	fmt.Printf("delete: key=%s%s isPrefix=%v\n", w.keyPrefix, key, isPrefix)
	return nil
}

func (w *StdoutWriter) PutValuesWithTxn(op OpList) (err error) {
	if len(op) == 0 {
		return nil
	}

	fmt.Printf("===txn begin:\n")
	for i := 0; i < len(op); i++ {
		if op[i].IsDelete() {
			fmt.Printf("delete: beginKey=%s%s\n", w.keyPrefix, string(op[i].KeyBytes()))
			endKey := op[i].RangeBytes()
			if endKey != nil {
				fmt.Printf("          endKey=%s%s\n", w.keyPrefix, string(endKey))
			}
		} else {
			fmt.Printf("%s%s=%s\n", w.keyPrefix,
				string(op[i].KeyBytes()), string(op[i].ValueBytes()))
		}
	}
	fmt.Printf("ops_count=%d\n", len(op))
	fmt.Printf("===txn end\n")

	return nil
}
