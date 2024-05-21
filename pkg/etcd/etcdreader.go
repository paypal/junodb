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
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/paypal/junodb/pkg/cluster"
	"github.com/paypal/junodb/pkg/shard"
	"github.com/paypal/junodb/pkg/stats/redist"
)

// Implements cluster.IReader
type EtcdReader struct {
	etcdcli *EtcdClient
}

func (cr *EtcdReader) Read(c *cluster.Cluster) (version uint32, err error) {

	var algVer uint32
	if version, algVer, err = cr.etcdcli.GetVersion(); err != nil {
		return
	}

	c.AlgVersion = algVer
	if c.NumShards, err = cr.etcdcli.GetUint32(TagNumShards); err != nil {
		return
	}

	if c.NumZones, err = cr.etcdcli.GetUint32(TagNumZones); err != nil {
		return
	}
	c.ConnInfo = make([][]string, c.NumZones)
	c.Zones = make([]*cluster.Zone, c.NumZones)

	if err = cr.readNodesIpport(c, TagNodeIpport, 2); err != nil {
		return
	}

	if err = cr.readNodesShards(c, TagNodeShards, 2); err != nil {
		return
	}

	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {
		if c.ConnInfo[zoneid] == nil {
			glog.Errorf("[ERROR]: ip:port is missing for zone %d in etcd.", zoneid)
			return 0, errors.New("Missing ip:port in etcd.")
		}

		if c.Zones[zoneid] == nil {
			glog.Errorf("[ERROR]: shardmap is missing for zone %d in etcd.", zoneid)
			return 0, errors.New("Missing shardmap info in etcd.")
		}
	}

	err1 := c.WriteToCache(cr.etcdcli.config.CacheDir, cr.etcdcli.config.CacheName,
		version, false)
	if err1 != nil {
		glog.Errorf("failed to write to etcd cache: %s", err1.Error())
	}

	return
}

func (cr *EtcdReader) ReadWithRedistInfo(c *cluster.Cluster) (version uint32, err error) {

	// read nodes from cluster info
	if version, err = cr.Read(c); err != nil {
		return
	}

	//	glog.Infof("initial cluster")
	//	c.Log()

	// read nodes from redist
	nc := &cluster.Cluster{
		Config: cluster.Config{
			NumZones:  c.NumZones,
			NumShards: c.NumShards,
		},
	}

	nc.ConnInfo = make([][]string, c.NumZones)
	nc.Zones = make([]*cluster.Zone, c.NumZones)
	if err = cr.readNodesIpport(nc, TagRedistNodeIpport, 3); err != nil {
		return
	}

	if err = cr.readNodesShards(nc, TagRedistNodeShards, 3); err != nil {
		return
	}

	if nc.Zones == nil {
		return
	}

	// merge nodes from redist to cluster info for storage server
	for zoneid := 0; zoneid < int(nc.NumZones); zoneid++ {
		originlen := len(c.ConnInfo[zoneid])
		if len(nc.ConnInfo[zoneid]) > originlen {
			c.ConnInfo[zoneid] = append(c.ConnInfo[zoneid], nc.ConnInfo[zoneid][originlen:]...)
		}
	}

	forRedist := false
	for zoneid := 0; zoneid < int(nc.NumZones); zoneid++ {
		if nc.Zones[zoneid] == nil {
			continue
		}

		originlen := len(c.Zones[zoneid].Nodes)
		if nc.Zones[zoneid] != nil && len(nc.Zones[zoneid].Nodes) > originlen {
			c.Zones[zoneid].Nodes = append(c.Zones[zoneid].Nodes, nc.Zones[zoneid].Nodes[originlen:]...)
			c.Zones[zoneid].NumNodes = uint32(len(c.Zones[zoneid].Nodes))
			forRedist = true
		}
	}
	//	glog.Info("redist cluster")
	//	nc.Log()
	glog.Info("cluster info adjusted with new nodes")
	c.Log()

	err1 := c.WriteToCache(cr.etcdcli.config.CacheDir, cr.etcdcli.config.CacheName,
		version, forRedist)
	if err1 != nil {
		glog.Errorf("failed to write to etcd cache: %s", err1.Error())
	}
	return
}

func (cr *EtcdReader) ReadRedistTargetShards(zone int, node int) (shards shard.Map, err error) {
	// outgoing shards
	key := KeyRedistFromNodeByZone(zone)
	resp, err := cr.etcdcli.getWithPrefix(key)
	if err != nil {
		glog.Infof("error reading redist node")
		return
	}

	shards = shard.NewMap()
	for _, ev := range resp.Kvs {
		changeInfo := strings.Split(string(ev.Value), TagRedistShardMoveSeparator)
		for _, change_str := range changeInfo {
			change := strings.Split(change_str, TagCompDelimiter)
			if len(change) < 2 {
				return nil, errors.New("bad cluster info")
			}
			shardid, _ := strconv.Atoi(change[0])
			nodeid, _ := strconv.Atoi(change[1])
			if nodeid == node {
				shards[shard.ID(shardid)] = struct{}{}
			}
		}
	}

	return
}

func (cr *EtcdReader) ReadWithRedistNodeShards(c *cluster.Cluster) (err error) {

	abortZone := make([]bool, int(c.NumZones))
	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {
		var key = KeyRedistEnable(zoneid)
		val, err := cr.etcdcli.GetValue(key)
		if err != nil &&
			val != NotFound {
			return err
		}
		if val == TagRedistAbortZone {
			abortZone[zoneid] = true
		}

	}

	c.Zones = make([]*cluster.Zone, c.NumZones)
	if err = cr.readNodesShards(c, TagRedistNodeShards, 3); err != nil {
		return err
	}

	foundErr := false
	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {
		if !c.IsRedistZone(zoneid) {
			continue
		}
		if c.Zones[zoneid] == nil &&
			!abortZone[zoneid] {
			foundErr = true
			glog.Errorf("Missing redist shards in etcd for zone %d", zoneid)
		}
		if c.Zones[zoneid] != nil &&
			abortZone[zoneid] {
			glog.Exitf("Bad redist shards left in etcd for aborted zone %d", zoneid)
		}
	}

	if foundErr {
		return errors.New("Missing redist shards info in etcd")
	}
	return nil
}

func (cr *EtcdReader) ReadRedistChangeMap(zone int, node int) (changeMap map[uint16]uint16, err error) {
	// outgoing shards
	key := KeyRedistFromNode(zone, node)
	glog.Infof("Getting redist change map: %s", key)
	value, err := cr.etcdcli.GetValue(key)
	if err != nil {
		return
	}

	changeInfo := strings.Split(value, TagRedistShardMoveSeparator)
	changeMap = make(map[uint16]uint16)
	for _, change_str := range changeInfo {
		change := strings.Split(change_str, TagCompDelimiter)
		if len(change) < 2 {
			return nil, errors.New("bad cluster info")
		}
		shardid, _ := strconv.Atoi(change[0])
		nodeid, _ := strconv.Atoi(change[1])
		changeMap[uint16(shardid)] = uint16(nodeid)
	}

	return
}

func (cr *EtcdReader) DumpRedistState() (err error) {

	key := Key(TagRedistPrefix)
	resp, err := cr.etcdcli.getWithPrefix(key)
	if err != nil {
		glog.Errorf("[ERROR] %s", err)
		return err
	}

	var file *os.File
	file, err = os.OpenFile("redist_state.json", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		glog.Error("[ERROR] Unable to open redist_state.json.")
		return err
	}
	defer file.Close()

	glog.Infof("Dump redist state to file.")
	file.WriteString("{\n")
	for i, ev := range resp.Kvs {
		buf := fmt.Sprintf("\"%s\": \"%s\",", string(ev.Key), string(ev.Value))
		if i == len(resp.Kvs)-1 {
			buf = buf[:len(buf)-1]
		}
		_, err = file.WriteString(buf)
		if err != nil {
			glog.Warningf("redist_state: %s", buf)
		}
	}
	file.WriteString("}\n")

	return nil
}

func (cr *EtcdReader) skipZone(zoneid int) (err error) {

	var op OpList = make(OpList, 0, 200)

	key := KeyRedistFromNodeByZone(zoneid)
	op.AddDeleteWithPrefix(key)

	key = KeyRedistNodeStateByZone(zoneid)
	op.AddDeleteWithPrefix(key)

	key = KeyRedistTgtNodeStateByZone(zoneid)
	op.AddDeleteWithPrefix(key)

	key = KeyRedistNodeIpportByZone(zoneid)
	op.AddDeleteWithPrefix(key)

	key = KeyRedistNodeShardsByZone(zoneid)
	op.AddDeleteWithPrefix(key)

	op.AddPut(KeyRedistEnable(zoneid), TagRedistAbortZone)

	glog.Infof("skip zone %d", zoneid)
	err = cr.etcdcli.PutValuesWithTxn(op)

	return err
}

func (cr *EtcdReader) WaitforFinishState(zone int, skip bool,
	maxFailures int, minWait int, notifyDone bool, testOnly bool, ratelimit int, markdown bool) (err error) {

	key := Key(TagRedistStatePrefix, zone)
	start := time.Now()

	for {
		resp, err := cr.etcdcli.getWithPrefix(key)

		if err == context.DeadlineExceeded {
			glog.Warningf("Got timeout from etcd get.  Retrying ...")
			time.Sleep(10 * time.Second)
			continue //  retry
		}
		if err != nil {
			return err
		}

		count := 0
		for _, ev := range resp.Kvs {
			st := redist.NewStats(string(ev.Value))
			if st.GetStatus() == redist.StatsFinish {
				count++
			}
		}

		minExpected := len(resp.Kvs) - maxFailures
		if minExpected < 0 {
			minExpected = 0
		}

		summary := fmt.Sprintf("zone=%d&finish_count=%d&min_expected=%d",
			zone, count, minExpected)
		if err = cr.etcdcli.PutValue(TagRedistStateSummary, summary); err != nil {
			glog.Error(err)
		}

		glog.Infoln("")
		glog.Infof("zone=%d, total_snapshots=%d, finish_count=%d, min_expected=%d",
			zone, len(resp.Kvs), count, minExpected)

		if count == len(resp.Kvs) {
			break
		}

		if count >= minExpected && int(time.Since(start).Seconds()) >= minWait {
			break
		}

		if skip { // Abort and skip this zone
			return cr.skipZone(zone)
		}

		// Continue to wait
		if !notifyDone { // Notify storage servers.
			if err = cr.updateRedistEnable(zone, testOnly, ratelimit, markdown); err != nil {
				return err
			}
			notifyDone = true
		}
		time.Sleep(10 * time.Second)
	}

	return nil
}

func (cr *EtcdReader) GetValue(k string) (value string, err error) {
	value, err = cr.etcdcli.GetValue(k)
	return
}

// tag: either TagNodeIpport or TagRedistNodeIpport
// offset: is the index of the token for zoneid after split with delimiter "_"
// redist_node_ipport_0_1 => 3
// node_ipport_0_0 => 2
func (cr *EtcdReader) readNodesIpport(c *cluster.Cluster, tag string, offset int) (err error) {
	resp, err := cr.etcdcli.getWithPrefix(tag)
	if err != nil {
		return err
	}

	// if resp.Count == 0 {
	//	return errors.New("0 node")
	// }

	for _, ev := range resp.Kvs {
		tokens := strings.Split(string(ev.Key), TagCompDelimiter)
		if len(tokens) < offset+2 {
			// log error?
			continue
		}
		zoneid, _ := strconv.Atoi(tokens[offset])
		nodeid, _ := strconv.Atoi(tokens[offset+1])

		if zoneid >= int(c.NumZones) {
			// log error?
			continue
		}

		// the prefix fetch is sorted by key in reverse order
		if c.ConnInfo[zoneid] == nil {
			c.ConnInfo[zoneid] = make([]string, nodeid+1)
		}

		c.ConnInfo[zoneid][nodeid] = string(ev.Value)
	}
	return nil
}

// tag: either TagNodeShards or TagRedistNodeShards
func (cr *EtcdReader) readNodesShards(c *cluster.Cluster, tag string, offset int) (err error) {
	resp, err := cr.etcdcli.getWithPrefix(tag)
	if err != nil {
		return err
	}

	// if resp.Count == 0 {
	//	return errors.New("0 node")
	// }

	for _, ev := range resp.Kvs {
		tokens := strings.Split(string(ev.Key), TagCompDelimiter)
		if len(tokens) < offset+2 {
			// log error?
			continue
		}
		zoneid, _ := strconv.Atoi(tokens[offset])
		nodeid, _ := strconv.Atoi(tokens[offset+1])

		if zoneid >= int(c.NumZones) {
			// log error?
			continue
		}

		// the prefix fetch is sorted by key in reverse order
		if c.Zones[zoneid] == nil {
			c.Zones[zoneid] = cluster.NewZoneFromConfig(uint32(zoneid), uint32(nodeid+1), c.NumZones, c.NumShards)
		}

		c.Zones[zoneid].Nodes[nodeid].StringToNode(uint32(zoneid), uint32(nodeid),
			string(ev.Value), TagPrimSecondaryDelimiter, TagShardDelimiter)
	}

	return nil
}

func (cr *EtcdReader) waitForTgtNodeReady(zone int) (err error) {

	key := Key(TagRedistTgtStatePrefix, zone)

	for {
		time.Sleep(10 * time.Second)
		resp, err := cr.etcdcli.getWithPrefix(key)

		if err == context.DeadlineExceeded {
			glog.Warningf("Got timeout from etcd get.  Retrying ...")
			continue //  retry
		}

		if err != nil {
			glog.Infof("error reading etcd")
			return err
		}

		count := 0
		for _, ev := range resp.Kvs {
			if string(ev.Value) == TagRedistTgtStateReady {
				count++
			}
		}

		glog.Infof("zone=%d, total_target_nodes=%d, ready_count=%d", zone,
			len(resp.Kvs), count)

		if count == len(resp.Kvs) {
			return nil
		}

	}
	return nil
}

func (cr *EtcdReader) updateRedistEnable(zone int, forTest bool, ratelimit int, markdown bool) (err error) {
	key := KeyRedistEnable(zone)
	val, err := cr.etcdcli.GetValue(key)
	if err != nil {
		return err
	}

	switch val {
	case TagRedistEnabledReady: // enable target
		if err := cr.etcdcli.PutValue(key, TagRedistEnabledTarget); err != nil {
			return err
		}
		fallthrough

	case TagRedistEnabledTarget: // enable source
		if !forTest {
			err := cr.waitForTgtNodeReady(zone)
			if err != nil {
				return err
			}
		}
		value := TagRedistEnabledSource
		if ratelimit > 0 {
			value = fmt.Sprintf("%s%s%s%s%d", TagRedistEnabledSourceRL, TagFieldSeparator,
				TagRedistRateLimit, TagKeyValueSeparator, ratelimit)
		}
		if err := cr.etcdcli.PutValue(key, value); err != nil {
			return err
		}

		if markdown {
			cr.etcdcli.PutValue(TagZoneMarkDown, strconv.Itoa(zone), 2)
			glog.Infof("Sleep 30 sec after markdown.")
			time.Sleep(30 * time.Second)
		}
	}

	return nil
}
