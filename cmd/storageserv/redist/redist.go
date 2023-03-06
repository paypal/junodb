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
  
package redist

import (
	"errors"
	//"strings"
	"sync"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/etcd"
	"juno/pkg/io"
	"juno/pkg/proto"
	"juno/pkg/shard"
	redistst "juno/pkg/stats/redist"
)

type IDBRedistHandler interface {
	SendRedistSnapshot(shardId shard.ID, rb *Replicator, mshardid int32) bool
}

var (
	enabled    int32
	theManager *Manager
	theLock    sync.RWMutex // or use unsafe ponter?
	watchHdr   etcd.IWatchHandler
	redistHdr  IDBRedistHandler
	etcdcli    *etcd.EtcdClient
)

func Init(clustername string, zoneid uint16, nodeid uint16, cfg *etcd.Config) (err error) {
	glog.Debugf("redist.Init: zoneid:%d, nodeid:%d", zoneid, nodeid)

	etcdcli := etcd.GetEtcdCli()
	if etcdcli == nil {
		etcdcli = etcd.NewEtcdClient(cfg, clustername)
	}

	if etcdcli == nil {
		return errors.New("failed to connect to etcd")
	}

	//	watcher := watcher.NewWatcher(clustername, zoneid, nodeid, etcdcli)
	//	go watcher.Watch()
	//watchHdr = newWatchHandler(clustername, zoneid, nodeid, etcdcli)
	return nil
}

func RegisterDBRedistHandler(hdr IDBRedistHandler) {
	redistHdr = hdr
}

func GetRedistHandler() IDBRedistHandler {
	return redistHdr
}

func IsEnabled() bool {
	enabled := atomic.LoadInt32(&enabled)
	return enabled != 0
}

func SetEnable(flag bool) {
	glog.Infof("RedistEnabled: %v", flag)
	var value int32 = 0
	if flag {
		value = 1
	}
	atomic.StoreInt32(&enabled, value)
}

func GetManager() *Manager {
	theLock.Lock()
	defer theLock.Unlock()
	return theManager
}

func SetManager(mgr *Manager) {
	theLock.Lock()
	theManager = mgr
	defer theLock.Unlock()
}

type Manager struct {
	zoneid       uint16
	nodeid       uint16
	ssconfig     *io.OutboundConfig
	nodeConnInfo []string                 // connection info for the all nodes in the rack
	processors   []*io.OutboundProcessor  // processors corresponding to new nodes
	changeMap    map[shard.ID]*Replicator // shards need to move to new node
	etcdcli      *etcd.EtcdClient
	wg           sync.WaitGroup
	stop         int32 // atomic flag to signal Manager to stop: 1 - stop, 0 - ok
	redistDone   int32 // atomic flag to indicate redistribution is done (all snapshot transferred)
}

func NewManager(zoneid uint16, nodeid uint16, connInfo []string,
	changeMap map[uint16]uint16, conf *Config, cli *etcd.EtcdClient, ratelimit int) (m *Manager, err error) {

	glog.Debugf("NewManager: %v, %v", connInfo, changeMap)
	m = &Manager{
		zoneid:       zoneid,
		nodeid:       nodeid,
		nodeConnInfo: connInfo,
		ssconfig:     &conf.Outbound,
		processors:   make([]*io.OutboundProcessor, len(connInfo)),
		changeMap:    make(map[shard.ID]*Replicator),
		etcdcli:      cli,
		stop:         0,
		redistDone:   0,
	}

	for shardid, nid := range changeMap {
		if nid >= uint16(len(connInfo)) {
			return nil, errors.New("bad Shard Change Map: node id out of bound")
		}

		processor := m.processors[nid]

		if processor == nil {
			processor = io.NewOutboundProcessor(connInfo[nid], &conf.Outbound, false)
			if processor == nil {
				return nil, errors.New("bad redistr manager: failed to create processor")
			}
			m.processors[nid] = processor
		}
		glog.Debugf("processor created: %v", processor)
		statskey := etcd.KeyRedistNodeState(int(m.zoneid), int(m.nodeid), int(shardid))
		Replicator := NewBalancer(shard.ID(shardid), processor, &m.wg, statskey, ratelimit, cli)
		m.changeMap[shard.ID(shardid)] = Replicator
	}

	glog.Debugf("rebalance mananger: %v", m)
	m.wg.Add(1)
	go m.Start()
	return m, nil
}

func (m *Manager) Start() {
	defer m.wg.Done()
	defer atomic.StoreInt32(&m.redistDone, 1)
	// wait a little bit so that the outboundconnectors are ready to use.
	time.Sleep(1 * time.Second)
	// TODO manage concurrent Replicator snapshot forwarding & rate_limit
	totalShards := len(m.changeMap)

	// try 5 times
	for i := 0; i < 5; i++ {
		finishCnt := 0

		for _, rb := range m.changeMap {
			//m.wg.Add(1)
			//go snapshotHdr.Send(rb.GetShardId(), rb, &m.wg)
			// for now, no parallization on redistributing the snapshot

			// TO revisit
			if m.IsStopped() {
				break
			}

			// check for snapshot finsih
			key := etcd.KeyRedistNodeState(int(m.zoneid), int(m.nodeid), int(rb.GetShardId()))
			curval, err := m.etcdcli.GetValue(key)
			var mshardid int32 = 0
			if err == nil {
				st := redistst.NewStats(curval)

				status := st.GetStatus()
				if status == redistst.StatsFinish {
					// already finished, skip
					glog.Infof("%d completed, skip", int(rb.GetShardId()))
					finishCnt++
					continue
				} else if status == redistst.StatsAbort {
					// resume from next mshard id
					rb.RestoretSnapShotState(st)
					mshardid = st.GetMShardId()
					if mshardid != 0 {
						mshardid++
					}
				}
			}

			// send the shard
			redistHdr.SendRedistSnapshot(rb.GetShardId(), rb, mshardid)

			curval, err = m.etcdcli.GetValue(key)
			if err == nil {
				st := redistst.NewStats(curval)
				if st.GetStatus() == redistst.StatsFinish {
					// already finished, skip
					finishCnt++
					continue
				}
			}
		}

		if finishCnt == totalShards {
			glog.Infof("Redistribution finished: total %d shards", totalShards)
			return
		}
	}
	glog.Infof("Redistribution aborted -- too many errors")
}

func (m *Manager) Resume(ratelimit int) {
	if !m.IsDone() {
		glog.Info("the redistribution is still running, skip")
		return
	}

	for _, rb := range m.changeMap {
		rb.SetRateLimit(ratelimit)
	}
	m.wg.Add(1)
	m.Start()
}

func (m *Manager) Stop() {
	atomic.StoreInt32(&m.stop, 1)
	m.wg.Wait()

	// cleanup the processors -- close the connections to the targets
	for _, proc := range m.processors {
		if proc != nil {
			proc.Shutdown()
		}
	}
}

func (m *Manager) IsStopped() bool {
	stopped := atomic.LoadInt32(&m.stop)
	return stopped != 0
}

func (m *Manager) IsDone() bool {
	redistDone := atomic.LoadInt32(&m.redistDone)
	return redistDone != 0
}

func (m *Manager) Forward(shardId shard.ID, msg *proto.RawMessage, realtime bool, cntOnFailure bool) error {

	Replicator := m.changeMap[shardId]
	if Replicator == nil {
		return nil
	}

	glog.Debugf("rebalance manager: forward msg %d, rb=%v", shardId, Replicator)
	err := Replicator.SendRequest(msg, realtime, cntOnFailure)
	if err != nil {
		glog.Debugf("forward msg %d err %v", shardId, err)
	}
	return err
}

func (m *Manager) GetReplicator(shardId shard.ID) (re *Replicator) {
	return m.changeMap[shardId]
}
