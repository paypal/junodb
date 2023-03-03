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
package watcher

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"juno/third_party/forked/golang/glog"

	clientv3 "go.etcd.io/etcd/client/v3"

	"juno/pkg/cluster"
	"juno/pkg/etcd"
	"juno/pkg/shard"
)

// watch for the following etcd changes
// 1) shard mapping change: watch for <clustername>_version
//    when version updated, the new shards for the storage node will be read and updated
//
// 2) redistribution enable/disable: <clustername>_redist_enable_<zoneid>
//    possible value: yes_target, or yes_source or no
//    - for target, the additional shards for redisbution will be read and updated
//    - for source, it will start to transfer the snapshots corresponding to the shards to the new target
//    -             and forward real time requests too

type IWatchEvtHandler interface {
	UpdateShards(shards shard.Map) bool
	UpdateRedistShards(shards shard.Map) bool

	RedistStart(ratelimit int)
	RedistResume(ratelimit int)
	RedistStop()
	RedistIsInProgress() bool
}

type Watcher struct {
	clustername string
	zoneid      uint16
	nodeid      uint16
	version     uint32
	etcdcli     *etcd.EtcdClient
	cancel      context.CancelFunc
	updateDelay time.Duration
	name        string
	hdr         IWatchEvtHandler
}

var (
	theWatcher *Watcher
	evtHdr     IWatchEvtHandler
)

func RegisterWatchEvtHandler(hdr IWatchEvtHandler) {
	if theWatcher != nil {
		theWatcher.hdr = hdr
	} else {
		evtHdr = hdr
	}
}

func Init(clustername string, zoneid uint16, nodeid uint16, cfg *etcd.Config, delay time.Duration, version uint32) (err error) {
	glog.Debugf("redist.Init: zoneid:%d, nodeid:%d", zoneid, nodeid)

	etcdcli := etcd.GetEtcdCli()
	if etcdcli == nil {
		etcdcli = etcd.NewEtcdClient(cfg, clustername)
	}

	if etcdcli == nil {
		return errors.New("failed to connect to etcd")
	}

	theWatcher = newWatcher(clustername, zoneid, nodeid, etcdcli, delay, version)
	if evtHdr != nil {
		theWatcher.hdr = evtHdr
	}
	go theWatcher.Watch()
	return nil
}

func newWatcher(clustername string, zoneid uint16, nodeid uint16, cli *etcd.EtcdClient, deley time.Duration, version uint32) *Watcher {
	w := &Watcher{
		clustername: clustername,
		zoneid:      zoneid,
		nodeid:      nodeid,
		etcdcli:     cli,
		updateDelay: deley,
		version:     version,
	}

	w.name = fmt.Sprintf("ss-%d-%d watcher", w.zoneid, w.nodeid)
	return w
}

func NewWatcher(clustername string, zoneid uint16, nodeid uint16, cli *etcd.EtcdClient, deley time.Duration, version uint32, hdr IWatchEvtHandler) *Watcher {
	w := &Watcher{
		clustername: clustername,
		zoneid:      zoneid,
		nodeid:      nodeid,
		etcdcli:     cli,
		updateDelay: deley,
		version:     version,
	}
	w.hdr = hdr
	w.name = fmt.Sprintf("Watcher ss-%d-%d", w.zoneid, w.nodeid)
	return w
}

// called during SS start up to resume any unfinished redistribution task.
func (w *Watcher) Restart() {
	rw := etcd.GetClsReadWriter()
	if rw == nil {
		glog.Info("no etcd reader")
		return
	}

	redist_enable, err := rw.GetValue(etcd.KeyRedistEnable(int(w.zoneid)))
	if err != nil {
		return
	}

	state := []byte(redist_enable)
	w.processRedistByState(state, true)
}

// run in a seperate go routine
func (w *Watcher) Watch() (cancel context.CancelFunc, err error) {
	// if needed
	w.Restart()

	ctx, cancel := context.WithCancel(context.Background())
	chShardMap, err := w.etcdcli.WatchEvt(etcd.TagVersion, ctx)
	if err != nil {
		return
	}
	chRedist, err := w.etcdcli.WatchEvt(etcd.KeyRedistEnable(int(w.zoneid)), ctx)
	if err != nil {
		return
	}

	glog.Infof("start %s go routine", w.name)
	for {
		select {
		case r := <-chRedist:
			for _, ev := range r.Events {
				if ev != nil {
					w.onRedistEvent(ev)
				}
			}
		case m := <-chShardMap:
			for _, ev := range m.Events {
				if ev.Type != clientv3.EventTypeDelete {
					w.onShardMapEvent(ev)
				}
			}
		case <-ctx.Done():
			glog.Info("Cancel")
			break
		case <-w.etcdcli.GetDoneCh():
			glog.Info("Done")
			break
		}
	}

	glog.Info("watcher exit")
	return cancel, nil
}

func (w *Watcher) onRedistEvent(ev *clientv3.Event) {
	glog.Infof("%s redist evt: %s", w.name, string(ev.Kv.Value))
	w.processRedistByState(ev.Kv.Value, false)
}

func (w *Watcher) processRedistByState(state []byte, restart bool) {

	lensrctag := len(etcd.TagRedistEnabledSourceRL)
	lenresumerltag := len(etcd.TagRedistResumeRL)
	enable := restart

	if bytes.Compare(state, []byte(etcd.TagRedistEnabledTarget)) == 0 {

		glog.Infof("%s redist start evt for target", w.name)
		w.processRedistTarget(true)

	} else if bytes.Compare(state, []byte(etcd.TagRedistAbortAll)) == 0 ||

		bytes.Compare(state, []byte(etcd.TagRedistAbortZone)) == 0 {
		glog.Infof("%s redist abort!", w.name)
		w.processRedistSource(false, 0, false)
		w.processRedistTarget(false)

	} else if bytes.Compare(state, []byte(etcd.TagRedistResume)) == 0 {

		// resume
		glog.Infof("%s redist resume", w.name)
		if restart {
			w.processRedistTarget(true)
		}
		w.processRedistSource(enable, 0, true)

	} else if bytes.Compare(state, []byte(etcd.TagRedistEnabledSource)) == 0 {

		glog.Info("redist start evt for src")
		if restart {
			w.processRedistTarget(true)
		}
		w.processRedistSource(true, 0, false)

	} else if len(state) >= lensrctag && bytes.Compare(state[0:lensrctag], []byte(etcd.TagRedistEnabledSourceRL)) == 0 {

		ratelimit := etcd.ParseRedistRateLimit(string(state))
		glog.Infof("redist start evt for src with ratelimit: %d", ratelimit)
		if restart {
			w.processRedistTarget(true)
		}
		w.processRedistSource(true, ratelimit, false)

	} else if len(state) >= lenresumerltag && bytes.Compare(state[0:lenresumerltag], []byte(etcd.TagRedistResumeRL)) == 0 {

		// resume with rate limit
		ratelimit := etcd.ParseRedistRateLimit(string(state))
		glog.Infof("redist resume with ratelimit: %d", ratelimit)
		if restart {
			w.processRedistTarget(true)
		}
		w.processRedistSource(enable, ratelimit, true)
	} else {
		//glog.Infof("unsupported request %s", state)
	}
}

func (w *Watcher) onShardMapEvent(e *clientv3.Event) {
	if ver, err := strconv.Atoi(string(e.Kv.Value)); err != nil {
		glog.Errorf("fail to convert event value to int. %s", err.Error())
		return
	} else if uint32(ver) <= w.version {
		glog.Infof("shard map update event, ignored. version (%d) < current version (%d)", ver, w.version)
		return
	}

	// if redistribution not finished, not allowed to change shard map.
	if w.hdr.RedistIsInProgress() {
		glog.Error("redistribution is in progress, not allowed to change ShardMap")
		return
	}

	// TODO: add some delay here
	time.Sleep(w.updateDelay)

	//	if redist.IsEnabled() {
	//		mgr := redist.GetManager()
	//		if mgr != nil && !mgr.IsDone() {
	//			glog.Error("redistribution is in progress, not allowed to change ShardMap")
	//			return
	//		}
	//	}

	// get clusterinfo
	rw := etcd.GetClsReadWriter()
	if rw == nil {
		glog.Info("shard map update event err: no etcd reader")
		return
	}

	var cls cluster.Cluster
	version, err := rw.Read(&cls)
	if err != nil || version <= w.version {
		glog.Info("shard map update event, ignore: not new version")
		return
	}

	glog.Infof("shard map update event, cur ver: %d, new ver: %d",
		w.version, version)

	var shardMap shard.Map
	if uint32(w.nodeid) >= cls.Zones[w.zoneid].NumNodes {
		shardMap = shard.NewMapWithSize(0) //?
	} else {
		node := cls.Zones[w.zoneid].Nodes[w.nodeid]
		shards := node.GetShards()
		shardMap = shard.NewMapWithSize(len(shards))
		for _, s := range shards {
			shardMap[shard.ID(s)] = struct{}{}
		}
	}

	if w.hdr != nil {
		w.hdr.UpdateShards(shardMap)
	}

	// stop redistribution
	w.processRedistSource(false, 0, false)
	w.processRedistTarget(false)
	w.version = version
}

// Prepare target node to accept new shards
// -- added new shards allowed
// -- set redist_tgtstate_zoneid_nodeid to ready
func (w *Watcher) processRedistTarget(enable bool) {

	glog.Infof("%s process tgt: enable=%t", w.name, enable)

	// get clusterinfo
	rw := etcd.GetClsReadWriter()
	if rw == nil {
		glog.Infof("%s no etcd reader", w.name)
		return
	}

	var shards shard.Map
	var err error
	if enable {
		// get shards that will be added for this node as target
		shards, err = rw.ReadRedistTargetShards(int(w.zoneid), int(w.nodeid))
		if err != nil {
			glog.Infof("%s err reading redist target shards, %s", w.name, err.Error())
			return
		}

		if len(shards) == 0 {
			glog.Infof("%s no new shards", w.name)
			return
		}

		glog.Infof("%s adding %d shards to the target: %v", w.name, len(shards), shards.Keys())
	}

	// update the db
	if w.hdr != nil {
		w.hdr.UpdateRedistShards(shards)
	}

	if enable {
		key := etcd.KeyRedistTgtNodeState(int(w.zoneid), int(w.nodeid))
		err := rw.PutValue(key, etcd.TagRedistTgtStateReady)
		// TODO: retry?
		if err != nil {
			glog.Errorf("%s set tgt ready state error: %s", w.name, err.Error())
		}
		glog.Infof("%s target ready", w.name)
	}
}

// source start to forward requests to target
func (w *Watcher) processRedistSource(enable bool, ratelimit int, resume bool) {
	glog.Infof("redist.Watcher process src: enable=%t, ratelimit: %d, resume=%v",
		enable, ratelimit, resume)
	defer glog.Infof("redist.Watcher process src done: enable=%t", enable)

	if enable {
		w.hdr.RedistStart(ratelimit)
	} else {
		if resume {
			glog.Infof("resume redist")
			w.hdr.RedistResume(ratelimit)
		} else {
			glog.Infof("redist process stop")
			w.hdr.RedistStop()
		}
	}
}
