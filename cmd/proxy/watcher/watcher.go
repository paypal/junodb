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

package watcher

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/proc"
	"juno/pkg/cluster"
	"juno/pkg/etcd"
	"juno/pkg/util"
)

type Watcher struct {
	clustername string
	etcdcli     *etcd.EtcdClient
	etcdcfg     *etcd.Config
	cancel      context.CancelFunc
	wg          sync.WaitGroup
}

var (
	theWatcher  *Watcher
	markdownobj *cluster.ZoneMarkDown
)

func Initialize(args ...interface{}) (err error) {
	sz := len(args)
	if sz == 0 {
		err = fmt.Errorf("watcher config expected")
		glog.Error(err)
		return
	}
	var clustername string
	var etcdcli *etcd.EtcdClient
	var ok bool
	var etcdcfg *etcd.Config

	clustername, ok = args[0].(string)
	if !ok {
		err = fmt.Errorf("wrong cluster name type")
		glog.Error(err)
		return
	}

	etcdcli, ok = args[1].(*etcd.EtcdClient)
	if !ok {
		err = fmt.Errorf("wrong etcd client type")
		glog.Error(err)
		return
	}

	etcdcfg, ok = args[2].(*etcd.Config)
	if !ok {
		err = fmt.Errorf("wrong etcd config")
		glog.Error(err)
		return
	}
	err = Init(clustername, etcdcli, etcdcfg)
	return
}

func Finalize() {
	if theWatcher != nil {
		theWatcher.Stop()
	}
}

func Init(clustername string, etcdcli *etcd.EtcdClient, cfg *etcd.Config) (err error) {
	glog.Debug("watcher.Init")

	markdownobj = cluster.GetMarkDownObj()
	markdownobj.Reset()
	theWatcher = newWatcher(clustername, etcdcli, cfg)
	theWatcher.Watch()
	return nil
}

func newWatcher(clustername string, cli *etcd.EtcdClient, cfg *etcd.Config) *Watcher {
	w := &Watcher{
		clustername: clustername,
		etcdcli:     cli,
		etcdcfg:     cfg,
	}

	return w
}

/////////////////////////////////////
// Watch for
// -- the shard map change (version)
// -- ZoneMarkDown
/////////////////////////////////////

// At any given time, we can mark down at most one zone
func (w *Watcher) Watch() (err error) {
	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	w.wg.Add(1)
	go func() {
		defer glog.Info("watcher exit")
		defer w.wg.Done()
		glog.Info("start proxy watcher go routine")

		var retryTimer *util.TimerWrapper
		var chRetry <-chan time.Time = nil
		var chMarkDown, chLimitsConfigChange clientv3.WatchChan

		if w.etcdcli != nil {
			val, err := w.etcdcli.GetValue(etcd.TagZoneMarkDown)
			if err == nil {
				zoneid, err := strconv.ParseUint(val, 10, 32)
				if err == nil {
					markdownobj.MarkDown(int32(zoneid))
					glog.Infof("markdown: zoneid=%d", zoneid)
				}
			}
			if chMarkDown, err = w.etcdcli.WatchEvt(etcd.TagZoneMarkDown, ctx); err != nil {
				glog.Errorln(err)
			}
			if chLimitsConfigChange, err = w.etcdcli.WatchEvt(etcd.TagLimitsConfig, ctx); err != nil {
				glog.Errorln(err)
			}
		} else {
			// set a timer to reconnect every 3 minutes
			retryTimer = util.NewTimerWrapper(time.Duration(3) * time.Minute)
			defer retryTimer.Stop()
			retryTimer.Reset(time.Duration(3) * time.Minute)
			chRetry = retryTimer.GetTimeoutCh()
		}

		for {
			select {
			case <-ctx.Done():
				glog.Info("Watcher::Cancel")
				return
			case <-w.etcdcli.GetDoneCh():
				glog.Info("Watcher::Done")
				return
			case d, ok := <-chMarkDown:
				if !ok {
					// watch again
					glog.Info("chMarkDown closed for unknow error, watch again")
					chMarkDown, err = w.etcdcli.WatchEvt(etcd.TagZoneMarkDown, ctx)
					continue
				}
				for _, ev := range d.Events {
					w.onMarkDownEvent(ev)
				}
			case <-chRetry:
				if w.etcdcli == nil {
					w.etcdcli = etcd.NewEtcdClient(w.etcdcfg, w.clustername)
				}

				if w.etcdcli != nil {
					glog.Info("etcd connected")
					val, err := w.etcdcli.GetValue(etcd.TagZoneMarkDown)
					if err == nil {
						zoneid, err := strconv.ParseUint(val, 10, 32)
						if err == nil {
							markdownobj.MarkDown(int32(zoneid))
							glog.Infof("markdown: zoneid=%d", zoneid)
						}
					}
					if chMarkDown, err = w.etcdcli.WatchEvt(etcd.TagZoneMarkDown, ctx); err != nil {
						glog.Errorln(err)
					}
					if chLimitsConfigChange, err = w.etcdcli.WatchEvt(etcd.TagLimitsConfig, ctx); err != nil {
						glog.Errorln(err)
					}
					retryTimer.Stop()
					chRetry = nil
				} else {
					retryTimer.Reset(time.Duration(3) * time.Minute)
				}
			case t, ok := <-chLimitsConfigChange:
				if ok {
					l := len(t.Events)
					if l > 0 {
						ev := t.Events[l-1]
						glog.Infof("Event: %s %s", ev.Type, etcd.TagLimitsConfig)
						if ev.Type == clientv3.EventTypePut {
							if tm, err := strconv.ParseInt(string(ev.Kv.Value), 10, 64); err == nil {
								proc.UpdateLimitsConfig(tm)
							}
						} else if ev.Type == clientv3.EventTypeDelete {
							// TODO
						}
					}
				}
			}
		}
	}()
	return nil
}

func (w *Watcher) onMarkDownEvent(ev *clientv3.Event) {
	glog.Infof("markdown evt: type=%d, value=%s", int(ev.Type), string(ev.Kv.Value))
	if ev.Type == clientv3.EventTypeDelete {
		// mark up
		glog.Infof("zone mark down removed")
		markdownobj.Reset()
		return
	}
	zoneid, err := strconv.ParseUint(string(ev.Kv.Value), 10, 32)
	if err == nil {
		glog.Infof("markdown: zoneid=%d", zoneid)
		markdownobj.MarkDown(int32(zoneid))
	} else {
		glog.Errorf("markdown failed. Error:", err.Error())
	}
}

func (w *Watcher) Stop() {
	glog.Infof("stop watcher")
	w.cancel()
	w.wg.Wait()
}
