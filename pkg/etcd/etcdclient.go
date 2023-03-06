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
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/namespace"

	"juno/third_party/forked/golang/glog"
)

var (
	errNotInitialized = errors.New("etcd client not initialized")
)

// etcd client wrapper
type EtcdClient struct {
	initOnce  sync.Once
	config    Config
	keyPrefix string
	client    *clientv3.Client
	doneCh    chan struct{}
	wg        sync.WaitGroup
}

const NotFound = "NotFound"

var (
	shuffleDone = false
	setOnce     sync.Once
)

func NewEtcdClient(cfg *Config, clusterName string) *EtcdClient {

	var client *clientv3.Client
	var err error

	now := time.Now()
	m := now.Second() % len(cfg.Endpoints)

	// Shuffle to balance load
	if m > 0 && !shuffleDone {
		endp := make([]string, len(cfg.Endpoints))
		copy(endp[0:], cfg.Endpoints[0:])
		copy(cfg.Endpoints[0:], endp[m:])
		copy(cfg.Endpoints[len(cfg.Endpoints)-m:], endp[0:m])
	}
	shuffleDone = true

	setOnce.Do(func() { // Bypass http_proxy for connecting to etcd server.
		val := strings.Join(cfg.Endpoints, ",")
		curr := os.Getenv("NO_PROXY")
		if strings.Contains(curr, val) {
			return
		}
		if len(curr) > 0 {
			val += "," + curr
		}
		os.Setenv("NO_PROXY", val)
		os.Setenv("no_proxy", val)
	})

	for i := 0; i < cfg.MaxConnectAttempts; i++ {
		client, err = clientv3.New((*cfg).Config)

		if err == nil {
			break
		}

		if client != nil {
			client.Close()
		}

		if i >= cfg.MaxConnectAttempts-1 {
			glog.Warningf("etcd: %v.", err)
			return nil
		}

		glog.Warningf("etcd: %v. Retry ...", err)
		backoff := (i + 1) * 2
		if backoff > cfg.MaxConnectBackoff {
			backoff = cfg.MaxConnectBackoff
		}
		time.Sleep(time.Duration(backoff) * time.Second)
	}

	etcdcli := &EtcdClient{
		client: client,
		config: *cfg,
		doneCh: make(chan struct{}),
	}

	etcdcli.keyPrefix = cfg.EtcdKeyPrefix + clusterName + TagCompDelimiter
	etcdcli.client.KV = namespace.NewKV(client.KV, etcdcli.keyPrefix)
	etcdcli.client.Watcher = namespace.NewWatcher(client.Watcher, etcdcli.keyPrefix)
	return etcdcli
}

func (e *EtcdClient) Close() {
	close(e.doneCh)

	if e.client != nil {
		e.client.Close()
	}
}

func (e *EtcdClient) GetDoneCh() chan struct{} {
	return e.doneCh
}

func (e *EtcdClient) GetValue(k string) (value string, err error) {
	var resp *clientv3.GetResponse
	resp, err = e.get(k)
	if err != nil {
		glog.Errorf("%v", err)
		return
	}
	if resp != nil {
		sz := len(resp.Kvs)
		if sz == 1 {
			value = string(resp.Kvs[0].Value)
		} else if sz == 0 {
			err = fmt.Errorf("key '%s' not found.", k)
			value = NotFound
		} else { /// not seem to be possible
			err = fmt.Errorf("unexpected response. %s", k)
		}
	} else {
		err = fmt.Errorf("unexpected nil response. key: %s", k)
	}
	return
}

func (e *EtcdClient) GetUint32(k string) (value uint32, err error) {
	var str string
	str, err = e.GetValue(k)
	if err != nil {
		glog.Warningf("%v", err)
		return
	}
	var n uint64
	n, err = strconv.ParseUint(str, 10, 32)
	if err != nil {
		return
	}
	value = uint32(n)
	return
}

func (e *EtcdClient) GetVersion() (version, algver uint32, err error) {
	var str string

	// get version
	str, err = e.GetValue(TagVersion)
	if err != nil {
		glog.Warningf("%v", err)
		return
	}

	var n uint64
	n, err = strconv.ParseUint(str, 10, 32)
	if err != nil {
		return
	}
	version = uint32(n)

	// get alg version, default is 1
	algver = 1
	str, err = e.GetValue(TagAlgVer)
	if err != nil {
		if str == NotFound {
			err = nil
		}
		return
	}

	n, err = strconv.ParseUint(str, 10, 32)
	if err != nil {
		return
	}
	algver = uint32(n)

	return
}

func (e *EtcdClient) PutValue(key string, val string, params ...int) (err error) {
	if e.client == nil {
		err = errNotInitialized
		return
	}

	// optional params: maxtries and backoff sleep time
	maxTries := 1
	backoff := 1 * time.Second // second
	if len(params) > 0 {
		maxTries = params[0]
	}
	if len(params) > 1 {
		backoff = time.Duration(params[1]) * time.Second
	}

	var valStr = val
	var endStr string
	if len(val) >= 50 {
		endStr = " ..."
		valStr = val[:50]
	}

	glog.Debugf("etcd put: key=%s%s val=%s%s", e.keyPrefix, key, valStr, endStr)

	for i := 0; i < maxTries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), e.config.RequestTimeout.Duration)
		_, err = e.client.Put(ctx, key, val)
		cancel()
		if err == nil {
			break
		}

		if i >= maxTries-1 {
			glog.Errorf("[ERROR]: etcd put: %v", err)
			return err
		}

		glog.Warningf("etcd put: %v. Retry ...", err)
		time.Sleep(backoff)
	}

	return nil
}

// Batch operations of delete and put.
func (e *EtcdClient) PutValuesWithTxn(op []clientv3.Op) (err error) {
	if e.client == nil {
		err = errNotInitialized
		return
	}

	if len(op) == 0 {
		return nil
	}

	glog.Infof("etcd txn:")
	for i := 0; i < len(op); i++ {
		if op[i].IsDelete() {
			glog.Infof("etcd delete: beginKey=%s%s", e.keyPrefix, string(op[i].KeyBytes()))
			endKey := op[i].RangeBytes()
			if endKey != nil {
				glog.Infof("               endKey=%s%s", e.keyPrefix, string(endKey))
			}
		} else {
			val := string(op[i].ValueBytes())
			var valStr = val
			var endStr string
			if len(val) >= 20 {
				endStr = " ..."
				valStr = val[:20]
			}
			glog.Debugf("etcd put: key=%s%s val=%s%s", e.keyPrefix,
				string(op[i].KeyBytes()), valStr, endStr)
		}
	}
	glog.Infof("ops_count=%d", len(op))

	maxTries := 5
	for i := 0; i < maxTries; i++ {
		timeout := e.config.RequestTimeout.Duration
		if timeout < 5*time.Second {
			timeout = 5 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)

		_, err = e.client.Txn(ctx).If().Then(op...).Commit()
		cancel()

		if err == nil {
			break
		}

		if i >= maxTries-1 {
			glog.Errorf("[ERROR]: etcd txn aborted: %v", err)
			return err
		}

		glog.Warningf("etcd: %v. Retry ...", err)
		backoff := 5 * time.Second
		time.Sleep(backoff)
	}

	glog.Infof("etcd txn completed")
	return nil
}

func (e *EtcdClient) DeleteKey(key string) (err error) {
	return e.DeleteKeyWithPrefix(key, false)
}

func (e *EtcdClient) DeleteKeyWithPrefix(key string, isPrefix ...bool) (err error) {

	usePrefix := true
	if len(isPrefix) > 0 && !isPrefix[0] {
		usePrefix = false
	}

	ctx, cancel := context.WithTimeout(context.Background(), e.config.RequestTimeout.Duration)

	glog.Infof("etcd delete: key=%s%s isPrefix=%v", e.keyPrefix, key, usePrefix)
	if usePrefix {
		_, err = e.client.Delete(ctx, key, clientv3.WithPrefix())
	} else {
		_, err = e.client.Delete(ctx, key)
	}
	cancel()

	if err != nil {
		glog.Errorf("%v", err)
	}

	return
}

func (e *EtcdClient) WatchEvt(key string, ctx context.Context) (ch clientv3.WatchChan, err error) {
	if e.client == nil {
		err = errNotInitialized
		return
	}

	ch = e.client.Watch(ctx, key, clientv3.WithProgressNotify())
	return
}

func (e *EtcdClient) Watch(key string, handler IWatchHandler, opts ...clientv3.OpOption) (cancel context.CancelFunc, err error) {
	if e.client == nil {
		err = errNotInitialized
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	ch := e.client.Watch(ctx, key, opts...)
	e.wg.Add(1)
	go func() {
		glog.Info("start watcher go routine")
		defer e.wg.Done()
		for {
			select {
			case r := <-ch:
				glog.Info("handle event")
				handler.OnEvent(r.Events...)
				//				for i, e := range r.Events {
				//					fmt.Printf("got %d %s", i, e)
				//				}
			case <-ctx.Done():
				glog.Info("Cancel")
				return
			case <-e.doneCh:
				return
			}
		}
	}()
	return cancel, nil
}

// different flavor
func (e *EtcdClient) WatchEvents(key string, ch chan int) {

	ctx := context.Background()

	rch := e.client.Watch(ctx, key,
		clientv3.WithProgressNotify())

	glog.Infof("etcd: Watcher waits for events.")
	for {
		select {
		case entry := <-rch:
			for _, ev := range entry.Events {
				val, _ := strconv.Atoi(string(ev.Kv.Value))

				glog.Infof("etcd watch: type=%s key=%q val=%q", ev.Type, ev.Kv.Key, ev.Kv.Value)
				if ev.Type == clientv3.EventTypeDelete {
					continue
				}
				// Notify subscriber
				ch <- val
			}
		case <-ctx.Done():
			glog.Info("Cancel")
			return
		case <-e.doneCh:
			close(ch)
			glog.Infof("etcd: Watcher exits.")
			return
		}
	}
}

// List of transactional operations
type OpList []clientv3.Op

func (op *OpList) AddPut(key string, val string) {
	*op = append(*op, clientv3.OpPut(key, val))
}

func (op *OpList) AddDeleteWithPrefix(key string) {
	*op = append(*op, clientv3.OpDelete(key, clientv3.WithPrefix()))
}

func (op *OpList) AddDeleteWithRange(beginKey string, endKey string) {
	*op = append(*op, clientv3.OpDelete(beginKey, clientv3.WithRange(endKey)))
}

func (op *OpList) Clear() {
	*op = (*op)[:0]
}

func (e *EtcdClient) get(key string, params ...int) (resp *clientv3.GetResponse, err error) {
	if e.client == nil {
		err = errNotInitialized
		return
	}

	//optional params: maxtries and backoff sleep time
	maxTries := 2
	backoff := 1 * time.Second
	if len(params) > 0 {
		maxTries = params[0]
	}
	if len(params) > 1 {
		backoff = time.Duration(params[1]) * time.Second
	}

	for i := 0; i < maxTries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), e.config.RequestTimeout.Duration)
		resp, err = e.client.Get(ctx, key)
		cancel()

		if err == nil {
			return
		}

		glog.Warningf("etcd get: %v. Retry ...", err)
		time.Sleep(backoff)
	}

	if err != nil {
		glog.Errorf("[ERROR]: etcd get: key=%s err=%v", key, err)
	}
	return
}

// key is sorted in descending order
func (e *EtcdClient) getWithPrefix(key string, params ...int) (resp *clientv3.GetResponse, err error) {
	if e.client == nil {
		err = errNotInitialized
		return
	}

	//optional params: maxtries and backoff sleep time
	maxTries := 2
	backoff := 1 * time.Second
	if len(params) > 0 {
		maxTries = params[0]
	}
	if len(params) > 1 {
		backoff = time.Duration(params[1]) * time.Second
	}

	for i := 0; i < maxTries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), e.config.RequestTimeout.Duration)
		resp, err = e.client.KV.Get(ctx, key, clientv3.WithPrefix(),
			clientv3.WithSort(clientv3.SortByKey, clientv3.SortDescend))
		cancel()

		if err == nil {
			return
		}

		glog.Warningf("etcd get: %v. Retry ...", err)
		time.Sleep(backoff)
	}

	if err != nil {
		glog.Errorf("[ERROR]: etcd get: key=%s%s err=%v", e.keyPrefix, key, err)
	}
	return
}
