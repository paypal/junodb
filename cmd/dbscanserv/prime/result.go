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
package prime

import (
	"strings"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/client"
	"juno/pkg/util"
)

type KeyList struct {
	Rangeid int
	Keys    []string
}

func (k *KeyList) AppendKey(key string) {
	k.Keys = append(k.Keys, key)
}

type Result struct {
	TotalKeys   int
	FailKeys    int
	OkRepairs   int
	FailRepairs int

	KeysByZone []KeyList
}

type Report struct {
	TotalKeys   int
	TotalFails  int
	OkRepairs   int
	FailRepairs int
}

var (
	proxyAddr     string
	junoClientMap = make(map[string]client.IClient, 10)
)

func SetProxyAddr(addr string) {
	proxyAddr = addr
}

func GetProxyAddr() string {
	return proxyAddr
}

func IsScanOnly() bool {
	return len(proxyAddr) == 0
}

func CloseProxyConnect() {
	junoClientMap = make(map[string]client.IClient, 10)
	proxyAddr = ""
}

func RepairKey(key []byte, display bool) bool {

	ns, appkey, ok := GetNamespaceAndKey(key)
	if !ok {
		LogMsg("[ERROR] Repair: Bad key=%v", key)
		return false
	}

	cli, found := junoClientMap[ns]
	if !found {

		clientCfg := client.Config{
			Appname:            "dbscan",
			Namespace:          ns,
			RetryCount:         1,
			ConnectTimeout:     util.Duration{500 * time.Millisecond},
			ReadTimeout:        util.Duration{500 * time.Millisecond},
			WriteTimeout:       util.Duration{500 * time.Millisecond},
			RequestTimeout:     util.Duration{1000 * time.Millisecond},
			ConnRecycleTimeout: util.Duration{60 * time.Second},
		}
		clientCfg.Server.Addr = proxyAddr

		var err error
		cli, err = client.New(clientCfg)
		if err != nil {
			glog.Errorf("[ERROR] %s", err)
			return false
		}
		junoClientMap[ns] = cli
	}

	val, ctx, err := cli.Get(appkey)

	if err != nil {
		if strings.Contains(err.Error(), "unsupported payload type") ||
			strings.Contains(err.Error(), "no key") {
			return true
		}
		LogMsg("[ERROR] Repair: %s, namespace=%s payload_len=%d key=%v",
			err.Error(), ns, len(val), appkey)
		return false
	}
	if display {
		glog.Infof("get ct=%d ver=%d et=%d",
			ctx.GetCreationTime(), ctx.GetVersion(),
			uint32(time.Now().Unix())+ctx.GetTimeToLive())
	}
	return true
}

func (r *Result) Init(numZones int) {
	r.KeysByZone = make([]KeyList, numZones)
}

func (r *Result) CountKeys() int {
	sum := 0
	for _, v := range r.KeysByZone {
		sum += len(v.Keys)
	}
	return sum
}

func (r *Result) AppendData(key []byte, val []byte) {

	if IsCopyNamespace() {
		return
	}
	r.FailKeys++

	if IsScanOnly() {
		return
	}

	if RepairKey(key, false) {
		r.OkRepairs++
	} else {
		r.FailRepairs++
	}
}

func (r *Result) AppendKey(key string, zoneid int) {
	if r.KeysByZone == nil {
		return
	}
	r.KeysByZone[zoneid].AppendKey(key)
}

func (r *Result) AddResult(other Result) {
	r.TotalKeys += other.TotalKeys
	r.OkRepairs += other.OkRepairs
	r.FailRepairs += other.FailRepairs
	r.FailKeys += other.FailKeys
}

func (t *Report) AddResult(i int, result Result) {

	t.TotalKeys += result.TotalKeys
	t.TotalFails += result.FailKeys
	t.OkRepairs += result.OkRepairs
	t.FailRepairs += result.FailRepairs

	LogMsg("node=%d keys=%d fails=%d okRepairs=%d failRepairs=%d", i,
		result.TotalKeys, result.FailKeys,
		result.OkRepairs, result.FailRepairs)
}

func (t *Report) Summary(driver bool) {

	glog.Infof("totalKeys=%d failKeys=%d okRepairs=%d failRepairs=%d",
		t.TotalKeys, t.TotalFails, t.OkRepairs, t.FailRepairs)

	LogMsg("\ntotalKeys=%d failKeys=%d okRepairs=%d failRepairs=%d",
		t.TotalKeys, t.TotalFails, t.OkRepairs, t.FailRepairs)

	if driver {
		glog.Infof("repKeys=%d repErrors=%d", GetReplicateCount(), GetReplicateErrors())
		LogMsg("repKeys=%d repErrors=%d", GetReplicateCount(), GetReplicateErrors())
	}
}

func (t *Report) AddCount(result Result) {

	t.TotalKeys += result.TotalKeys
	t.TotalFails += result.FailKeys
	t.OkRepairs += result.OkRepairs
	t.FailRepairs += result.FailRepairs
}

func GetNamespaceAndKey(key []byte) (namespace string, appkey []byte, ok bool) {
	if len(key) < 6 {
		ok = false
		return
	}
	last := 4 + uint8(key[3])

	if len(key) < int(last)+1 {
		ok = false
		return
	}
	namespace = string(key[4:last])
	appkey = key[last:]
	ok = true
	return
}
