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
  
package prime

import (
	"bytes"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/storage/db"
	"juno/pkg/proto"
)

type OpRecord struct {
	key                  string
	lastModificationTime uint64
	creationTime         uint32
	version              uint32
	expirationTime       uint32
	originatorRequestId  proto.RequestId

	payloadType proto.PayloadType
	payloadData []byte
}

type OpData struct {
	Rangeid    int
	recordList []OpRecord
}

var (
	repKeys        int64
	repErrors      int64
	namespaceNames string
	namespaceList  [][]byte
	nsCount        map[string]int64
	locker         sync.Mutex
)

const (
	MAX_RANGES = 256
)

func (d *OpData) Insert(key []byte, rec *db.Record) {

	r := OpRecord{
		key:                  string(key),
		lastModificationTime: rec.LastModificationTime,
		creationTime:         rec.CreationTime,
		version:              rec.Version,
		expirationTime:       rec.ExpirationTime,
		originatorRequestId:  rec.OriginatorRequestId,
	}

	r.payloadType, r.payloadData = rec.Payload.Clone()
	d.recordList = append(d.recordList, r)
}

func (d *OpData) SetData(zoneid, shardid int, mb *MessageBlock) {

	other := mb.opData
	if d.Rangeid >= other.Rangeid {
		return
	}

	if other.Rangeid == MAX_RANGES && GetReplicateCount() > 0 {
		glog.Infof("zoneid=%d shardid=%d replicateKeys=%d replicateErrors=%d",
			zoneid, shardid, GetReplicateCount(), GetReplicateErrors())
		LogMsg("\nzoneid=%d shardid=%d replicateKeys=%d replicateErrors=%d",
			zoneid, shardid, GetReplicateCount(), GetReplicateErrors())
		if zoneid == 0 {
			for k, v := range nsCount {
				glog.Infof("ns=%s count=%d", k, v)
			}
		}
	}

	*d = other
	mb.opData.recordList = nil
}

func (d *OpData) IsEmpty() bool {
	return len(d.recordList) == 0
}

func (d *OpData) Reset() {
	d.Rangeid = -1
	d.recordList = nil
}

func (d *OpData) FindAndReplicate(zoneid, rangeid int, k KeyList) bool {

	if d.Rangeid != k.Rangeid {
		return false
	}

	if d.IsEmpty() || len(k.Keys) == 0 {
		d.recordList = nil
		return true
	}

	m := make(map[string]int)
	for i := range k.Keys {
		m[k.Keys[i]] = 0
	}

	count := int64(0)
	fails := int64(0)
	now := uint32(time.Now().Unix() + 2)
	for i := range d.recordList {
		rec := d.recordList[i]
		_, found := m[rec.key]
		if !found || rec.expirationTime < now {
			continue
		}

		var req proto.OperationalMessage
		req.SetAsRequest()
		req.SetAsReplication()
		req.SetOpCode(proto.OpCodeUpdate)

		buf := []byte(d.recordList[i].key)
		ns, appkey, ok := GetNamespaceAndKey(buf)
		if !ok {
			continue
		}
		req.SetKey(appkey)
		req.SetNamespace([]byte(ns))
		req.SetLastModificationTime(rec.lastModificationTime)
		req.SetCreationTime(rec.creationTime)
		req.SetVersion(rec.version)
		req.SetExpirationTime(rec.expirationTime)
		req.SetNewRequestID()
		req.SetOriginatorRequestID(rec.originatorRequestId)
		var p proto.Payload
		p.SetPayload(rec.payloadType, rec.payloadData)
		req.SetPayload(&p)

		count++
		if (count % 10000) == 0 {
			UpdateDbAccessTime()
		}
		if !ReplicateRecord(&req) {
			fails++
		}
	}

	atomic.AddInt64(&repKeys, count)
	atomic.AddInt64(&repErrors, fails)
	d.recordList = nil
	return true
}

func GetReplicateCount() int64 {
	return atomic.LoadInt64(&repKeys)
}

func GetReplicateErrors() int64 {
	return atomic.LoadInt64(&repErrors)
}

func ResetReplicateCount() {
	atomic.StoreInt64(&repKeys, 0)
	atomic.StoreInt64(&repErrors, 0)
	nsCount = nil
}

func SetNamespaceNames(names string, trace bool) {
	namespaceNames = names
	ns := strings.Split(namespaceNames, "|")
	namespaceList = make([][]byte, len(ns))

	for i := range ns {
		namespaceList[i] = []byte(ns[i])
	}

	if trace {
		nsCount = make(map[string]int64)
	}
}

func GetNamespaceNames() string {
	return namespaceNames
}

func GetTrace() bool {
	return nsCount != nil
}

func IsCopyNamespace() bool {
	return len(namespaceNames) > 0
}

func MatchNamespace(zoneid int, key []byte) bool {

	if len(namespaceNames) == 0 {
		return true
	}
	if len(key) < 6 {
		return false
	}

	stop := 4 + int(uint8(key[3]))
	if len(key) < stop+1 {
		return false
	}
	namespace := key[4:stop]
	if zoneid == 0 && nsCount != nil {
		str := string(namespace)
		locker.Lock()
		nsCount[str] += 1
		locker.Unlock()
	}

	for i := range namespaceList {
		if bytes.Compare(namespace, namespaceList[i]) == 0 {
			return true
		}
	}

	return false
}
