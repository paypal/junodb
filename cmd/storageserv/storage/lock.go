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

package storage

import (
	"sync"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/storage/db"
	"juno/pkg/logging"
	"juno/pkg/proto"
	"juno/pkg/shard"
)

var (
	///TODO to use sharded sync.Map
	prepareMap []*sync.Map // sharded map
)

func InitializeCMap(numShards int) {
	prepareMap = make([]*sync.Map, numShards)
	for i := 0; i < numShards; i++ {
		prepareMap[i] = new(sync.Map) // it's a bit wast for now.
	}
}

func acquireLock(p *reqProcCtxT) (owner *reqProcCtxT, success bool) {
	req := &p.request

	shardId := p.recordId.GetShardID()
	recInMap, loaded := prepareMap[shardId].LoadOrStore(string(p.recordId), p)
	owner = recInMap.(*reqProcCtxT)

	if loaded { //exists in map
		if glog.LOG_DEBUG {
			glog.Debugf("prepare record exists. %s rid=%s", p.recordId, req.GetRequestID())
		}
		//	releasePData(pdata)
		if owner.request.GetRequestID().Equal(req.GetRequestID()) {
			success = true
		}
	} else {
		if glog.LOG_DEBUG {
			glog.Debugf("add new prepare record. %s rid=%s", p.recordId, req.GetRequestID())
		}
		if p != owner {
			panic("")
		}
		success = true
	}
	if glog.LOG_DEBUG {
		if success {
			if loaded {
				glog.Debugf("lock success (R). recId=%s,rid=%s", p.recordId, req.GetRequestID())
			} else {
				glog.Debugf("lock success. recId=%s,rid=%s", p.recordId, req.GetRequestID())
			}
		} else {
			glog.Debugf("lock fail. recId=%s,rid=%s,lockrid=%s", p.recordId, req.GetRequestID(), owner.request.GetRequestID())
		}
	}
	return
}

func releaseLock(owner *reqProcCtxT) {
	if owner == nil {
		glog.Errorf("nil pdata")
		return
	}
	if owner.recordId == nil {
		glog.Errorf("nil recordId")
		return
	}
	shardId := owner.recordId.GetShardID()
	if glog.LOG_DEBUG {
		glog.Debugf("unlock rid=%s", owner.request.GetRequestID())
	}
	if glog.LOG_VERBOSE {
		b := logging.NewKVBufferForLog()
		b.AddRequestID(owner.request.GetRequestID()).AddShardId(owner.request.GetShardId()).Add([]byte("recId"), owner.recordId.String())
		glog.Verbosef("Cleanup data & lock - %v", b)
	}
	prepareMap[shardId].Delete(string(owner.recordId))
}

func getFromPrepareMap(shardId shard.ID, recId db.RecordID, reqId proto.RequestId) (owner *reqProcCtxT, ok bool) {

	data, loaded := prepareMap[shardId].Load(string(recId))
	if loaded {
		owner = data.(*reqProcCtxT)
		if owner.request.GetRequestID().Equal(reqId) {
			ok = true
			return
		}
		owner = nil
	}
	return
}
