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
	"bytes"
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/dbscanserv/patch"
	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/redist"
	"juno/cmd/storageserv/storage/db"
	"juno/cmd/storageserv/watcher"
	"juno/pkg/cluster"
	"juno/pkg/debug"
	"juno/pkg/etcd"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
	"juno/pkg/shard"
	"juno/pkg/util"
)

var (
	shardMap shard.Map ///TODO to be removed. it does not seem to be used
	onceSe   sync.Once
)

func Initialize(args ...interface{}) (err error) {
	sz := len(args)
	if sz < 3 {
		err = fmt.Errorf("three arguments expected")
		glog.Error(err)
		return
	}
	var zoneId, machineId, lruCacheSizeInMB int
	var ok bool
	if zoneId, ok = args[0].(int); !ok {
		err = fmt.Errorf("zoneId of type int expected")
		glog.Error(err)
		return
	}
	if machineId, ok = args[1].(int); !ok {
		err = fmt.Errorf("machineId of type int expected")
		glog.Error(err)
		return
	}
	if lruCacheSizeInMB, ok = args[2].(int); !ok {
		err = fmt.Errorf("lruCacheSizeInMB of type int expected")
		glog.Error(err)
		return
	}
	initialize(zoneId, machineId, lruCacheSizeInMB)
	return
}

func Finalize() {
	Shutdown()
}

func initialize(zoneId int, machineId int, lruCacheSizeInMB int) {
	glog.Debugf("setting up storage engine ...")
	onceSe.Do(func() {
		glog.Verbosef("creating a storage engine instance.")
		shardMap = config.ServerConfig().NewShardMap(zoneId, machineId) ///TODO ...
		cfg := config.ServerConfig()

		InitializeCMap(int(cfg.ClusterInfo.NumShards))
		db.Initialize(int(cfg.ClusterInfo.NumShards), int(cfg.NumMicroShards),
			int(cfg.NumMicroShardGroups), int(cfg.NumPrefixDbs),
			zoneId, machineId, shardMap, lruCacheSizeInMB)

		glog.Infof("storage engine initialized")
	})

	etcdcli := etcd.GetEtcdCli()
	//	if etcdcli == nil {
	//		etcdcli = etcd.NewEtcdClient(cfg, clustername)
	//	}
	handler := SSRedistWatchHandler{zoneid: uint16(zoneId), nodeid: uint16(machineId), etcdcli: etcdcli}
	watcher.RegisterWatchEvtHandler(&handler)
	redist.RegisterDBRedistHandler(&handler)
}

func Shutdown() {
	glog.Infof("shutting down storage engine ...")
	db.GetDB().Shutdown()
}

func validate(r *proto.OperationalMessage) bool {
	shardid := shard.ID(r.GetShardId())
	opcode := r.GetOpCode()

	if r.IsRequestIDSet() == false {
		glog.Errorf("Bad Param: RequestId empty")
		if cal.IsEnabled() {
			cal.Event(kCalMsgTypeReqProc, "BadParam_empty_rid", cal.StatusSuccess, nil)
		}
		return false
	}

	if r.GetNamespace() == nil || len(r.GetNamespace()) <= 0 {

		glog.Error("Bad Param: Namespace is empty")
		if cal.IsEnabled() {
			cal.Event(kCalMsgTypeReqProc, "BadParam_empty_namespace", cal.StatusSuccess, nil)
		}
		return false
	}

	if r.GetKey() == nil || len(r.GetKey()) <= 0 {
		glog.Error("Bad Param: Key is empty")
		if cal.IsEnabled() {
			cal.Event(kCalMsgTypeReqProc, "BadParam_no_key", cal.StatusSuccess, nil)
		}
		return false
	}

	if r.GetTimeToLive() == 0 && opcode == proto.OpCodePrepareCreate {
		glog.Error("Bad Param: 0 TTL")
		if cal.IsEnabled() {
			cal.Event(kCalMsgTypeReqProc, "BadParam_0TTL_for_create", cal.StatusSuccess, nil)
		}
		return false
	}
	///TODO to enable later
	//	if r.IsForReplication() {
	//		if len(r.GetOriginatorRequestID()) == 0 {
	//			return errors.New("Bad Param, replication request should have originator request ID")
	//		}
	//	}
	// for create
	if r.IsForReplication() && r.GetOpCode() == proto.OpCodePrepareCreate &&
		(r.GetTimeToLive() == 0 || r.GetCreationTime() == 0 || r.GetVersion() == 0) {
		glog.Error("Bad Param, missing fields for replication")
		if cal.IsEnabled() {
			cal.Event(kCalMsgTypeReqProc, "BadParam_missing_data_for_replication", cal.StatusSuccess, nil)
		}
		return false
	}

	// for set/update replication
	if r.IsForReplication() &&
		(r.GetOpCode() == proto.OpCodePrepareSet || r.GetOpCode() == proto.OpCodePrepareUpdate) &&
		(r.GetCreationTime() == 0 || r.GetVersion() == 0) {
		glog.Error("Bad Param, missing fields for replication")
		if cal.IsEnabled() {
			cal.Event(kCalMsgTypeReqProc, "BadParam_missing_data_for_replication", cal.StatusSuccess, nil)
		}
		return false
	}

	ok := db.GetDB().ShardSupported(shardid)
	if !ok {
		glog.Errorf("bad Param: shard id, %d, is not owned by SS", shardid)
		if cal.IsEnabled() {
			cal.Event(kCalMsgTypeReqProc, "BadParam_invalid_shard_id", cal.StatusSuccess, nil)
		}
		return false
	}

	return true
}

func process(p *reqProcCtxT) {
	req := &p.request
	if glog.LOG_VERBOSE {
		b := logging.NewKVBufferForLog()
		b.AddReqIdString(req.GetRequestIDString()).AddHexKey(req.GetKey())

		glog.Verbosef("Process %s %v", req.GetOpCode().String(), b)
	}

	opcode := req.GetOpCode()
	if !opcode.IsForStorage() {
		p.replyWithErrorOpStatus(proto.OpStatusServiceDenied)
		return
	}

	if validate(&p.request) == false {
		p.replyWithErrorOpStatus(proto.OpStatusBadParam)
		return
	}

	switch opcode {
	case proto.OpCodePrepareCreate, proto.OpCodePrepareUpdate, proto.OpCodePrepareSet, proto.OpCodePrepareDelete:
		processTwoPC(p)
	case proto.OpCodeCommit:
		forwardCommit(p)
	case proto.OpCodeAbort:
		forwardAbort(p)
	case proto.OpCodeRead:
		read(p)
	case proto.OpCodeDelete:
		processDelete(p)
	case proto.OpCodeRepair:
		repair(p)
	case proto.OpCodeClone:
		clone(p)
	case proto.OpCodeMarkDelete:
		markDelete(p)
	default:
		// should never come here, but handle it anyway
		glog.Errorf("bad opcode: %s rid=%s", opcode.String(), req.GetRequestIDString())
		if cal.IsEnabled() {
			cal.Event(kCalMsgTypeReqProc, "unsupport_"+opcode.String(), cal.StatusSuccess, nil)
		}
		p.replyWithErrorOpStatus(proto.OpStatusServiceDenied)
		return
	}

	return
}

// Read: one phase operation
// Need to lock for extending ttl
func read(p *reqProcCtxT) {
	rec := &db.Record{}
	defer rec.ResetRecord()

	exist, err := db.GetDB().GetRecord(p.recordId, rec)
	if err != nil {
		p.replyWithErrorOpStatus(proto.OpStatusSSError)
		return
	}

	// NoKey
	if !exist || rec.IsExpired() {
		p.replyWithErrorOpStatus(proto.OpStatusNoKey)
		return
	}

	status := proto.OpStatusNoError
	if rec.IsMarkedDelete() {
		status = proto.OpStatusKeyMarkedDelete
	} else {
		if p.request.GetExpirationTime() > 0 && p.request.GetExpirationTime() > rec.ExpirationTime {

			if pdata, ok := acquireLock(p); ok && pdata == p { ///TODO check if pdata == p??>>?>
				trec := *rec
				trec.ExpirationTime = p.request.GetExpirationTime()
				///TODO to change
				if werr := dbPutWrapper(p, &trec); werr != nil {
					status = proto.OpStatusSSReadTTLExtendErr
				}
				releaseLock(pdata) /// TODO
				rec.ExpirationTime = trec.ExpirationTime
				//				rec.LastModificationTime = trec.LastModificationTime
			} else {
				status = proto.OpStatusSSReadTTLExtendErr
			}
		}
	}

	/// TODO to change
	p.initResponse(status, rec.Version, rec.ExpirationTime, rec.CreationTime)
	p.response.SetPayload(&rec.Payload)
	p.response.SetOriginatorRequestID(rec.OriginatorRequestId)
	p.response.SetLastModificationTime(rec.LastModificationTime)
	p.reply()
	return
}

// Repair: one phase operation
func repair(p *reqProcCtxT) {
	request := &p.request

	pdata, ok := acquireLock(p)
	if !ok || pdata != p {
		p.replyWithErrorOpStatus(proto.OpStatusRecordLocked)
		return
	}

	reqId := pdata.request.GetRequestID()

	rec := db.Record{
		RecordHeader: db.RecordHeader{
			RequestId:            reqId,
			Version:              request.GetVersion(),
			ExpirationTime:       request.GetExpirationTime(),
			CreationTime:         request.GetCreationTime(),
			OriginatorRequestId:  request.GetOriginatorRequestID(),
			LastModificationTime: request.GetLastModificationTime(),
		},
		Payload: *request.GetPayload(),
	}
	if request.GetFlags().IsFlagMarkDeleteSet() {
		rec.MarkDelete()
	}
	if err := dbPutWrapper(p, &rec); err != nil {
		glog.Error(err)
		releaseLock(pdata)
		p.replyWithErrorOpStatus(proto.OpStatusSSError)
		return
	}

	///TODO to change
	p.initResponseWithStatus(proto.OpStatusNoError)
	releaseLock(pdata)
	p.reply()

	return
}

// Clone: one phase operation used during data redistribution
// the difference between clone and repair:
// - repair does blind overwrite
// - clone need to compare version etc, overwrite only if necessary
func clone(p *reqProcCtxT) {
	request := &p.request
	recId := p.recordId
	pdata, ok := acquireLock(p)
	if !ok || pdata != p {
		p.replyWithErrorOpStatus(proto.OpStatusRecordLocked)
		return
	}

	reqId := pdata.request.GetRequestID() //preq.GetRequestID()

	rec := db.Record{
		RecordHeader: db.RecordHeader{
			RequestId:            reqId,
			Version:              request.GetVersion(),
			ExpirationTime:       request.GetExpirationTime(),
			CreationTime:         request.GetCreationTime(),
			LastModificationTime: request.GetLastModificationTime(),
			OriginatorRequestId:  request.GetOriginatorRequestID(),
		},
		Payload: *request.GetPayload(),
	}
	dbrec := &p.dbRec

	present, err := db.GetDB().IsRecordPresent(recId, dbrec)
	if err != nil {
		releaseLock(pdata)
		glog.Error(err)
		p.replyWithErrorOpStatus(proto.OpStatusSSError)
		return
	}

	p.dbRecExist = present
	if present {
		// TODO!!!
		if isConflict(request, dbrec) {
			releaseLock(pdata)
			p.initResponse(proto.OpStatusVersionConflict, dbrec.Version, dbrec.ExpirationTime, dbrec.CreationTime)
			p.reply()
			return
		}
	}

	// if not present in db or version check is ok, write to db
	if err = dbPutWrapper(p, &rec); err != nil {
		releaseLock(pdata)
		glog.Error(err)
		p.replyWithErrorOpStatus(proto.OpStatusSSError)
		return
	}

	releaseLock(pdata)
	p.initResponseWithStatus(proto.OpStatusNoError)
	p.reply()
	return
}

// Delete: one phase operation for now
func processDelete(p *reqProcCtxT) {
	request := &p.request
	recId := p.recordId
	shardId := p.shardId
	pdata, ok := acquireLock(p)
	if !ok || pdata != p {
		p.replyWithErrorOpStatus(proto.OpStatusRecordLocked)
		return
	}

	rec := &p.dbRec

	present, err := db.GetDB().IsRecordPresent(recId, rec)
	if err != nil {
		releaseLock(pdata)
		p.replyWithErrorOpStatus(proto.OpStatusSSError)
		return
	}

	p.dbRecExist = present
	if !present {
		releaseLock(pdata)
		p.replyWithErrorOpStatus(proto.OpStatusNoKey)
		return
	}
	if request.IsForReplication() {
		if present { //check conflict only if present
			if isConflict(request, rec) {
				p.initResponse(proto.OpStatusVersionConflict, rec.Version, rec.ExpirationTime, rec.CreationTime)
				releaseLock(pdata)
				p.reply()
				return
			}
		}
	}

	if err := dbDeleteRecord(request, shardId, recId, rec); err != nil {
		releaseLock(pdata)
		p.replyWithErrorOpStatus(proto.OpStatusSSError)
		return
	}
	p.initResponse(proto.OpStatusNoError, rec.Version, rec.ExpirationTime, rec.CreationTime)
	releaseLock(pdata)
	p.reply()
}

func toWaitForPhaseTwoRequest(st proto.OpStatus, opcode proto.OpCode) bool {

	if st == proto.OpStatusNoKey && opcode == proto.OpCodePrepareDelete {
		return true
	}
	if st != proto.OpStatusNoError &&
		st != proto.OpStatusAlreadyFulfilled &&
		st != proto.OpStatusInserting {
		return false
	}
	return true
}

func processTwoPC(p *reqProcCtxT) {
	req := &p.request
	recId := p.recordId
	pdata, ok := acquireLock(p)
	if ok {
		if pdata != p {
			if pdata.chReq != nil && pdata.dbRecExist {
				rec := &pdata.dbRec
				///TODO
				p.initResponse(proto.OpStatusAlreadyFulfilled, rec.Version, rec.ExpirationTime, rec.CreationTime)
				p.reply()
			} else {
				p.replyWithErrorOpStatus(proto.OpStatusAlreadyFulfilled)
			}
			return
		}
	} else {
		if glog.LOG_DEBUG {
			glog.Debugf("rec locked - shardId: %d, recId: %X, reqId: %s",
				req.GetShardId(), recId, req.GetRequestIDString())
		}
		p.replyWithErrorOpStatus(proto.OpStatusRecordLocked)
		return
	}
	if debug.DEBUG {
		if p != pdata {
			p.request.PrettyPrint(os.Stdout)

			pdata.request.PrettyPrint(os.Stdout)
			panic("")
		}
	}
	var err error
	if p.dbRecExist, err = db.GetDB().IsRecordPresent(recId, &p.dbRec); err == nil {
		p.chReq = make(chan *reqProcCtxT, 1)
		p.timer.Reset(config.ServerConfig().RecLockExpiration.Duration)
	} else {
		glog.Error(err)
		p.replyWithErrorOpStatus(proto.OpStatusSSError)
		return
	}
	p.cacheable = false

	opcode := req.GetOpCode()
	isDelete := (opcode == proto.OpCodePrepareDelete) && p.dbRecExist
	prepareStatus := proto.OpStatusNoError
	switch opcode {
	case proto.OpCodePrepareCreate:
		prepareStatus = prepareCreate(p)
	case proto.OpCodePrepareUpdate, proto.OpCodePrepareSet:
		prepareStatus = prepareUpdate(p)
	case proto.OpCodePrepareDelete:
		prepareStatus = prepareDelete(p)
	default:
		// should never come here, but handle it anyway
		glog.Errorf("bad opcode: %d", opcode)
		releaseLock(pdata)
		p.replyWithErrorOpStatus(proto.OpStatusServiceDenied)
		return
	}

	// return if no need to wait for Phase II (commit/abort)
	if toWaitForPhaseTwoRequest(prepareStatus, opcode) == false {
		releaseLock(pdata)
		return
	}

	if debug.DEBUG {
		if opcode != p.request.GetOpCode() {
			msg := fmt.Sprintf("opcode: %s req_opcode: %s", opcode.String(), p.request.GetOpCodeText())
			glog.Error(msg)
			cal.Event("SSReqProcError", "corrupt", cal.StatusSuccess, []byte(msg))
			panic("")
		}
	}
	// wait for phase II request
	var p2reqctx *reqProcCtxT
	select {
	case <-pdata.timer.GetTimeoutCh():
		if glog.LOG_VERBOSE {
			glog.Verbosef("twopc timer: waiting for phase II request timed out")
		}
		select {
		case p2reqctx, ok = <-pdata.chReq:
		default:
			// no msg
		}

	case p2reqctx, ok = <-pdata.chReq:
		if glog.LOG_VERBOSE {
			glog.Verbosef("twopc timer: stopped")
		}
		pdata.timer.Stop()
	}

	if !ok || p2reqctx == nil {
		// timed out or channel closed
		releaseLock(pdata)
		p.dbRec.ResetRecord()
		return
	}

	p2reqctx.prepareCtx = p
	if debug.DEBUG {
		if opcode != p.request.GetOpCode() {
			msg := fmt.Sprintf("opcode: %s req_opcode: %s",
				opcode.String(), p.request.GetOpCodeText())

			glog.Error(msg)
			cal.Event("SSReqProcError", "corrupt", cal.StatusSuccess, []byte(msg))
			panic("")
		}
	}

	// handle commit/abort
	opcode = p2reqctx.request.GetOpCode()
	switch opcode {
	case proto.OpCodeCommit:
		if isDelete { // for namespace migration
			patch.RelayDelete(req.GetNamespace(), recId.GetKey(), &p.dbRec)
		}
		doCommitAndReleaselock(p2reqctx, pdata)
	case proto.OpCodeAbort:
		// abort does nothing except releasing lock & returning ok
		p2reqctx.initResponseWithStatus(proto.OpStatusNoError)
		releaseLock(pdata)
		p2reqctx.reply()
	case proto.OpCodeMarkDelete:
		if isDelete { // for namespace migration
			patch.RelayDelete(req.GetNamespace(), recId.GetKey(), &p.dbRec)
		}
		doMarkDeleteAndReleaseLock(p2reqctx, pdata)
	default:
		// should never come here, but handle it anyway
		glog.Errorf("bad phase II opcode: %d", opcode)
		releaseLock(pdata)
		p2reqctx.replyWithErrorOpStatus(proto.OpStatusServiceDenied)
	}

	// ideal sequence: release lock, reply to client,
	// and relesase memory for both Phase I & II requests.
}

func prepareCreate(p *reqProcCtxT) proto.OpStatus {

	rec := &p.dbRec

	st := proto.OpStatusNoError
	if p.dbRecExist {
		p.dbRecExist = true
		///TODO check if rec.IsExpired() gets called
		if rec.IsMarkedDelete() {
			st = proto.OpStatusInserting
			p.dbRecExist = false
			p.dbRec.ResetRecord()
		} else {
			reqId := p.request.GetRequestID()
			if rec.RequestId.Equal(reqId) {
				st = proto.OpStatusAlreadyFulfilled
				p.initResponse(st, ///TODO to change
					rec.Version, rec.ExpirationTime, rec.CreationTime)
				p.reply()
			} else {
				st = proto.OpStatusDupKey
				p.initResponse(st, ///TODO to change
					rec.Version, rec.ExpirationTime, rec.CreationTime)
				p.response.SetOriginatorRequestID(rec.OriginatorRequestId)
				p.reply()
			}
			return st
		}
	}
	p.initResponse(st, 0, 0, 0) ///TODO to change
	p.reply()
	return st
}

func prepareSet(p *reqProcCtxT) (st proto.OpStatus) {

	rec := &p.dbRec

	versionToReturn := uint32(0)

	if p.dbRecExist {
		reqId := p.request.GetRequestID()
		if rec.RequestId.Equal(reqId) {
			st = proto.OpStatusAlreadyFulfilled
			p.initResponse(st, ///TODO to change
				rec.Version, rec.ExpirationTime, rec.CreationTime)
			p.reply()
			return
		}
		if rec.IsMarkedDelete() {
			versionToReturn = rec.Version
			st = proto.OpStatusInserting
			p.initResponse(st, rec.Version, ///TODO to change
				rec.ExpirationTime, rec.CreationTime)
			p.response.SetLastModificationTime(rec.LastModificationTime)
			p.response.SetOriginatorRequestID(rec.OriginatorRequestId)
			p.reply()
			return
		} else {
			p.initResponse(proto.OpStatusNoError, rec.Version, rec.ExpirationTime, rec.CreationTime)
			p.response.SetOriginatorRequestID(rec.OriginatorRequestId)
			p.response.SetLastModificationTime(rec.LastModificationTime)
			p.reply()
			return
		}
	}
	p.initResponse(proto.OpStatusInserting, versionToReturn,
		p.request.GetExpirationTime(), p.request.GetCreationTime())
	p.reply()
	st = proto.OpStatusInserting
	return
}

func prepareDelete(p *reqProcCtxT) (st proto.OpStatus) {

	rec := &p.dbRec

	if p.dbRecExist && !rec.IsExpired() {
		request := &p.request
		reqId := request.GetRequestID()
		if rec.RequestId.Equal(reqId) {
			st = proto.OpStatusAlreadyFulfilled
			p.initResponse(st,
				rec.Version, rec.ExpirationTime, rec.CreationTime)
			p.reply()
			return
		}
		if request.IsForReplication() && !rec.IsMarkedDelete() {
			if isConflict(request, rec) {
				p.initResponse(proto.OpStatusVersionConflict, rec.Version, rec.ExpirationTime, rec.CreationTime)
				p.reply()
				return
			}
		}
		st = proto.OpStatusNoError
		p.initResponse(st, rec.Version, rec.ExpirationTime, rec.CreationTime)
		p.response.SetOriginatorRequestID(rec.OriginatorRequestId)
		p.response.SetLastModificationTime(rec.LastModificationTime)
		p.response.SetExpirationTime(rec.ExpirationTime)
		p.reply()
		return
	} else {
		st = proto.OpStatusNoKey
		p.replyWithErrorOpStatus(proto.OpStatusNoKey)
	}
	return
}

func prepareUpdate(p *reqProcCtxT) proto.OpStatus {

	if p.request.GetVersion() > 0 {
		// version is given, so it's a conditional update
		return prepareCUpdate(p)
	}

	return prepareSet(p)
}

func prepareCUpdate(p *reqProcCtxT) (st proto.OpStatus) {
	rec := &p.dbRec
	if p.request.IsForReplication() {
		if p.dbRecExist {
			if rec.IsMarkedDelete() {
				p.dbRecExist = false
				p.dbRec.ResetRecord()
				st = proto.OpStatusInserting
				p.initResponse(st, rec.Version, rec.ExpirationTime, rec.CreationTime)
				p.response.SetOriginatorRequestID(rec.OriginatorRequestId)
				p.response.SetLastModificationTime(rec.LastModificationTime)
				p.reply()
				return
			}
			if p.request.IsForDeleteReplication() { // for namespace migration
				if patch.DeleteNeeded(&p.request, rec) {
					err := db.GetDB().Delete(p.recordId)
					if err != nil {
						glog.Errorf("%s", err)
					}
				}
				st = proto.OpStatusVersionConflict
				p.initResponse(st, rec.Version, rec.ExpirationTime, rec.CreationTime)
				p.reply()
				return
			}

			if isConflict(&p.request, rec) {
				st = proto.OpStatusVersionConflict
				p.initResponse(st, rec.Version, rec.ExpirationTime, rec.CreationTime)
				p.reply()
				return
			}
			st = proto.OpStatusNoError
			p.initResponse(st, p.request.GetVersion(), p.request.GetExpirationTime(), p.request.GetCreationTime())
			p.response.SetOriginatorRequestID(rec.OriginatorRequestId)
			p.reply()
			return

		} else { // rec does not exist
			if p.request.IsForDeleteReplication() { // noop
				st = proto.OpStatusVersionConflict
				p.initResponse(st, rec.Version, rec.ExpirationTime, rec.CreationTime)
				p.reply()
				return
			}
			st = proto.OpStatusInserting
			p.initResponse(st, 0, p.request.GetExpirationTime(), p.request.GetCreationTime())
			p.reply()
			return
		}
	} else {
		if !p.dbRecExist {
			st = proto.OpStatusInserting
			p.initResponse(st, uint32(0), p.request.GetExpirationTime(), p.request.GetCreationTime())
			p.reply()
			return
		}
		if rec.IsMarkedDelete() {
			p.dbRecExist = false
			p.dbRec.ResetRecord()
			st = proto.OpStatusInserting
			p.initResponse(st, 0, p.request.GetExpirationTime(), p.request.GetCreationTime())
			p.reply()
			return
		}
		if p.request.GetVersion() < rec.Version {
			st = proto.OpStatusVersionConflict
			p.initResponse(st, rec.Version, rec.ExpirationTime, rec.CreationTime)
			p.response.SetPayload(&rec.Payload)
			p.reply()
			return
		}
		if p.request.GetCreationTime() != 0 && p.request.GetCreationTime() != rec.CreationTime {
			glog.Warningf("not same creationTime. %d - %d", p.request.GetCreationTime(), rec.CreationTime)
			st = proto.OpStatusVersionConflict
			p.initResponse(st, rec.Version, rec.ExpirationTime, rec.CreationTime)
			p.response.SetPayload(&rec.Payload)
			p.reply()
			return
		}
		if p.request.GetOriginatorRequestID().IsSet() {
			if !p.request.GetOriginatorRequestID().Equal(rec.OriginatorRequestId) {
				glog.Warningf("not same oid. prep oid: %s  rec.oid: %s", p.request.GetOriginatorRequestID(),
					rec.OriginatorRequestId.String())
				st = proto.OpStatusVersionConflict
				p.initResponse(st, rec.Version, rec.ExpirationTime, rec.CreationTime)
				p.response.SetPayload(&rec.Payload)
				p.reply()
				return
			}
		}
		st = proto.OpStatusNoError
		p.initResponse(st, rec.Version, rec.ExpirationTime, rec.CreationTime)
		p.response.SetOriginatorRequestID(rec.OriginatorRequestId)
		p.reply()
		return
	}
}

func forwardAbort(p *reqProcCtxT) {
	shardId := p.shardId
	recId := p.recordId

	pdata, present := getFromPrepareMap(shardId, recId, p.request.GetRequestID())
	if !present {
		p.replyWithErrorOpStatus(proto.OpStatusNoUncommitted)
		return
	}

	if err := forwardRequest(p, pdata); err != nil {
		logForwardRequestError(p, err)
		p.replyWithErrorOpStatus(proto.OpStatusNoUncommitted)
		return
	}
	// if request is forwarded, no need to send response back from this go routine.
}

func logForwardRequestError(p *reqProcCtxT, err error) {
	glog.ErrorDepth(1, err)
	if cal.IsEnabled() {
		b := logging.NewKVBuffer()
		b.AddOpRequest(&p.request)
		b.Add([]byte("err"), err.Error())
		cal.Event(kCalMsgTypeReqProc, "fail_to_forward_commit", cal.StatusSuccess, b.Bytes())
	}
}

func forwardCommit(p *reqProcCtxT) {
	req := &p.request
	shardId := p.shardId
	recId := p.recordId

	pdata, present := getFromPrepareMap(shardId, recId, req.GetRequestID())
	// forward commit if pdata exist
	if present && (pdata != nil) {
		glog.Debugf("commit. pdata opcode=%s", pdata.request.GetOpCode())
		if err := forwardRequest(p, pdata); err == nil {
			return
		} else {
			logForwardRequestError(p, err)
		}
	}

	// otherwise, check db
	dbRec := &p.dbRec
	presentindb, e := db.GetDB().IsRecordPresent(recId, dbRec)
	if e != nil {
		glog.Error(e)
		p.replyWithErrorOpStatus(proto.OpStatusSSError)
		return
	}

	if presentindb {
		if dbRec.RequestId.Equal(req.GetRequestID()) {
			p.initResponse(proto.OpStatusAlreadyFulfilled,
				dbRec.Version, dbRec.ExpirationTime, dbRec.CreationTime)
			p.reply()
			return
		}
	}
	p.initResponseWithStatus(proto.OpStatusNoUncommitted)
	p.reply()
	return
}

func markDelete(p *reqProcCtxT) {
	req := &p.request
	shardId := p.shardId
	recId := p.recordId

	pdata, present := getFromPrepareMap(shardId, recId, req.GetRequestID())
	// forward if pdata exists -- markDelete for prepareDelete
	if present && (pdata != nil) {
		if err := forwardRequest(p, pdata); err == nil {
			return
		} else {
			if err.Error() == "missing twopc info" {
				// This record could be locked by one-pc operation like: Clone or another MarkDelete
				glog.Errorf("markdelete, record locked: %d, %s, %+v", shardId, recId, req.GetRequestID())
				p.replyWithErrorOpStatus(proto.OpStatusRecordLocked)
				return
			} else {
				logForwardRequestError(p, err)
			}
		}
	}

	// otherwise, perform the markdelete as one phase operation
	pdata, ok := acquireLock(p)
	if !ok || pdata != p {
		p.replyWithErrorOpStatus(proto.OpStatusRecordLocked)
		return
	}

	rec := &p.dbRec

	presentindb, err := db.GetDB().IsRecordPresent(recId, rec)
	if err != nil {
		releaseLock(pdata)
		glog.Error(err)
		p.replyWithErrorOpStatus(proto.OpStatusSSError)
		return
	}

	if presentindb {
		if rec.RequestId.Equal(req.GetRequestID()) && rec.IsMarkedDelete() {
			p.initResponse(proto.OpStatusAlreadyFulfilled,
				rec.Version, rec.ExpirationTime, rec.CreationTime)
			releaseLock(pdata)
			p.reply()
			return
		} else {
			rec.MarkDelete()
			rec.LastModificationTime = uint64(time.Now().UnixNano())
			if req.GetVersion() != 0 {
				rec.Version = req.GetVersion()
			} else {
				rec.Version++
			}
			rec.RequestId = req.GetRequestID()

			rec.Payload.Clear()
			if err = dbPutWrapper(p, rec); err != nil {
				releaseLock(pdata)
				glog.Error(err)
				p.replyWithErrorOpStatus(proto.OpStatusSSError)
				return
			}
			releaseLock(pdata)
			p.initResponseWithStatus(proto.OpStatusNoError)
			p.reply()
		}
	} else {
		if len(req.GetOriginatorRequestID()) == 0 {
			releaseLock(p)
			p.replyWithErrorOpStatus(proto.OpStatusBadParam)
			return
		}
		//TODO more validation
		rec.OriginatorRequestId = req.GetOriginatorRequestID()
		rec.CreationTime = req.GetCreationTime()
		rec.Version = req.GetVersion()
		if rec.Version == 0 {
			rec.Version = 1
		}
		rec.RequestId = req.GetRequestID()
		rec.ExpirationTime = util.GetExpirationTime(req.GetTimeToLive())
		rec.LastModificationTime = uint64(time.Now().UnixNano())
		rec.MarkDelete()
		if err = dbPutWrapper(p, rec); err != nil {
			releaseLock(pdata)
			glog.Error(err)
			p.replyWithErrorOpStatus(proto.OpStatusSSError)
			return
		}
		releaseLock(pdata)
		p.initResponseWithStatus(proto.OpStatusNoError)
		p.reply()
	}
	//	resp = newResponseWithOpStatus(req, proto.OpStatusNoUncommitted)
	return
}

// This is the phase two of preparedelete
func doMarkDeleteAndReleaseLock(p *reqProcCtxT, prepare *reqProcCtxT) {
	req := &p.request

	//	if pdata.preq.GetOpCode() != proto.OpCodePrepareDelete {
	//		resp = newResponseWithOpStatus(req, proto.OpStatusBadParam)
	//		return resp, nil
	//	}
	if prepare.chReq == nil {
		releaseLock(prepare)
		p.replyWithErrorOpStatus(proto.OpStatusNoError)
		return
	}
	rec := &prepare.dbRec
	if rec == nil {
		rec = &db.Record{
			RecordHeader: db.RecordHeader{
				Version:              req.GetVersion(),
				CreationTime:         req.GetCreationTime(),
				LastModificationTime: req.GetLastModificationTime(),
				ExpirationTime:       req.GetExpirationTime(),
				OriginatorRequestId:  req.GetOriginatorRequestID(),
				RequestId:            req.GetRequestID(),
			},
		}
	} else {
		rec.RequestId = req.GetRequestID()
		rec.Version++
		rec.LastModificationTime = uint64(time.Now().UnixNano())
		rec.Payload.Clear()
	}

	rec.MarkDelete()
	if err := dbPutWrapper(p, rec); err != nil {
		releaseLock(prepare)
		p.replyWithErrorOpStatus(proto.OpStatusSSError)
		return
	}

	///TODO
	p.initResponse(proto.OpStatusNoError, rec.Version, rec.ExpirationTime, rec.CreationTime)
	p.response.SetLastModificationTime(rec.LastModificationTime)
	p.response.SetOriginatorRequestID(rec.OriginatorRequestId)
	p.response.SetExpirationTime(rec.ExpirationTime)
	releaseLock(prepare)
	p.reply()
	return
}

func doCommitAndReleaselock(p *reqProcCtxT, prepare *reqProcCtxT) {

	req := &p.request
	recId := p.recordId
	shardId := p.shardId
	prepOpCode := prepare.request.GetOpCode()

	if prepOpCode != proto.OpCodePrepareDelete {
		if req.GetCreationTime() <= 0 {
			releaseLock(prepare)
			p.replyWithErrorOpStatus(proto.OpStatusBadParam)
			msg := fmt.Sprintf("rid=%s", req.GetRequestIDString())
			glog.Errorf("Bad Param: no creation time. %s", msg)
			if cal.IsEnabled() {
				cal.Event(kCalMsgTypeReqProc, "BadParam_0_creation_time", cal.StatusSuccess, []byte(msg))
			}
			return
		}

		if prepare.IsForInserting() == true && req.GetExpirationTime() <= util.Now() {
			releaseLock(prepare)
			p.replyWithErrorOpStatus(proto.OpStatusBadParam)
			msg := fmt.Sprintf("rid=%s,exptime=%d", req.GetRequestIDString(), req.GetExpirationTime())
			glog.Errorf("BadParam: bad expiration time. %s", msg)
			if cal.IsEnabled() {
				cal.Event(kCalMsgTypeReqProc, "BadParam_expired", cal.StatusSuccess, []byte(msg))
			}
			return
		}
	}

	rec := &prepare.dbRec
	//	prepare.GetCommitRecord(req, rec)

	rec.RequestId = req.GetRequestID()
	rec.CreationTime = req.GetCreationTime()
	rec.Version = req.GetVersion()
	rec.LastModificationTime = req.GetLastModificationTime()
	rec.Payload.Set(prepare.request.GetPayload())
	//rec.ExpirationTime = d.request.GetExpirationTime()
	//rec.RequestId = d.request.GetRequestID()
	if req.IsOriginatorSet() {
		rec.OriginatorRequestId = req.GetOriginatorRequestID()
	} else {
		if prepare.chReq != nil {
			if !prepare.dbRecExist {
				rec.OriginatorRequestId = rec.RequestId
			}
		}
	}

	if req.GetExpirationTime() > rec.ExpirationTime && req.GetExpirationTime() > util.Now() {
		rec.ExpirationTime = req.GetExpirationTime()
	}

	if rec.IsMarkedDelete() {
		rec.ClearMarkedDelete()
	}

	switch prepOpCode {

	case proto.OpCodePrepareCreate, proto.OpCodePrepareUpdate, proto.OpCodePrepareSet:
		if err := dbPutWrapper(p, rec); err != nil {
			releaseLock(prepare)
			p.replyWithErrorOpStatus(proto.OpStatusSSError)
			return
		}

	case proto.OpCodePrepareDelete: // to be used later
		//		rec.CreationTime = pdata.twopc.curRec.CreationTime
		if err := dbDeleteRecord(&p.request, shardId, recId, rec); err != nil {
			releaseLock(prepare)
			p.replyWithErrorOpStatus(proto.OpStatusSSError)
			return
		}

	default:
		var errmsg bytes.Buffer
		errmsg.WriteString("wrong opcode ")
		errmsg.WriteString(prepOpCode.String())
		errmsg.WriteString("\nprepare request:\n")
		prepare.request.PrettyPrint(&errmsg)
		errmsg.WriteString("\nrequest:\n")
		p.request.PrettyPrint(&errmsg)
		glog.Error(errmsg.String())
		releaseLock(prepare)
		p.replyWithErrorOpStatus(proto.OpStatusServiceDenied)
		return
	}

	///TODO revisit
	p.initResponse(proto.OpStatusNoError, rec.Version, rec.ExpirationTime, rec.CreationTime)
	p.response.SetOriginatorRequestID(rec.OriginatorRequestId)
	p.response.SetLastModificationTime(rec.LastModificationTime)
	p.response.SetExpirationTime(rec.ExpirationTime)
	releaseLock(prepare)
	p.reply()
	return
}

func TruncateExpired() {
	db.GetDB().TruncateExpired()
}

// TODO: revisit...
//Used to detect replication conflict. Not for conditional update
func isConflict(request *proto.OperationalMessage, rec *db.Record) (conflict bool) {
	lmt := request.GetLastModificationTime()
	if lmt != 0 {
		return lmt < rec.LastModificationTime
	}
	// for conflict checking, last modification time should be sufficient.
	// the following code is for handling mayfly request, which does not have modification time

	ct := request.GetCreationTime()
	recCt := rec.CreationTime

	if ct < recCt {
		return true
	} else if ct == recCt {
		ver := request.GetVersion()
		recVer := rec.Version

		if ver < recVer {
			return true
		} else if ver == recVer {
			opcode := request.GetOpCode()
			if opcode != proto.OpCodeDelete && opcode != proto.OpCodePrepareDelete {
				if util.GetExpirationTimeFrom(time.Now(), request.GetTimeToLive()) < rec.ExpirationTime {
					return true
				}
			}
		} else { //ver > recVer
			// this block is temporarily added for pool migration from mayfly to juno.
			// it could have problem updating stale record in the SS coming back from
			// a period of down time
			if ver-recVer > 32768 {
				return true
			}
		}
	}

	return false
}

func forwardRequest(p2 *reqProcCtxT, prepare *reqProcCtxT) error {
	if prepare.chReq == nil {
		return errors.New("missing twopc info")
	}

	///TODO make sure d.timer not nil
	if prepare.timer.IsStopped() {
		return errors.New("pdata exipred")
	}
	// channel buffer size 1, should never block
	select {
	case prepare.chReq <- p2:
	default:
		// should never happen
		glog.Debugf("result channel busy, should never happen")
		return errors.New("chReq busy")
	}
	return nil
}

// TODO: need a version for delete
// Wrapper of db.Put & Forward
func dbPutWrapper(p *reqProcCtxT, rec *db.Record) (err error) {
	szBuf := rec.EncodingSize()
	//	pool := util.GetBufferPool(szBuf)
	//	buf := pool.Get()
	//	buf.Resize(szBuf)
	p.encodeBuf.Grow(szBuf)
	p.encodeBuf.Reset()
	//rec.PrettyPrint(os.Stdout)
	if err = rec.EncodeToBuffer(&p.encodeBuf); err != nil {
		//		pool.Put(buf)
		return
	}

	//util.HexDump(p.encodeBuf.Bytes())
	err = db.GetDB().Put(p.recordId, p.encodeBuf.Bytes())
	//	pool.Put(buf)
	if err != nil {
		return
	}

	// Forwarding if needed
	if redist.IsEnabled() == false {
		return nil
	}

	mgr := redist.GetManager()
	if mgr == nil {
		return nil
	}

	var frMsg proto.RawMessage
	rec.EncodeRedistMsg(p.shardId, []byte(p.request.GetNamespace()), p.request.GetKey(), &frMsg)
	mgr.Forward(p.shardId, &frMsg, true, true)
	return nil
}

func dbDeleteRecord(request *proto.OperationalMessage, shardId shard.ID,
	recordId db.RecordID, rec *db.Record) (err error) {

	err = db.GetDB().Delete(recordId)

	if err != nil || redist.IsEnabled() == false || rec == nil {
		return
	}

	if mgr := redist.GetManager(); mgr != nil {
		opMsg := &proto.OperationalMessage{}
		opMsg.SetRequest(proto.OpCodeMarkDelete, request.GetKey(), request.GetNamespace(), nil, util.GetTimeToLive(rec.ExpirationTime))
		opMsg.SetCreationTime(rec.CreationTime)
		opMsg.SetLastModificationTime(rec.LastModificationTime)
		opMsg.SetOriginatorRequestID(rec.OriginatorRequestId)
		opMsg.SetRequestID(request.GetRequestID())
		opMsg.SetShardId(request.GetShardId())
		opMsg.SetVersion(rec.Version + 1)
		if request.IsForReplication() { //TODO revisit
			opMsg.SetAsReplication()
		}
		var raw proto.RawMessage
		err = opMsg.Encode(&raw)
		if err == nil {
			mgr.Forward(shardId, &raw, true, true)
		} else {
			//TODO log
		}
	}
	return
}

func ReplicateSnapshot(shardId shard.ID, rb *redist.Replicator, mshardid int32) bool {
	return db.GetDB().ReplicateSnapshot(shardId, rb, mshardid)
}

type SSRedistWatchHandler struct {
	zoneid  uint16
	nodeid  uint16
	etcdcli *etcd.EtcdClient
}

func (h *SSRedistWatchHandler) UpdateShards(shards shard.Map) bool {
	db.GetDB().UpdateShards(shards)
	return true
}

func (h *SSRedistWatchHandler) UpdateRedistShards(shards shard.Map) bool {
	db.GetDB().UpdateRedistShards(shards)
	return true
}

func (h *SSRedistWatchHandler) SendRedistSnapshot(shardId shard.ID, rb *redist.Replicator, mshardid int32) bool {
	return ReplicateSnapshot(shardId, rb, mshardid)
}

func (r *SSRedistWatchHandler) RedistStart(ratelimit int) {
	if redist.IsEnabled() {
		glog.Warningf("redistribution is in progress, ignore new change")
		return
	}

	// get clusterinfo
	rw := etcd.GetClsReadWriter()
	if rw == nil {
		glog.Info("no etcd reader")
		return
	}

	// clster contains new node info
	var clster cluster.Cluster
	_, err := clster.ReadWithRedistInfo(rw)
	if err != nil {
		glog.Infof("nodeinfo, %s", err.Error())
		return
	}

	changeMap, err := rw.ReadRedistChangeMap(int(r.zoneid), int(r.nodeid))
	if err != nil {
		// if changemap is empty, no need to do anything
		glog.Infof("changemap, %s", err.Error())
		return
	}

	glog.Infof("redist process start")
	glog.Infof("redist nodeinfo: %#v, change map: %v", clster.ConnInfo[r.zoneid], changeMap)

	mgr, err := redist.NewManager(r.zoneid, r.nodeid, clster.ConnInfo[r.zoneid],
		changeMap, &redist.RedistConfig, r.etcdcli, ratelimit)

	if err != nil {
		return
	}

	// enable the new node
	//
	redist.SetManager(mgr)
	redist.SetEnable(true)
}

func (r *SSRedistWatchHandler) RedistResume(ratelimit int) {
	if !redist.IsEnabled() {
		glog.Infof("Redist not Enabled, ignore resume")
		return
	}
	glog.Infof("Redist Enabled")

	mgr := redist.GetManager()
	if mgr != nil {
		glog.Infof("redist resumed")
		mgr.Resume(ratelimit)
	}
}

func (r *SSRedistWatchHandler) RedistStop() {
	if !redist.IsEnabled() {
		return
	}

	redist.SetEnable(false)
	mgr := redist.GetManager()
	redist.SetManager(nil)
	if mgr != nil {
		mgr.Stop()
	}
}

func (r *SSRedistWatchHandler) RedistIsInProgress() bool {
	if redist.IsEnabled() {
		mgr := redist.GetManager()
		if mgr != nil && !mgr.IsDone() {
			return true
		}
	}

	return false
}
