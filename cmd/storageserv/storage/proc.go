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
	"fmt"
	goio "io"
	"runtime"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/config"
	ssstats "juno/cmd/storageserv/stats"
	"juno/cmd/storageserv/storage/db"
	"juno/pkg/debug"
	"juno/pkg/io"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/logging/otel"
	"juno/pkg/proto"
	"juno/pkg/shard"
	"juno/pkg/stats"
	"juno/pkg/util"
)

const (
	kCalMsgTypeReqProc = "ReqProc"
)

type (
	reqProcCtxT struct {
		pool         *ReqProcCtxPool
		reqctx       io.IRequestContext
		request      proto.OperationalMessage
		recordId     db.RecordID
		recIdBuf     bytes.Buffer
		shardId      shard.ID
		microShardId uint8
		response     proto.OperationalMessage
		rawResponse  proto.RawMessage
		replied      bool
		cacheable    bool
		prepareCtx   *reqProcCtxT

		dbRecExist bool
		dbRec      db.Record
		timer      *util.TimerWrapper
		chReq      chan *reqProcCtxT
		encodeBuf  bytes.Buffer
	}
	ReqProcCtxPool util.ChanPool
)

var _ io.IResponseContext = (*reqProcCtxT)(nil)

func (p *reqProcCtxT) GetStatus() uint32 {
	return 0
}

func (p *reqProcCtxT) GetMessage() *proto.RawMessage {
	return &p.rawResponse
}

func (p *reqProcCtxT) GetMsgSize() uint32 {
	return p.rawResponse.GetMsgSize()
}

func (p *reqProcCtxT) addToPool() {
	if p.pool != nil {
		p.resetProcContext()
		p.pool.Put(p)
	}
}

func (p *reqProcCtxT) OnComplete() {

	if p.reqctx == nil {
		glog.ErrorDepth(1, "nil reqctx")
		return
	}
	rht := time.Since(p.reqctx.GetReceiveTime())
	rhtus := int(rht / 1000)
	opcode := p.request.GetOpCode()

	if config.ServerConfig().StateLogEnabled {
		var st stats.ProcStat
		st.Init(&p.request)
		st.OnComplete(uint32(rhtus), p.response.GetOpStatus())
		ssstats.SendProcState(st)
	}
	if cal.IsEnabled() {
		opst := p.response.GetOpStatus()
		calst := logging.CalStatus(opst)

		if cal.LogInfoPercent() || calst.NotSuccess() {
			calData := logging.NewKVBuffer()
			calData.AddOpRequestResponse(&p.request, &p.response).AddRequestHandleTime(rhtus)
			cal.AtomicTransaction(cal.TxnTypeAPI, opcode.String(), calst.CalStatus(), rht, calData.Bytes())
		}
		if (opst == proto.OpStatusInconsistent) || calst.NotSuccess() {
			cal.Event("ProcErr", opcode.String()+"_"+opst.String(), cal.StatusSuccess, nil)
		}
	}

	otel.RecordOperation(opcode.String(), p.response.GetOpStatus(), int64(rhtus))

	if p.cacheable {
		if p.prepareCtx != nil {
			if p.prepareCtx.cacheable {
				p.prepareCtx.addToPool()
			} else {
				if cal.IsEnabled() {
					b := logging.NewKVBuffer()
					b.AddOpRequestResponse(&p.request, &p.response)
					cal.Event(kCalMsgTypeReqProc, "pdata_not_cacheable", cal.StatusSuccess, b.Bytes())
				}
			}
		} else {
			//	glog.Infof("nil prepareCtx")

		}
		p.addToPool()
	} else {
		p.cacheable = true
	}
}

func (p *reqProcCtxT) Read(r goio.Reader) (n int, err error) {
	return p.rawResponse.Read(r)
}

func (p *reqProcCtxT) Write(w goio.Writer) (n int, err error) {
	return p.rawResponse.Write(w)
}

func (p *reqProcCtxT) init() {
	p.replied = false
	p.cacheable = true

	p.recordId = nil

	p.shardId = 0
	p.microShardId = 0

	p.dbRecExist = false
	p.timer = util.NewTimerWrapper(config.ServerConfig().RecLockExpiration.Duration)
	p.timer.Stop()
	p.chReq = nil
}

func (p *reqProcCtxT) resetProcContext() {
	p.reqctx = nil
	p.request = proto.OperationalMessage{}
	p.replied = false
	p.cacheable = true

	p.recordId = nil

	p.shardId = 0
	p.microShardId = 0

	p.dbRecExist = false
	p.dbRec.ResetRecord()
	p.timer.Stop()
	p.chReq = nil
	p.prepareCtx = nil
}

func (p *reqProcCtxT) attach(ctx io.IRequestContext) bool {
	p.reqctx = ctx
	p.request = proto.OperationalMessage{}

	var err error
	if err = p.request.Decode(p.reqctx.GetMessage()); err != nil {
		glog.Errorf("Failed to decode inbound request: %s", err)
		if cal.IsEnabled() {
			cal.Event(kCalMsgTypeReqProc, "fail_to_decode", cal.StatusSuccess, nil)
		}
		if debug.DEBUG {
			raw := p.reqctx.GetMessage()
			raw.HexDump()
			raw.PrintBytesForTest()
			panic("")
		}
		return false
	}

	p.shardId = shard.ID(p.request.GetShardId())

	return true
}

func (p *reqProcCtxT) Process(reqCtx io.IRequestContext) {
	if p.attach(reqCtx) == false {
		glog.Error("Failed to init inbound ss request context")
		return
	}

	opCode := p.request.GetOpCode()
	if opCode == proto.OpCodeNop || opCode == proto.OpCodeVerHandshake { // for now
		resp := io.NewInboundRespose(opCode, reqCtx.GetMessage())
		reqCtx.Reply(resp)
		return
	}

	// get computed shard id & micro shard id
	req := &p.request
	computedShardId, microShardId := util.GetShardIds(req.GetKey(),
		uint32(config.ServerConfig().ClusterInfo.NumShards), config.ServerConfig().NumMicroShards)
	p.microShardId = microShardId // used in prefix only if enabled

	// shard id validation
	if computedShardId != uint16(p.shardId) && config.ServerConfig().ShardIdValidation &&
		req.GetOpCode() != proto.OpCodeClone {

		glog.Errorf("Bad Param: shard id does not match key: %d, %d", uint16(p.shardId), computedShardId)
		if cal.IsEnabled() {
			cal.Event(kCalMsgTypeReqProc, "BadParam_invalid_shard_id", cal.StatusSuccess, nil)
		}
		p.replyWithErrorOpStatus(proto.OpStatusBadParam)
		return
	}

	p.recordId = db.NewRecordIDWithBuffer(&p.recIdBuf, p.shardId, p.microShardId, p.request.GetNamespace(), p.request.GetKey())

	if glog.LOG_DEBUG {
		req := &p.request
		b := logging.NewKVBufferForLog()
		b.AddVersion(req.GetVersion()).AddReqIdString(req.GetRequestIDString()).
			AddHexKey(req.GetKey()).AddNamespace(req.GetNamespace()).AddShardId(req.GetShardId())
		b.AddOriginator(req.GetOriginatorRequestID()).AddCreationTime(req.GetCreationTime())
		glog.Debugf("->: %s %v", req.GetOpCodeText(), b)
	}

	process(p)
}

func (p *reqProcCtxT) replyWithErrorOpStatus(st proto.OpStatus) {
	if p.replied {
		msg := fmt.Sprintf("already replied rid=%s", p.request.GetRequestID())
		glog.ErrorDepth(1, msg)
		if cal.IsEnabled() {
			b := logging.NewKVBuffer()
			b.AddOpStatus(st).AddOpRequest(&p.request)
			cal.Event(kCalMsgTypeReqProc, "replied", cal.StatusSuccess, b.Bytes())
		}
		if debug.DEBUG {
			var buf bytes.Buffer
			p.request.PrettyPrint(&buf)
			p.response.PrettyPrint(&buf)
			glog.Error(buf.String())
		}
		return
	}
	p.replied = true
	op := &p.request
	p.response = proto.OperationalMessage{}
	resp := &p.response
	resp.SetOpCode(op.GetOpCode())
	resp.SetOpaque(op.GetOpaque())
	resp.SetKey(op.GetKey())
	resp.SetNamespace(op.GetNamespace())
	resp.SetRequestID(op.GetRequestID())
	resp.SetAsResponse()
	resp.SetOpStatus(st)

	if glog.LOG_DEBUG {
		b := logging.NewKVBufferForLog()
		b.AddOpStatus(resp.GetOpStatus()).AddVersion(resp.GetVersion()).
			AddReqIdString(resp.GetRequestIDString()).AddOriginator(resp.GetOriginatorRequestID())
		glog.Debugf("<-: %s %v", resp.GetOpCodeText(), b)
	}
	err := p.response.Encode(&p.rawResponse)
	if err == nil {
		if p.reqctx != nil {
			p.reqctx.Reply(p)
		} else {
			glog.Error("request context is nil")
		}
	} else {
		glog.Errorf("Error encoding response using NewInboundResponseContext: %s", err)
	}
}

func (p *reqProcCtxT) reply() {
	if p.replied {
		msg := fmt.Sprintf("already replied rid=%s", p.request.GetRequestID())
		glog.ErrorDepth(1, msg)
		if cal.IsEnabled() {
			b := logging.NewKVBuffer()
			b.AddOpRequestResponse(&p.request, &p.response)
			cal.Event(kCalMsgTypeReqProc, "replied", cal.StatusSuccess, b.Bytes())
		}
		if debug.DEBUG {
			var buf bytes.Buffer
			p.request.PrettyPrint(&buf)
			p.response.PrettyPrint(&buf)
			glog.Error(buf.String())
		}
		return
	}
	p.replied = true
	if glog.LOG_DEBUG {
		resp := &p.response
		b := logging.NewKVBufferForLog()
		b.AddOpStatus(resp.GetOpStatus()).AddVersion(resp.GetVersion()).
			AddReqIdString(resp.GetRequestIDString()).AddOriginator(resp.GetOriginatorRequestID())
		glog.Debugf("<-: %s %v", resp.GetOpCodeText(), b)
	}
	err := p.response.Encode(&p.rawResponse)

	if err == nil {
		if p.reqctx != nil {
			p.reqctx.Reply(p)
		} else {
			glog.Error("request context is nil")
		}
	} else {
		glog.Errorf("Error encoding response using NewInboundResponseContext: %s", err)
	}
}

func (p *reqProcCtxT) IsForInserting() bool {
	return p.chReq != nil && (!p.dbRecExist)
}

func (p *reqProcCtxT) initResponse(opStatus proto.OpStatus, version uint32, expTime uint32, creationTime uint32) {
	op := &p.request
	p.response = proto.OperationalMessage{}
	resp := &p.response

	resp.SetOpCode(op.GetOpCode())
	resp.SetOpaque(op.GetOpaque())
	resp.SetKey(op.GetKey())
	resp.SetNamespace(op.GetNamespace())
	resp.SetRequestID(op.GetRequestID())
	resp.SetAsResponse()

	resp.SetOpStatus(opStatus)
	resp.SetVersion(version)
	resp.SetCreationTime(creationTime)

	existingRemainingTTL := util.GetTimeToLive(expTime)

	switch op.GetOpCode() {
	case proto.OpCodePrepareCreate, proto.OpCodeCommit, proto.OpCodeAbort, proto.OpCodePrepareDelete, proto.OpCodeDelete, proto.OpCodeRepair:
		resp.SetTimeToLive(op.GetTimeToLive())

	case proto.OpCodePrepareUpdate, proto.OpCodePrepareSet:
		if op.GetTimeToLive() > existingRemainingTTL {
			resp.SetTimeToLive(op.GetTimeToLive())
		} else {
			resp.SetTimeToLive(existingRemainingTTL)
		}

	case proto.OpCodeRead:
		resp.SetTimeToLive(existingRemainingTTL)
	}

	resp.SetExpirationTime(util.GetExpirationTime(resp.GetTimeToLive()))
}

func (p *reqProcCtxT) initResponseWithStatus(opStatus proto.OpStatus) {
	op := &p.request
	p.response = proto.OperationalMessage{}
	resp := &p.response
	resp.SetOpCode(op.GetOpCode())
	resp.SetOpaque(op.GetOpaque())
	resp.SetKey(op.GetKey())
	resp.SetNamespace(op.GetNamespace())
	resp.SetRequestID(op.GetRequestID())
	resp.SetAsResponse()
	resp.SetOpStatus(opStatus)
}

func NewReqProcCtxPool(chansize int32) *ReqProcCtxPool {
	pool := util.NewChanPool(int(chansize), func() interface{} {
		p := &reqProcCtxT{}
		runtime.SetFinalizer(p, func(ctx *reqProcCtxT) {
			ctx.resetProcContext()
		})
		p.init()
		return p
	})

	return (*ReqProcCtxPool)(pool)
}

func (p *ReqProcCtxPool) Get() *reqProcCtxT {
	ctx := (*util.ChanPool)(p).Get().(*reqProcCtxT)
	ctx.pool = p
	return ctx
}

func (p *ReqProcCtxPool) Put(proc *reqProcCtxT) {
	(*util.ChanPool)(p).Put(proc)
}
