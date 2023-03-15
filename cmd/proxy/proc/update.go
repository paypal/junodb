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

package proc

import (
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
)

var _ ITwoPhaseProcessor = (*UpdateProcessor)(nil)

type UpdateProcessor struct {
	TwoPhaseProcessor
	numInserting int

	conflictResp ResponseWrapper // for conditional update
}

func NewUpdateProcessor() *UpdateProcessor {
	p := &UpdateProcessor{
		TwoPhaseProcessor: TwoPhaseProcessor{
			prepareOpCode: proto.OpCodePrepareUpdate,
		},
	}
	p.self = p
	return p
}

func (p *UpdateProcessor) Init() {
	p.TwoPhaseProcessor.Init()
	p.numInserting = 0
	p.conflictResp.ssRequest = nil
}

func (p *UpdateProcessor) isRecordMarkDeleted() bool {
	if p.prepare.mostUpdatedOkResponse != nil &&
		p.prepare.mostUpdatedOkResponse.ssRequest.ssRespOpMsg.GetOpStatus() == proto.OpStatusInserting &&
		p.prepare.mostUpdatedOkResponse.ssRequest.ssRespOpMsg.GetVersion() != 0 {
		return true
	}
	return false
}

func (p *UpdateProcessor) actIfDoneWithPrepare() bool {
	if p.prepare.hasNoPending() {
		if p.isRecordMarkDeleted() {
			p.state = stTwoPhaseProcAbort
			for i := 0; i < p.prepare.getNumSuccessResponse(); i++ {
				resp := p.prepare.successResponses[i].ssRequest.ssRespOpMsg
				if resp.GetOpStatus() == proto.OpStatusNoError || resp.GetOpStatus() == proto.OpStatusAlreadyFulfilled {
					markDelete := proto.OperationalMessage{}
					markDelete.SetAsRequest()
					markDelete.SetKey(p.clientRequest.GetKey())
					markDelete.SetNamespace(p.clientRequest.GetNamespace())
					markDelete.SetOpCode(proto.OpCodeMarkDelete)
					markDelete.SetCreationTime(p.prepare.mostUpdatedOkResponse.ssRequest.ssRespOpMsg.GetCreationTime())
					markDelete.SetShardId(p.shardId)
					markDelete.SetRequestID(p.clientRequest.GetRequestID())

					var raw proto.RawMessage
					if markDelete.Encode(&raw) != nil {
						panic("fail to encode markdelete request")
					}
					p.sendMessage(&raw, p.prepare.successResponses[i].ssRequest.ssIndex)
				} else {
					p.sendAbort(p.prepare.successResponses[i].ssRequest.ssIndex)

				}

			}
			p.replyStatusToClient(proto.OpStatusNoKey) //p.errorPrepareResponseOpStatus())
			return true
		} else if p.prepareFailed() { //p.prepare.getNumErrors() >= confNumWrites {
			st := p.errorPrepareResponseOpStatus()
			if p.conflictResp.ssRequest != nil && (st == proto.OpStatusVersionConflict) {
				p.replyToClient(&p.conflictResp)
			} else {
				p.replyStatusToClient(st)
			}
			for i := 0; i < p.prepare.getNumSuccessResponse(); i++ {
				p.sendAbort(p.prepare.successResponses[i].ssRequest.ssIndex)
			}
			return true
		} else if p.prepareSucceeded() {
			p.setCommitMsg()
			p.sendCommits()
			return true
		}
	}
	return false
}

func (p *UpdateProcessor) onPrepareSuccess(rc *SSRequestContext) {
	p.prepare.onSuccess(rc)
	p.actIfDoneWithPrepare()
}

func (p *UpdateProcessor) onPrepareFailure(rc *SSRequestContext) {
	p.prepare.onFailure(rc)
	if p.actIfDoneWithPrepare() == false {
		p.sendPrepareRequest()
	}
}

func (p *UpdateProcessor) prepareFailed() bool {
	if p.isRecordMarkDeleted() {
		return true
	}
	nErr := p.prepare.getNumErrorResponse() + p.prepare.getNumIOAndTimeout() //numIOError + p.p1.numTimeout
	return (nErr+p.ssGroup.numBrokenSSs >= confNumWrites) || (p.numInserting >= confNumWrites && !p.clientRequest.IsForReplication())
}

func (p *UpdateProcessor) prepareSucceeded() bool {
	if p.isRecordMarkDeleted() {
		return false
	}
	if p.clientRequest.IsForReplication() {
		return (p.prepare.getNumSuccessResponse() >= confNumWrites)
	} else {
		return p.prepare.getNumSuccessResponse()-p.numInserting > 0 && (p.prepare.getNumSuccessResponse() >= confNumWrites)
	}
}

func (p *UpdateProcessor) onCommitSuccess(st *SSRequestContext) {
	p.TwoPhaseProcessor.onCommitSuccess(st)
	if p.numBadRequestID > 0 {
		p.replyStatusToClient(proto.OpStatusInconsistent) ///TODO
		return
	} else {
		if p.commitSucceeded() {
			p.replyToClient(&p.commit.noErrResponse)
		}
		p.sendRepairs()
	}
}

func (p *UpdateProcessor) onCommitFailure(st *SSRequestContext) {
	p.TwoPhaseProcessor.onCommitFailure(st.ssIndex, st.ssResponseOpStatus)
	if p.commit.getNumSuccessResponse() != 0 {
		p.sendRepairs()
	} else if p.commitFailed() {
		p.replyStatusToClient(proto.OpStatusCommitFailure)
	}
}

func (p *UpdateProcessor) onBadRequestID(rc *SSRequestContext) {
	p.numBadRequestID++
	//p.p2.numCommitFailure++
	p.commit.onErrorResponse()
	if p.commit.getNumSuccessResponse() != 0 {
		p.replyStatusToClient(proto.OpStatusInconsistent) ///TODO
		p.sendRepairs()
	} else if p.commitFailed() {
		p.replyStatusToClient(proto.OpStatusCommitFailure) ///TODO
	}
}
func (p *UpdateProcessor) setCommitMsg() {
	opMsg := &p.commit.opMsg
	isForReplication := p.clientRequest.IsForReplication()
	var version, creationTime, ttl uint32
	if p.prepare.mostUpdatedOkResponse != nil {
		p.setSSOpRequestFromClientRequest(opMsg, proto.OpCodeCommit, version, false)

		resp := &p.prepare.mostUpdatedOkResponse.ssRequest.ssRespOpMsg
		if LOG_DEBUG {
			b := logging.NewKVBufferForLog()
			b.AddOpStatus(resp.GetOpStatus()).AddVersion(resp.GetVersion()).AddTTL(resp.GetTimeToLive()).AddOriginator(resp.GetOriginatorRequestID())
			glog.DebugInfof("mostupdatedOkResp: %v", b)
		}
		if isForReplication {
			opMsg.SetOriginatorRequestID(p.clientRequest.GetOriginatorRequestID())
		} else {
			if LOG_DEBUG {
				if len(resp.GetOriginatorRequestID()) != 16 {
					glog.DebugInfof("oid not set in prepare response. rid=%s", p.requestID)
				}
			}
			opMsg.SetOriginatorRequestID(resp.GetOriginatorRequestID())
		}
		ttl = resp.GetTimeToLive()
		if isForReplication {
			version = p.clientRequest.GetVersion()
			if p.clientRequest.GetTimeToLive() > ttl {
				ttl = p.clientRequest.GetTimeToLive()
			}
			creationTime = p.clientRequest.GetCreationTime()
		} else {
			creationTime = resp.GetCreationTime()
			version = resp.GetVersion() + 1
		}
	}
	if version != 0 {
		opMsg.SetVersion(version)
	}
	if creationTime != 0 {
		opMsg.SetCreationTime(creationTime)
	}
	if ttl != 0 {
		opMsg.SetTimeToLive(ttl)
	}
	if isForReplication {
		lmt := p.clientRequest.GetLastModificationTime()
		if lmt != 0 {
			opMsg.SetLastModificationTime(lmt)
		} else {
			if LOG_DEBUG {
				glog.DebugInfof("LastModificationTime not set for replication request rid=%s", p.requestID)
			}
			opMsg.SetLastModificationTime(uint64(time.Now().UnixNano()))
			if opMsg.GetVersion() >= confMaxRecordVersion {
				opMsg.SetVersion(0)
				if cal.IsEnabled() {
					cal.Event(kCalMsgTypeReqProc, "R"+kRecVerOverflow, cal.StatusSuccess, []byte("rid="+p.requestID))
				}
				if LOG_DEBUG {
					glog.DebugInfof("version overflow. reset to 0. rid=%s", p.requestID)
				}
			}
		}
	} else {
		opMsg.SetLastModificationTime(uint64(time.Now().UnixNano()))
		if opMsg.GetVersion() > confMaxRecordVersion {
			opMsg.SetVersion(1)
			opMsg.SetCreationTime(uint32(time.Now().Unix()))
			opMsg.SetOriginatorRequestID(p.clientRequest.GetRequestID())
			if cal.IsEnabled() {
				cal.Event(kCalMsgTypeReqProc, kRecVerOverflow, cal.StatusSuccess, []byte("rid="+p.requestID))
			}
			if LOG_DEBUG {
				glog.DebugInfof("version overflow. reset to 1. rid=%s", p.requestID)
			}
		}
	}
	p.commit.setFromOpMsg(opMsg)
}

func (p *UpdateProcessor) OnResponseReceived(rc *SSRequestContext) { ///TODO

	opStatus := rc.ssResponseOpStatus

	switch rc.opCode {
	case proto.OpCodePrepareUpdate: //first phase response
		switch opStatus {
		case proto.OpStatusNoError, proto.OpStatusAlreadyFulfilled:
			p.onPrepareSuccess(rc)
			return
		case proto.OpStatusInserting:
			p.numInserting++
			p.onPrepareSuccess(rc)
		case proto.OpStatusRecordLocked, proto.OpStatusNoStorageServer,
			proto.OpStatusBadParam:
			///SHOULDN't HAVE NoStorageServer
			p.onPrepareFailure(rc)

		case proto.OpStatusVersionConflict:
			if p.conflictResp.ssRequest == nil {
				p.conflictResp.ssRequest = rc
			} else {
				if recordMostUpdatedThan(&rc.ssRespOpMsg, &p.conflictResp.ssRequest.ssRespOpMsg) {
					p.conflictResp.ssRequest = rc
				}
			}
			p.onPrepareFailure(rc)
			///TODO
		default:
			//Not expected
			//TODO...
			glog.Info("Got unexpected OpStatus: ", rc.ssResponseOpStatus.String())
			p.onPrepareFailure(rc)
		}
	case proto.OpCodeCommit:
		switch opStatus {
		case proto.OpStatusNoError,
			proto.OpStatusAlreadyFulfilled:
			//OpStatusAlreadyFulfilled is added for mock unit test for now. SS should not return AlreadyFulfilled to commit
			p.onCommitSuccess(rc)
		default:
			p.onCommitFailure(rc)
		}
	case proto.OpCodeRepair:
		switch rc.ssResponseOpStatus {
		case proto.OpStatusNoError, proto.OpStatusAlreadyFulfilled:
			p.onRepairSuccess(rc)
		default:
			p.onRepairFailure(rc)
		}
	}
}

func (p *UpdateProcessor) OnSSTimeout(rc *SSRequestContext) {
	switch rc.opCode {
	case proto.OpCodePrepareUpdate:
		p.onPrepareFailure(rc) ///TODO proto.OpStatusNoStorageServer)
	case proto.OpCodeCommit:
		p.onCommitFailure(rc)
	case proto.OpCodeRepair:
		p.onRepairFailure(rc)
	}
}

func (p *UpdateProcessor) OnSSIOError(rc *SSRequestContext) {
	//	st.done = true
	p.pendingResponses[rc.ssIndex] = nil
	switch rc.opCode {
	case proto.OpCodePrepareUpdate:
		p.onPrepareFailure(rc) ///TODO proto.OpStatusNoStorageServer) ///TODO to a new OpStatus
	case proto.OpCodeCommit:
		p.onCommitFailure(rc)
	case proto.OpCodeRepair:
		p.onRepairFailure(rc)
	}
}

func (p *UpdateProcessor) errorPrepareResponseOpStatus() (st proto.OpStatus) {
	if p.numInserting >= confNumWrites && !p.clientRequest.IsForReplication() {
		st = proto.OpStatusNoKey
		return
	}
	numLocked := 0
	for i := 0; i < p.prepare.getNumErrorResponse(); i++ {
		t := p.prepare.errorResponses[i].ssRequest.ssRespOpMsg.GetOpStatus()
		switch t {
		case proto.OpStatusRecordLocked:
			numLocked++
		case proto.OpStatusDupKey:
			st = t
			return
		default:
			st = t
		}
	}
	if numLocked > 0 {
		st = proto.OpStatusRecordLocked
	}
	if p.prepare.getNumIOAndTimeout() > confMaxNumFailures {
		if p.prepare.getNumNoStageErrors() > confMaxNumFailures {
			st = proto.OpStatusNoStorageServer
		} else {
			st = proto.OpStatusBusy
		}
	}
	return
}
