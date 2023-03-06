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

	"juno/pkg/logging/cal"
	"juno/pkg/proto"
)

var _ ITwoPhaseProcessor = (*SetProcessor)(nil)

type SetProcessor struct {
	TwoPhaseProcessor
	numInserting int
	conflictResp ResponseWrapper
}

func NewSetProcessor() *SetProcessor {
	p := &SetProcessor{
		TwoPhaseProcessor: TwoPhaseProcessor{
			prepareOpCode: proto.OpCodePrepareSet,
		},
	}
	p.self = p
	return p
}

func (p *SetProcessor) Init() {
	p.TwoPhaseProcessor.Init()
	p.numInserting = 0
	p.conflictResp.ssRequest = nil
}

func (p *SetProcessor) onPrepareSuccess(rc *SSRequestContext) {
	p.prepare.onSuccess(rc)

	switch p.state {
	case stTwoPhaseProcAbort:
		p.sendAbort(rc.ssIndex)
	case stTwoPhaseProcCommit:
		p.sendCommit(rc.ssIndex)
	default:

		if p.prepareSucceeded() {
			p.state = stTwoPhaseProcCommit
			p.setCommitMsg()
			p.sendCommits()
		}
	}
}

func (p *SetProcessor) onPrepareFailure(rc *SSRequestContext) {
	p.prepare.onFailure(rc)
	if p.prepareFailed() {
		st := p.errorPrepareResponseOpStatus()
		if p.conflictResp.ssRequest != nil && (st == proto.OpStatusVersionConflict) {
			p.replyToClient(&p.conflictResp)
		} else {
			p.replyStatusToClient(st)
		}
		p.abortSucceededPrepares()
	} else {
		p.sendPrepareRequest()
	}
}

func (p *SetProcessor) onCommitSuccess(st *SSRequestContext) {
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

func (p *SetProcessor) onCommitFailure(st *SSRequestContext) {
	p.TwoPhaseProcessor.onCommitFailure(st.ssIndex, st.ssResponseOpStatus)
	if p.commit.getNumSuccessResponse() != 0 {
		p.sendRepairs()
	} else if p.commitFailed() {
		p.replyStatusToClient(proto.OpStatusCommitFailure)
	}
}

func (p *SetProcessor) onBadRequestID(rc *SSRequestContext) {
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

func (p *SetProcessor) setCommitMsg() {
	opMsg := &p.commit.opMsg
	isForReplication := p.clientRequest.IsForReplication()
	var version, creationTime, ttl uint32
	p.setSSOpRequestFromClientRequest(opMsg, proto.OpCodeCommit, version, false)
	if p.prepare.mostUpdatedOkResponse != nil {
		resp := &p.prepare.mostUpdatedOkResponse.ssRequest.ssRespOpMsg
		if resp.GetOpStatus() == proto.OpStatusInserting {
			opMsg.SetOriginatorRequestID(p.clientRequest.GetRequestID())
			creationTime = p.clientRequest.GetCreationTime()
			if creationTime == 0 {
				creationTime = uint32(time.Now().Unix())
			}
			ttl = p.clientRequest.GetTimeToLive()
			if ttl == 0 {
				ttl = confDefaultTimeToLive
			}
		} else {
			if LOG_DEBUG {
				if len(resp.GetOriginatorRequestID()) != 16 {
					glog.DebugInfof("oid not set in prepare response. rid=%s", p.requestID)
				}
			}
			opMsg.SetOriginatorRequestID(resp.GetOriginatorRequestID())
			creationTime = resp.GetCreationTime()
			version = resp.GetVersion()
			ttl = resp.GetTimeToLive()
		}
		if isForReplication {
			if p.clientRequest.GetVersion() > version {
				version = p.clientRequest.GetVersion()
			}
			if p.clientRequest.GetTimeToLive() > ttl {
				ttl = p.clientRequest.GetTimeToLive()
			}
			if p.clientRequest.GetCreationTime() > creationTime {
				creationTime = p.clientRequest.GetCreationTime()
			}
		} else {
			version = version + 1
		}
	}
	if p.prepare.getNumSuccessResponse() == p.numInserting {
		if creationTime == 0 {
			creationTime = uint32(time.Now().Unix())
		}
		if p.clientRequest.GetTimeToLive() == 0 {
			if LOG_DEBUG {
				glog.DebugInfof("Zero TTL from client Set request: %s, set TTL to default: %d", opMsg.GetRequestIDString(), confDefaultTimeToLive)
			}
			ttl = confDefaultTimeToLive
		}
	}
	if creationTime != 0 {
		opMsg.SetCreationTime(creationTime)
	}
	if version != 0 {
		opMsg.SetVersion(version)
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
	//opMsg.Encode(&p.p2.commitMsg)
}

func (p *SetProcessor) OnResponseReceived(rc *SSRequestContext) {
	switch rc.opCode {
	case proto.OpCodePrepareSet: //first phase response
		switch rc.ssResponseOpStatus {
		case proto.OpStatusNoError, proto.OpStatusAlreadyFulfilled:
			p.onPrepareSuccess(rc)
			return
		case proto.OpStatusInserting:
			p.numInserting++
			p.onPrepareSuccess(rc)
			return
		case proto.OpStatusRecordLocked, proto.OpStatusNoStorageServer,
			proto.OpStatusBadParam:
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
		default:
			//Not expected
			//TODO...
			glog.Info("Got unexpected OpStatus: ", rc.ssResponseOpStatus.String())
			p.onPrepareFailure(rc)
		}
	case proto.OpCodeCommit:
		switch rc.ssResponseOpStatus {
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

func (p *SetProcessor) OnSSTimeout(rc *SSRequestContext) {
	//p.TwoPhaseProcessor.OnSSTimeout(st)
	switch rc.opCode {
	case proto.OpCodePrepareSet:
		p.onPrepareFailure(rc)
	case proto.OpCodeCommit:
		p.onCommitFailure(rc)
	case proto.OpCodeRepair:
		p.onRepairFailure(rc)
	}
}

func (p *SetProcessor) OnSSIOError(rc *SSRequestContext) {
	p.pendingResponses[rc.ssIndex] = nil
	switch rc.opCode {
	case proto.OpCodePrepareSet:
		p.onPrepareFailure(rc) ///TODO to a new OpStatus
	case proto.OpCodeCommit:
		p.onCommitFailure(rc)
	case proto.OpCodeRepair:
		p.onRepairFailure(rc)
	}
}
