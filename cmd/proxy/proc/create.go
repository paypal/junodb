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
	"fmt"
	"time"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/paypal/junodb/pkg/logging/cal"
	"github.com/paypal/junodb/pkg/logging/otel"
	"github.com/paypal/junodb/pkg/proto"
)

var _ ITwoPhaseProcessor = (*CreateProcessor)(nil)

type CreateProcessor struct {
	TwoPhaseProcessor
	numDupKey    uint8
	numInserting uint8
}

func NewCreateProcessor() *CreateProcessor {
	p := &CreateProcessor{
		TwoPhaseProcessor: TwoPhaseProcessor{
			prepareOpCode: proto.OpCodePrepareCreate,
		},
	}
	p.self = p
	return p
}

func (p *CreateProcessor) Init() {
	p.TwoPhaseProcessor.Init()
	p.numDupKey = 0
	p.numInserting = 0
}

func (p *CreateProcessor) setInitSSRequest() bool {
	reEncode := false
	ct := p.clientRequest.GetCreationTime()

	if ct == 0 {
		reEncode = true
		ct = uint32(time.Now().Unix())
		p.clientRequest.SetCreationTime(ct)
		if LOG_DEBUG {
			glog.DebugInfof("Creation time from clientrequest is zero, set it to be %d", ct)
		}
	}
	if p.clientRequest.GetTimeToLive() == 0 {
		reEncode = true
		if LOG_DEBUG {
			glog.DebugInfof("Zero TTL from client Create request: %s, set TTL to default: %d", p.requestID, confDefaultTimeToLive)
		}
		p.clientRequest.SetTimeToLive(confDefaultTimeToLive)
	}

	if confEncryptionEnabled && p.clientRequest.GetPayload().GetLength() != 0 && p.clientRequest.GetPayload().GetPayloadType() == proto.PayloadTypeClear {
		opMsg := p.clientRequest
		if err := opMsg.GetPayload().Encrypt(proto.PayloadTypeEncryptedByProxy); err != nil {
			errmsg := fmt.Sprintf("err=%s", err.Error())
			glog.Error(errmsg)
			if cal.IsEnabled() {
				calLogReqProcError(kEncrypt, []byte(errmsg))
			}
			if otel.IsEnabled() {
				otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Operation, kEncrypt}, {otel.Status, otel.StatusError}})
			}
			p.replyStatusToClient(proto.OpStatusInternal)
			return false
		}
		if err := p.prepare.resetFromOpMsg(&opMsg); err != nil {
			return false
		}
		p.prepare.setShardId(p.shardId)
	} else {
		if reEncode {
			if err := p.prepare.resetFromOpMsg(&p.clientRequest); err != nil {
				return false
			}
			p.prepare.setShardId(p.shardId)
		} else {
			p.TwoPhaseProcessor.setInitSSRequest()
		}
	}
	return true
}

func (p *CreateProcessor) actIfDoneWithPrepare() bool {
	if p.prepare.hasNoPending() {
		if p.prepare.getNumSuccessResponse() >= confNumWrites {
			if p.numDupKey == 0 {
				p.setCommitMsg()
				p.sendCommits()
			} else if p.numInserting != 0 {
				p.setCommitMsg()
				p.sendCommits()
			} else {
				p.replyStatusToClient(p.errorPrepareResponseOpStatus())
				p.abortSucceededPrepares()

			}
			return true
		} else if p.prepare.getNumErrors()+p.ssGroup.numBrokenSSs >= confNumWrites {
			p.replyStatusToClient(p.errorPrepareResponseOpStatus())
			p.abortSucceededPrepares()
			return true
		}
	}
	return false
}

func (p *CreateProcessor) onPrepareSuccess(rc *SSRequestContext) {
	p.prepare.onSuccess(rc)
	p.actIfDoneWithPrepare()
}

func (p *CreateProcessor) onPrepareFailure(rc *SSRequestContext) {
	p.prepare.onFailure(rc)
	if p.actIfDoneWithPrepare() == false {
		p.sendPrepareRequest()
	}
}

func (p *CreateProcessor) onCommitSuccess(st *SSRequestContext) {
	p.TwoPhaseProcessor.onCommitSuccess(st)
	if p.numBadRequestID > 0 {
		p.replyStatusToClient(proto.OpStatusInconsistent) ///TODO
		return

	} else {
		if p.commitSucceeded() {
			if !p.hasRepliedClient {
				p.replyToClient(&p.commit.noErrResponse)
				for i := 0; i < int(p.prepare.numErrorResponse); i++ {
					ssReq := p.prepare.errorResponses[i].ssRequest
					if ssReq != nil {
						p.sendRepair(ssReq.ssIndex)
					}
				}
			}
		}
	}
}

func (p *CreateProcessor) onRepairSuccess(rc *SSRequestContext) {
	p.repair.onSuccessResponse()
	if p.commitSucceeded() {
		if !p.hasRepliedClient {
			p.replyToClient(&p.commit.noErrResponse)
			for i := 0; i < int(p.prepare.numErrorResponse); i++ {
				ssReq := p.prepare.errorResponses[i].ssRequest
				if ssReq != nil {
					p.sendRepair(ssReq.ssIndex)
				}
			}
		}
	}
}

func (p *CreateProcessor) onCommitFailure(st *SSRequestContext) {
	p.TwoPhaseProcessor.onCommitFailure(st.ssIndex, st.ssResponseOpStatus)
	if p.commitFailed() {
		p.replyStatusToClient(proto.OpStatusCommitFailure)
	} else {
		p.sendRepair(st.ssIndex)
	}
}

func (p *CreateProcessor) onBadRequestID(rc *SSRequestContext) {
	p.numBadRequestID++
	p.commit.onErrorResponse()
	if p.commit.getNumSuccessResponse() != 0 {
		p.replyStatusToClient(proto.OpStatusInconsistent) ///TODO
		p.sendRepairs()
	} else if p.commitFailed() {
		p.replyStatusToClient(proto.OpStatusCommitFailure) ///TODO
	}
}

func (p *CreateProcessor) setCommitMsg() {
	opMsg := &p.commit.opMsg
	p.setSSOpRequestFromClientRequest(opMsg, proto.OpCodeCommit, 1, false)
	if p.clientRequest.IsForReplication() {
		lmt := p.clientRequest.GetLastModificationTime()
		if lmt != 0 {
			p.commit.opMsg.SetLastModificationTime(lmt)
		} else {
			if LOG_DEBUG {
				glog.DebugInfof("LastModificationTime not set for replication request rid=%s", p.requestID)
			}
			opMsg.SetLastModificationTime(uint64(time.Now().UnixNano()))
		}
	} else {
		opMsg.SetLastModificationTime(uint64(time.Now().UnixNano()))
	}
	p.commit.setFromOpMsg(opMsg)
}

func (p *CreateProcessor) OnResponseReceived(rc *SSRequestContext) { ///TODO

	switch rc.opCode {
	case proto.OpCodePrepareCreate: //first phase response
		switch rc.ssResponseOpStatus {
		case proto.OpStatusNoError, proto.OpStatusAlreadyFulfilled:
			p.onPrepareSuccess(rc)
		case proto.OpStatusInserting:
			p.numInserting++
			p.onPrepareSuccess(rc)
		case proto.OpStatusDupKey:
			p.numDupKey++
			p.onPrepareFailure(rc)
		case proto.OpStatusRecordLocked, proto.OpStatusNoStorageServer, proto.OpStatusBadParam:
			p.onPrepareFailure(rc)
		default:
			//Not expected
			glog.Info("Got unexpected OpStatus: ", rc.ssResponseOpStatus.String())
			p.onPrepareFailure(rc)
		}
	case proto.OpCodeCommit:
		switch rc.ssResponseOpStatus {
		case proto.OpStatusNoError, proto.OpStatusAlreadyFulfilled:
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

func (p *CreateProcessor) OnSSTimeout(rc *SSRequestContext) {
	switch rc.opCode {
	case proto.OpCodePrepareCreate:
		p.onPrepareFailure(rc)
	case proto.OpCodeCommit:
		p.onCommitFailure(rc)
	case proto.OpCodeRepair:
		p.onRepairFailure(rc)
	}
}

func (p *CreateProcessor) OnSSIOError(rc *SSRequestContext) {
	p.pendingResponses[rc.ssIndex] = nil
	switch rc.opCode {
	case proto.OpCodePrepareCreate:
		p.onPrepareFailure(rc) ///TODO to a new OpStatus
	case proto.OpCodeCommit:
		p.onCommitFailure(rc)
	case proto.OpCodeRepair:
		p.onRepairFailure(rc)
	}
}

func (p *CreateProcessor) errorPrepareResponseOpStatus() (st proto.OpStatus) {
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
