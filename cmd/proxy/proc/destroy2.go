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
package proc

import (
	"juno/third_party/forked/golang/glog"

	"juno/pkg/proto"
)

// SUCCESS: NoError, AlreadyFulfilled, NoKey

var _ ITwoPhaseProcessor = (*TwoPhaseDestroyProcessor)(nil)

type TwoPhaseDestroyProcessor struct {
	TwoPhaseProcessor
	numP1NoKeyResponses int
	delRequest          RequestAndStats
	markDelRequest      RequestAndStats
	noErrMarkDelResp    ResponseWrapper ///TODO refactoring
}

func newDestroyRequestProcessor() IRequestProcessor {
	if confTwoPhaseDestroyEnabled {
		return newTwoPhaseDestroyProcessor()
	} else {
		return newDestroyProcessor()
	}
}

func newTwoPhaseDestroyProcessor() *TwoPhaseDestroyProcessor {
	p := &TwoPhaseDestroyProcessor{
		TwoPhaseProcessor: TwoPhaseProcessor{
			prepareOpCode: proto.OpCodePrepareDelete,
		},
	}
	p.self = p
	return p
}

func (p *TwoPhaseDestroyProcessor) Init() {
	p.TwoPhaseProcessor.Init()
	p.numP1NoKeyResponses = 0

	p.markDelRequest.init()
	p.noErrMarkDelResp.ssRequest = nil
	p.delRequest.init()
}

func (p *TwoPhaseDestroyProcessor) sendInitRequests() {
	p.state = stTwoPhaseProcPrepare
	if p.self.setInitSSRequest() {
		if err := p.prepare.setOpCode(p.prepareOpCode); err != nil {
			p.replyStatusToClient(proto.OpStatusBadMsg) // TODO revisit
			return
		}
		for i := 0; i < p.ssGroup.numAvailableSSs; i++ {
			p.sendPrepareRequest()
		}
		if p.numSSRequestSent < confNumWrites {
			if p.prepare.numFailToSend == p.prepare.numFailToSendNoConn {
				p.replyStatusToClient(proto.OpStatusNoStorageServer)
			} else {
				p.replyStatusToClient(proto.OpStatusBusy)
			}
		}
	} else {
		p.replyStatusToClient(proto.OpStatusBadMsg) // TODO revisit
	}
}

func (p *TwoPhaseDestroyProcessor) actIfDoneWithPrepare() {
	if p.prepare.hasNoPending() {
		numSuccess := p.prepare.getNumSuccessResponse()
		if numSuccess == confNumZones {
			p.setCommitMsg()
			p.sendCommits()
		} else if numSuccess >= confNumWrites {
			p.markDeleteIfNeeded()
		} else {
			p.setAbortMsg()
			for i := 0; i < p.prepare.getNumSuccessResponse(); i++ {
				ssIndex := p.prepare.successResponses[i].ssRequest.ssIndex
				///TODO valid ssIndex
				p.sendAbort(ssIndex)
			}
			p.replyStatusToClient(p.errorPrepareResponseOpStatus())
		}
	}
}

func (p *TwoPhaseDestroyProcessor) onPrepareSuccess(rc *SSRequestContext) {
	p.prepare.onSuccess(rc)
	p.actIfDoneWithPrepare()
}

func (p *TwoPhaseDestroyProcessor) onPrepareNoKey(rc *SSRequestContext) {
	p.prepare.onSuccess(rc) //.onOkResponse()
	p.numP1NoKeyResponses++
	p.actIfDoneWithPrepare()
}

func (p *TwoPhaseDestroyProcessor) sendMarkDelete(ssIndex uint32) {
	if p.markDelRequest.isSet == false {
		var opMsg proto.OperationalMessage
		p.setSSOpRequestFromClientRequest(&opMsg, proto.OpCodeMarkDelete, 0, false)
		p.markDelRequest.setFromOpMsg(&opMsg)
	}
	p.send(&p.markDelRequest, ssIndex)
}

func (p *TwoPhaseDestroyProcessor) markDeleteIfNeeded() {
	numSuccess := p.prepare.getNumSuccessResponse()
	for i := 0; i < numSuccess; i++ {
		ssIndex := p.prepare.successResponses[i].ssRequest.ssIndex
		if p.prepare.successResponses[i].ssRequest.ssRespOpMsg.GetOpStatus() == proto.OpStatusNoKey {
			p.sendAbort(ssIndex)

		} else {
			p.sendMarkDelete(ssIndex) ///TODO valid ssIndex
		}
	}
	if numSuccess == p.numP1NoKeyResponses {
		p.replyStatusToClient(proto.OpStatusNoError)
	}
}

func (p *TwoPhaseDestroyProcessor) onPrepareFailure(rc *SSRequestContext) {
	p.prepare.onFailure(rc)
	p.actIfDoneWithPrepare()
}

func (p *TwoPhaseDestroyProcessor) actIfDoneWithCommitDeleteRepair() {
	if p.commit.hasNoPending() && p.delRequest.hasNoPending() {
		if p.commit.getNumSuccessResponse()+int(p.delRequest.numSuccessResponse) >= confNumWrites {
			//      Reply Ok to client
			///TODO to revisit
			if p.commit.getNumSuccessResponse() != 0 {
				msgToClient := &p.commit.noErrResponse.ssRequest.ssRespOpMsg
				if p.prepare.mostUpdatedOkResponse != nil {
					opMsg := &p.prepare.mostUpdatedOkResponse.ssRequest.ssRespOpMsg
					msgToClient.SetCreationTime(opMsg.GetCreationTime())
					msgToClient.SetTimeToLive(opMsg.GetTimeToLive())
					msgToClient.SetLastModificationTime(opMsg.GetLastModificationTime())
					msgToClient.SetOriginatorRequestID(opMsg.GetOriginatorRequestID())
				}
				p.replyToClient(&p.commit.noErrResponse)
			} else {
				p.replyStatusToClient(proto.OpStatusInconsistent) ///TODO: temporary
			}
		} else if p.commit.getNumSuccessResponse()+p.delRequest.getNumSuccessResponse() == 0 {
			p.replyStatusToClient(proto.OpStatusCommitFailure)
		} else {
			p.replyStatusToClient(proto.OpStatusInconsistent)
		}
	}
}

func (p *TwoPhaseDestroyProcessor) onCommitSuccess(st *SSRequestContext) {
	p.TwoPhaseProcessor.onCommitSuccess(st)
	if p.numBadRequestID > 0 {
		p.replyStatusToClient(proto.OpStatusInconsistent) ///TODO
		return
	}
	p.actIfDoneWithCommitDeleteRepair()
}

func (p *TwoPhaseDestroyProcessor) onCommitFailure(st *SSRequestContext) {
	p.TwoPhaseProcessor.onCommitFailure(st.ssIndex, st.ssResponseOpStatus)

	if p.delRequest.isSet == false {
		var opMsg proto.OperationalMessage
		p.setSSOpRequestFromClientRequest(&opMsg, proto.OpCodeDelete, 0, false) ///TODO ### CHECK ### othe meta info...
		p.delRequest.setFromOpMsg(&opMsg)
	}
	p.send(&p.delRequest, st.ssIndex)
	p.actIfDoneWithCommitDeleteRepair()
}

func (p *TwoPhaseDestroyProcessor) setCommitMsg() {
	opMsg := &proto.OperationalMessage{}

	p.setSSOpRequestFromClientRequest(opMsg, proto.OpCodeCommit, 1, false)
	p.commit.setFromOpMsg(opMsg)
}

func (p *TwoPhaseDestroyProcessor) onMarkDeleteResponse(rc *SSRequestContext) {
	if p.markDelRequest.isSuccessStatus(rc.ssResponseOpStatus) {
		if p.noErrMarkDelResp.ssRequest == nil {
			p.noErrMarkDelResp.ssRequest = rc
		}
		p.markDelRequest.onSuccessResponse()
	} else {
		p.markDelRequest.onErrorResponse()
	}
	if p.markDelRequest.hasNoPending() {
		if p.markDelRequest.respSuccessForAll() {
			respOpMsg := &p.noErrMarkDelResp.ssRequest.ssRespOpMsg
			if p.noErrMarkDelResp.ssRequest != nil && respOpMsg.Decode(p.noErrMarkDelResp.ssRequest.ssResponse.GetMessage()) == nil {
				p.replyToClient(&p.noErrMarkDelResp)
			} else {
				p.replyStatusToClient(proto.OpStatusNoError)
			}
		} else {
			p.replyStatusToClient(proto.OpStatusInconsistent)
		}
	}
}

func (p *TwoPhaseDestroyProcessor) onDeleteResponse(rc *SSRequestContext) {
	if p.delRequest.isSuccessStatus(rc.ssResponseOpStatus) {
		p.delRequest.onSuccessResponse()
	} else {
		p.delRequest.onErrorResponse()
	}
	p.actIfDoneWithCommitDeleteRepair()
}

func (p *TwoPhaseDestroyProcessor) OnResponseReceived(rc *SSRequestContext) {
	switch rc.opCode {
	case proto.OpCodePrepareDelete:
		switch rc.ssResponseOpStatus {
		case proto.OpStatusNoError, proto.OpStatusAlreadyFulfilled:
			p.onPrepareSuccess(rc)
		case proto.OpStatusNoKey:
			p.onPrepareNoKey(rc)
		case proto.OpStatusBadParam, proto.OpStatusRecordLocked, proto.OpStatusVersionConflict:
			p.onPrepareFailure(rc)
		default:
			glog.Warningf("unexpected prepare opstatus: %s", rc.ssResponseOpStatus.String())
			p.onPrepareFailure(rc)
		}
	case proto.OpCodeCommit:
		switch rc.ssResponseOpStatus {
		case proto.OpStatusNoError,
			proto.OpStatusAlreadyFulfilled:
			p.onCommitSuccess(rc)
		default:
			p.onCommitFailure(rc)
		}
	case proto.OpCodeMarkDelete:
		p.onMarkDeleteResponse(rc)

	case proto.OpCodeAbort:
	case proto.OpCodeDelete:
		p.onDeleteResponse(rc)
	default:
		glog.Warningf("unexpected opcode: %s", rc.opCode.String())

	}
}

func (p *TwoPhaseDestroyProcessor) OnSSTimeout(rc *SSRequestContext) {
	switch rc.opCode {
	case proto.OpCodePrepareDelete:
		p.onPrepareFailure(rc)
	case proto.OpCodeCommit:
		p.onCommitFailure(rc)
	case proto.OpCodeRepair:
		p.onRepairFailure(rc)
	case proto.OpCodeMarkDelete:
		p.markDelRequest.onTimeout()
	case proto.OpCodeAbort:
	case proto.OpCodeDelete:
		p.delRequest.onTimeout()
	default:
		glog.Warningf("unexpected opcode %s", rc.opCode.String())
	}
}

func (p *TwoPhaseDestroyProcessor) OnSSIOError(rc *SSRequestContext) {
	p.pendingResponses[rc.ssIndex] = nil
	switch rc.opCode {
	case proto.OpCodePrepareDelete:
		p.onPrepareFailure(rc) ///TODO to a new OpStatus
	case proto.OpCodeCommit:
		p.onCommitFailure(rc)
	case proto.OpCodeRepair:
		p.onRepairFailure(rc)
	case proto.OpCodeMarkDelete:
		p.markDelRequest.onIOError()
	case proto.OpCodeAbort:
	case proto.OpCodeDelete:
		p.delRequest.onIOError()
	default:
		glog.Warningf("unexpected opcode %s", rc.opCode.String())
	}
}

func (p *TwoPhaseDestroyProcessor) errorResponseOpStatus() (st proto.OpStatus) {
	st = p.TwoPhaseProcessor.errorPrepareResponseOpStatus()
	if st == proto.OpStatusNoStorageServer {
		panic("noss")
	}
	return
}
