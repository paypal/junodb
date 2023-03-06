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

	"juno/third_party/forked/golang/glog"

	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
)

type IOnePhaseProcessor interface {
	IRequestProcessor
	succeeded() bool
	failed() bool
	sendRequest()
	onSuccess(rc *SSRequestContext)
	errorResponseOpStatus() proto.OpStatus
}

type OnePhaseProcessor struct {
	ProcessorBase
	request         OnePhaseRequestAndStats
	ssRequestOpCode proto.OpCode
}

type ITwoPhaseProcessor interface {
	IRequestProcessor
	onPrepareSuccess(rc *SSRequestContext)
	prepareSucceeded() bool
	prepareFailed() bool
	commitSucceeded() bool
	sendCommit(ssIndex uint32)
	sendAbort(ssIndex uint32)
	sendRepair(ssIndex uint32)
	sendPrepareRequest()
	abortSucceededPrepares()
	//Returns the OpStatus to Client
	errorPrepareResponseOpStatus() proto.OpStatus
}

type twoPhaseProcessorState uint8

const (
	stTwoPhaseProcInit twoPhaseProcessorState = iota
	stTwoPhaseProcPrepare
	stTwoPhaseProcCommit
	stTwoPhaseProcAbort
)

type TwoPhaseProcessor struct {
	ProcessorBase
	prepareOpCode proto.OpCode
	prepare       OnePhaseRequestAndStats
	state         twoPhaseProcessorState

	numBadRequestID int
	commit          CommitRequestAndStats

	abort  RequestAndStats
	repair RequestAndStats
}

func (p *OnePhaseProcessor) onSuccess(rc *SSRequestContext) {
	p.request.onSuccess(rc)
}

func (p *OnePhaseProcessor) OnSSTimeout(st *SSRequestContext) {
	glog.Warning("SS Timeout. ss: ", st.ssIndex)
}

func (p *OnePhaseProcessor) succeeded() bool {
	return p.request.getNumSuccessResponse() >= confNumWrites
}

func (p *OnePhaseProcessor) failed() bool {
	return p.request.getNumErrorResponse()+p.request.getNumIOAndTimeout() > confMaxNumFailures
}

func (p *OnePhaseProcessor) setInitSSRequest() bool {
	p.request.raw.ShallowCopy(p.requestContext.GetMessage())
	return true
}

func (p *OnePhaseProcessor) sendInitRequests() {

	if p.self.setInitSSRequest() {
		if err := p.request.setOpCode(p.ssRequestOpCode); err != nil {
			p.replyStatusToClient(proto.OpStatusBadMsg) // TODO revisit
			return
		}

		for i := 0; i < p.ssGroup.numAvailableSSs && int(p.request.numSent) < confNumWrites && p.request.getNumIOAndTimeout() < confNumWrites; i++ {
			p.sendRequest()
		}
		if p.numSSRequestSent < confNumWrites {
			if p.request.numFailToSend == p.request.numFailToSendNoConn {
				p.replyStatusToClient(proto.OpStatusNoStorageServer)
			} else {
				p.replyStatusToClient(proto.OpStatusBusy)
			}
		}
	} else {
		p.replyStatusToClient(proto.OpStatusBadMsg) // TODO revisit
	}
}

func (p *TwoPhaseProcessor) setInitSSRequest() bool {
	if confEncryptionEnabled && p.clientRequest.GetPayload().GetLength() != 0 && p.clientRequest.GetPayload().GetPayloadType() == proto.PayloadTypeClear {
		opMsg := p.clientRequest
		if err := opMsg.GetPayload().Encrypt(proto.PayloadTypeEncryptedByProxy); err != nil {
			errmsg := fmt.Sprintf("err=%s", err.Error())
			glog.Error(errmsg)
			if cal.IsEnabled() {
				calLogReqProcError(kEncrypt, []byte(errmsg))
			}
			p.replyStatusToClient(proto.OpStatusInternal)
			return false
		}
		if err := p.prepare.resetFromOpMsg(&opMsg); err != nil {
			///TODO log
			return false
		}
	} else {
		raw := &p.prepare.raw
		raw.ShallowCopy(p.requestContext.GetMessage())
	}
	p.prepare.setShardId(p.shardId)
	return true
}

func (p *TwoPhaseProcessor) sendInitRequests() {
	p.state = stTwoPhaseProcPrepare
	if p.self.setInitSSRequest() {
		if err := p.prepare.setOpCode(p.prepareOpCode); err != nil {
			p.replyStatusToClient(proto.OpStatusBadMsg) // TODO revisit
			return
		}
		for i := 0; i < p.ssGroup.numAvailableSSs && p.numSSRequestSent < confNumWrites && p.prepare.getNumIOAndTimeout() < confNumWrites; i++ {
			p.sendPrepareRequest()
		}
		if p.numSSRequestSent < confNumWrites {
			p.replyStatusToClient(proto.OpStatusNoStorageServer)
		}
	} else {
		p.replyStatusToClient(proto.OpStatusBadMsg) // TODO revisit
	}
}

func (p *TwoPhaseProcessor) prepareSucceeded() bool {
	if LOG_VERBOSE {
		glog.VerboseInfof("#OkResp: %d", p.prepare.getNumSuccessResponse())
	}
	return p.prepare.getNumSuccessResponse() >= confNumWrites
}

func (p *TwoPhaseProcessor) prepareFailed() bool {
	return p.prepare.getNumErrorResponse()+p.prepare.getNumIOAndTimeout() > confMaxNumFailures
}

func (p *TwoPhaseProcessor) commitSucceeded() bool {
	return (p.commit.getNumSuccessResponse() > 0 && (p.commit.getNumSuccessResponse()+p.repair.getNumSuccessResponse() >= confNumWrites))
}

func (p *TwoPhaseProcessor) commitFailed() bool {
	return (p.commit.getNumErrorResponse()+p.repair.getNumErrorResponse() >= confNumWrites)
	// confNumZones-confNumWrites
}

func (p *TwoPhaseProcessor) OnSSTimeout(st *SSRequestContext) {
	glog.Warning("SS Timeout. ss: ", st.opCode.String(), " ", st.ssIndex)
}

func (p *OnePhaseProcessor) sendRequest() {
	if int(p.request.nextSSIndex) < p.ssGroup.numAvailableSSs {
		p.send(&p.request.RequestAndStats, p.request.nextSSIndex)
		p.request.nextSSIndex++
	}
}

func (p *OnePhaseProcessor) Init() {
	p.ProcessorBase.Init()
	p.request.init()
}

func (p *TwoPhaseProcessor) Init() {
	p.ProcessorBase.Init()
	p.prepare.init()
	p.state = stTwoPhaseProcInit

	p.commit.init()
	p.abort.init()
	p.repair.init()

	p.numBadRequestID = 0
}

func (s *TwoPhaseProcessor) onCommitSuccess(rc *SSRequestContext) {
	s.commit.onSuccessResponse()
	if s.commit.noErrResponse.ssRequest == nil {
		rawResp := rc.ssResponse.GetMessage()

		if rc.ssResponseOpStatus != proto.OpStatusNoError {
			if err := proto.SetOpStatus(rawResp, proto.OpStatusNoError); err != nil {
				glog.Error("Couldn't set Opstatus. ", err)
			}
		}
		s.commit.noErrResponse.ssRequest = rc
	}
}

func (s *TwoPhaseProcessor) onCommitFailure(ssIndex uint32, opstatus proto.OpStatus) {
	s.commit.onErrorResponse()
	s.commit.ssIndicesOfFailedCommit = append(s.commit.ssIndicesOfFailedCommit, ssIndex)
}

func (p *TwoPhaseProcessor) sendCommit(ssIndex uint32) {
	p.send(&p.commit.RequestAndStats, ssIndex)
}

func (p *TwoPhaseProcessor) sendCommits() {
	for i := 0; i < p.prepare.getNumSuccessResponse(); i++ {
		ssIndex := p.prepare.successResponses[i].ssRequest.ssIndex
		///TODO valid ssIndex
		p.sendCommit(ssIndex)
	}
}

func (p *TwoPhaseProcessor) setAbortMsg() {
	var opMsg proto.OperationalMessage

	p.setSSOpRequestFromClientRequest(&opMsg, proto.OpCodeAbort, 0, false)
	p.abort.setFromOpMsg(&opMsg)
}

func (p *TwoPhaseProcessor) sendAbort(ssIndex uint32) {
	if p.abort.isSet == false {
		p.setAbortMsg()
	}

	p.send(&p.abort, ssIndex)
}

func (p *TwoPhaseProcessor) sendRepair(ssIndex uint32) {
	if p.repair.isSet == false {
		opMsg := p.commit.opMsg
		opMsg.SetOpCode(proto.OpCodeRepair)
		opMsg.SetPayload(p.clientRequest.GetPayload())
		if err := p.repair.setFromOpMsg(&opMsg); err != nil {
			glog.Error(err)
			p.replyStatusToClient(proto.OpStatusInconsistent) // TODO revisit
			return
		}
	}
	if p.send(&p.repair, ssIndex) == false {
		p.replyStatusToClient(proto.OpStatusInconsistent) // TODO revisit
	}
}

func (p *TwoPhaseProcessor) sendPrepareRequest() {
	if p.prepare.nextSSIndex < uint32(p.ssGroup.numAvailableSSs) {
		if p.state == stTwoPhaseProcPrepare {
			p.send(&p.prepare.RequestAndStats, p.prepare.nextSSIndex)
			p.prepare.nextSSIndex++
		}
	}
}

func (p *TwoPhaseProcessor) abortSucceededPrepares() {
	if p.state != stTwoPhaseProcAbort {
		p.state = stTwoPhaseProcAbort
		if p.abort.isSet == false {
			p.setAbortMsg()
		}
		for i := 0; i < p.prepare.getNumSuccessResponse(); i++ {
			p.sendAbort(p.prepare.successResponses[i].ssRequest.ssIndex)
		}
	}
}

func (p *TwoPhaseProcessor) sendRepairs() {
	if len(p.commit.ssIndicesOfFailedCommit) != 0 {
		for _, ssIndex := range p.commit.ssIndicesOfFailedCommit {
			p.sendRepair(ssIndex)
		}
		p.commit.ssIndicesOfFailedCommit = p.commit.ssIndicesOfFailedCommit[:0]
	}
}

func (p *TwoPhaseProcessor) errorPrepareResponseOpStatus() (st proto.OpStatus) {
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

func (p *TwoPhaseProcessor) onRepairSuccess(rc *SSRequestContext) {
	p.repair.onSuccessResponse()
	if p.commitSucceeded() {
		p.replyToClient(&p.commit.noErrResponse)
	}
}

func (p *TwoPhaseProcessor) onRepairFailure(rc *SSRequestContext) {
	p.repair.onErrorResponse()
	if !p.hasRepliedClient {
		if cal.IsEnabled() {
			buf := logging.NewKVBuffer()
			writeBasicSSRequestInfo(buf, rc.opCode, int(rc.ssIndex), p.ssGroup.processors[rc.ssIndex].GetConnInfo(), &p.ProcessorBase)
			calLogReqProcEvent(kInconsistent, buf.Bytes())
		}
		p.replyStatusToClient(proto.OpStatusInconsistent) ///TODO
	}
}

func (p *TwoPhaseProcessor) replyStatusToClient(st proto.OpStatus) {
	if st == proto.OpStatusInconsistent && p.commit.getNumSuccessResponse() > 0 && p.commit.noErrResponse.ssRequest != nil {
		p.commit.noErrResponse.ssRequest.ssRespOpMsg.SetAsResponse()
		p.commit.noErrResponse.ssRequest.ssRespOpMsg.SetOpStatus(proto.OpStatusInconsistent)
		payload := p.commit.noErrResponse.ssRequest.ssRespOpMsg.GetPayload()
		// follow the sendRepairs
		if payload != nil || payload.GetLength() == 0 {
			p.commit.noErrResponse.ssRequest.ssRespOpMsg.SetPayload(p.clientRequest.GetPayload())
		}
		p.replyToClient(&p.commit.noErrResponse)
	} else {
		p.ProcessorBase.replyStatusToClient(st)
	}
}

///TODO to be reviewed
func recordMostUpdatedThan(m1, m2 *proto.OperationalMessage) bool {
	lmt1 := m1.GetLastModificationTime()
	lmt2 := m2.GetLastModificationTime()
	if lmt1 == 0 || lmt2 == 0 {
		ct1 := m1.GetCreationTime()
		ct2 := m2.GetCreationTime()

		if ct1 > ct2 {
			return true
		} else if ct1 == ct2 {
			oid1 := m1.GetOriginatorRequestID()
			oid2 := m2.GetOriginatorRequestID()
			v1 := m1.GetVersion()
			v2 := m2.GetVersion()
			if (!oid1.IsSet()) || (!oid2.IsSet()) {
				if v1 > v2 {
					return true
				} else if v1 == v2 {
					if m1.GetTimeToLive() > m2.GetTimeToLive() {
						return true
					}
				}

				return false
			} else {
				if oid1.Equal(oid2) {
					if v1 > v2 {
						return true
					} else if v1 == v2 {
						if m1.GetTimeToLive() > m2.GetTimeToLive() {
							return true
						}
					}
				} else {
					if LOG_DEBUG {
						glog.DebugInfof("oid not same. oid1: %s  oid2: %s", oid1.String(), oid2.String())
						glog.DebugInfof("m1.lmt: %d  m2.lmt: %d", m1.GetLastModificationTime(), m2.GetLastModificationTime())
					}
					if v1 > v2 {
						return true
					} else if v1 == v2 {
						if m1.GetTimeToLive() > m2.GetTimeToLive() {
							return true
						}
					}
				}
			}
		}
	} else {
		return lmt1 > lmt2
	}

	return false
}
