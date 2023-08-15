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
	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/paypal/junodb/pkg/logging"
	"github.com/paypal/junodb/pkg/proto"
)

// SUCCESS: NoError, NoKey, MarkedDelete

var _ IOnePhaseProcessor = (*GetProcessor)(nil)

type GetProcessor struct {
	OnePhaseProcessor

	repair               RequestAndStats
	numNoKey             int
	numTTLExtendFailures int
}

func NewGetProcessor() *GetProcessor {
	p := &GetProcessor{
		OnePhaseProcessor: OnePhaseProcessor{
			ssRequestOpCode: proto.OpCodeRead,
		},
	} //proto.OpCodeGet
	p.self = p
	return p
}

func (p *GetProcessor) Init() {
	p.OnePhaseProcessor.Init()
	p.repair.init()
	p.numNoKey = 0
	p.numTTLExtendFailures = 0
}

func (p *GetProcessor) sendRepair(ssIndex uint32) {
	if p.repair.isSet == false {
		if p.request.mostUpdatedOkResponse == nil {
			glog.Error("No most updated OK response")
			return
		}
		opMsg := p.request.mostUpdatedOkResponse.ssRequest.ssRespOpMsg
		opMsg.SetAsRequest()
		opMsg.SetOpCode(proto.OpCodeRepair)
		opMsg.SetShardId(p.shardId)
		p.repair.setFromOpMsg(&opMsg)
	}
	p.send(&p.repair, ssIndex)
}

func (p *GetProcessor) succeeded() bool {
	return p.request.getNumSuccessResponse() > 0 && (p.request.getNumSuccessResponse()+p.numNoKey+p.numTTLExtendFailures) >= confNumWrites
}

func (p *GetProcessor) replyToClientAndRepair() {
	if !p.hasRepliedClient {
		st := p.request.mostUpdatedOkResponse.ssRequest.ssRespOpMsg.GetOpStatus()
		if st == proto.OpStatusKeyMarkedDelete {
			opMsg := p.request.mostUpdatedOkResponse.ssRequest.ssRespOpMsg
			opMsg.SetAsRequest()
			opMsg.SetOpCode(proto.OpCodeMarkDelete)
			opMsg.SetShardId(p.shardId)
			var markDelMsg proto.RawMessage
			if LOG_DEBUG {
				if len(opMsg.GetOriginatorRequestID()) != 16 {
					glog.DebugInfof("oid not set in read response. rid=%s", p.requestID)
				}
			}
			opMsg.Encode(&markDelMsg)
			for i := 0; i < p.request.getNumSuccessResponse(); i++ {
				t := &p.request.successResponses[i]
				if t != p.request.mostUpdatedOkResponse {
					ssResp := &t.ssRequest.ssRespOpMsg
					if ssResp.GetOpStatus() != proto.OpStatusKeyMarkedDelete ||
						ssResp.GetVersion() != opMsg.GetVersion() ||
						ssResp.GetOriginatorRequestID().Equal(opMsg.GetOriginatorRequestID()) == false ||
						t.ssRequest.ssRespOpMsg.GetTimeToLive() != opMsg.GetTimeToLive() ||
						t.ssRequest.ssRespOpMsg.GetCreationTime() != opMsg.GetCreationTime() {
						p.sendMessage(&markDelMsg, p.request.successResponses[i].ssRequest.ssIndex)
					}
				}
			}
			for i := 0; i < p.request.getNumErrorResponse(); i++ {
				st := p.request.errorResponses[i].ssRequest.ssResponseOpStatus
				if st != proto.OpStatusNoKey {
					p.sendMessage(&markDelMsg, p.request.errorResponses[i].ssRequest.ssIndex)
				}
			}
			p.replyStatusToClient(proto.OpStatusNoKey)
		} else {
			resp := &p.request.mostUpdatedOkResponse.ssRequest.ssRespOpMsg
			if LOG_DEBUG {
				b := logging.NewKVBufferForLog()
				b.AddOpStatus(resp.GetOpStatus()).AddVersion(resp.GetVersion()).AddTTL(resp.GetTimeToLive()).AddOriginator(resp.GetOriginatorRequestID())
				glog.DebugInfof("mostupdatedOkResp: %v", b)
			}
			for i := 0; i < p.request.getNumSuccessResponse(); i++ {
				t := &p.request.successResponses[i]
				if t != p.request.mostUpdatedOkResponse {
					m := &t.ssRequest.ssRespOpMsg
					if resp.GetCreationTime() != m.GetCreationTime() ||
						resp.GetTimeToLive() != m.GetTimeToLive() ||
						resp.GetVersion() != m.GetVersion() ||
						resp.GetLastModificationTime() != m.GetLastModificationTime() ||
						!resp.GetOriginatorRequestID().Equal(m.GetOriginatorRequestID()) ||
						!resp.GetPayload().Equal(m.GetPayload()) {
						//	; recordMostUpdatedThan(&p.request.mostUpdatedOkResponse.ssRequest.ssRespOpMsg, &t.ssRequest.ssRespOpMsg) {
						//	resp.PrettyPrint(os.Stdout)
						//	m.PrettyPrint(os.Stdout)
						p.sendRepair(p.request.successResponses[i].ssRequest.ssIndex)
					}
				}
			}
			for i := 0; i < p.request.getNumErrorResponse(); i++ {
				p.sendRepair(p.request.errorResponses[i].ssRequest.ssIndex)
			}
			p.replyToClient(p.request.mostUpdatedOkResponse)
		}
	}
}

func (p *GetProcessor) onSuccess(rc *SSRequestContext) {
	p.OnePhaseProcessor.onSuccess(rc)

	if p.succeeded() {
		p.replyToClientAndRepair()
	}
}

func (p *GetProcessor) failed() bool {
	return (p.request.getNumErrorResponse()+p.request.getNumIOAndTimeout()-p.numNoKey-p.numTTLExtendFailures >= confNumWrites || p.numNoKey >= confNumWrites)
}

func (p *GetProcessor) onNoKey(rc *SSRequestContext) {
	p.request.onFailure(rc)
	p.numNoKey++
	if p.succeeded() {
		p.replyToClientAndRepair()
	} else if p.failed() {
		p.replyStatusToClient(p.errorResponseOpStatus())
	}
}

func (p *GetProcessor) onFailToExtendTTL(rc *SSRequestContext) {
	p.request.onFailure(rc)
	p.numTTLExtendFailures++
	if p.succeeded() {
		p.replyToClientAndRepair()
	} else if p.failed() {
		p.replyStatusToClient(p.errorResponseOpStatus())
	}
}

func (p *GetProcessor) onFailure(rc *SSRequestContext) {
	p.request.onFailure(rc)
	if p.succeeded() {
		p.replyToClientAndRepair()
	} else if p.failed() {
		p.replyStatusToClient(p.errorResponseOpStatus())
	} else {
		p.sendRequest()
	}
}

func (p *GetProcessor) OnResponseReceived(rc *SSRequestContext) {
	if rc.opCode == proto.OpCodeRead {
		switch rc.ssResponseOpStatus {
		case proto.OpStatusNoError:
			p.onSuccess(rc)
			return
		case proto.OpStatusNoKey:
			p.onNoKey(rc)
		case proto.OpStatusBadParam:
			p.onFailure(rc)
		case proto.OpStatusKeyMarkedDelete:
			p.onSuccess(rc)
		case proto.OpStatusSSReadTTLExtendErr:
			p.onFailToExtendTTL(rc)
		default:
			glog.Infof("unexpected response. %s %s", rc.opCode.String(),
				rc.ssResponseOpStatus.String())
			p.onFailure(rc)
		}
	}
}

func (p *GetProcessor) OnSSTimeout(rc *SSRequestContext) {
	p.onFailure(rc) ///TODO proto.OpStatusNoStorageServer)
}

func (p *GetProcessor) OnSSIOError(rc *SSRequestContext) {
	p.onFailure(rc) ///TODO proto.OpStatusNoStorageServer)
}

func (p *GetProcessor) errorResponseOpStatus() (st proto.OpStatus) {
	if p.request.getNumIOAndTimeout() > confMaxNumFailures {
		if p.request.getNumNoStageErrors() > confMaxNumFailures {
			st = proto.OpStatusNoStorageServer
		} else {
			st = proto.OpStatusBusy
		}
		return
	}
	st = proto.OpStatusNoKey
	for i := 0; i < p.request.getNumErrorResponse(); i++ {
		st = p.request.errorResponses[i].ssRequest.ssRespOpMsg.GetOpStatus()
		if st == proto.OpStatusNoError {
			glog.Fatalf("i=%d %v wrong status", i, p.request.errorResponses[i])
		}
		switch st {
		case proto.OpStatusNoKey:
			return
		default:
		}
	}
	if LOG_VERBOSE {
		glog.VerboseInfof("status: %s", st.String())
	}

	return
}
