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
	"juno/third_party/forked/golang/glog"

	"juno/pkg/debug"
	"juno/pkg/errors"
	"juno/pkg/proto"
)

type RequestAndStats struct {
	raw                 proto.RawMessage
	isSet               bool
	numSent             uint8 //successfully sent
	numPending          uint8
	numFailToSend       uint8
	numFailToSendBusy   uint8 //how much "FailToSend" (numFailToSend) is because of busy
	numFailToSendNoConn uint8 //how much "FailToSend" (numFailToSend) is because of no available connection
	numIOError          uint8 //IO Read Error
	numTimeout          uint8
	numSuccessResponse  uint8
	numErrorResponse    uint8
	funcIsSuccess       func(proto.OpStatus) bool
}

///TODO may just change to *SSRequestContext
type ResponseWrapper struct {
	ssRequest *SSRequestContext
}

type OnePhaseRequestAndStats struct {
	RequestAndStats
	successResponses []ResponseWrapper
	errorResponses   []ResponseWrapper

	nextSSIndex           uint32
	mostUpdatedOkResponse *ResponseWrapper
}

func (r *RequestAndStats) init() {
	r.isSet = false
	r.numSent = 0
	r.numFailToSend = 0
	r.numFailToSendBusy = 0
	r.numFailToSendNoConn = 0
	r.numPending = 0
	r.numIOError = 0
	r.numTimeout = 0
	r.numSuccessResponse = 0
	r.numErrorResponse = 0
	r.raw.ReleaseBuffer()
}

func (r *RequestAndStats) verboseLogCounters() {
	glog.VerboseInfof("failSent=%d,sent=%d,pending=%d,ok=%d,err=%d,timeout=%d,IOErr=%d",
		r.numFailToSend,
		r.numSent, r.numPending, r.numSuccessResponse, r.numErrorResponse,
		r.numTimeout, r.numIOError)
}

func (r *RequestAndStats) isSuccessStatus(st proto.OpStatus) bool {
	if r.funcIsSuccess != nil {
		return r.funcIsSuccess(st)
	} else {
		return (st == proto.OpStatusNoError || st == proto.OpStatusAlreadyFulfilled)
	}
}

func (r *RequestAndStats) setShardId(shid uint16) error {
	return proto.SetShardId(&r.raw, shid)
}

func (r *RequestAndStats) setOpCode(opcode proto.OpCode) error {
	return proto.SetOpCode(&r.raw, opcode)
}

func (r *RequestAndStats) setFromOpMsg(opmsg *proto.OperationalMessage) error {
	err := opmsg.Encode(&r.raw)
	if err != nil {
		glog.Error(err)
		if debug.DEBUG {
			panic("")
		}
	}
	r.isSet = (err == nil)
	return err
}

func (r *RequestAndStats) resetFromOpMsg(opmsg *proto.OperationalMessage) (err error) {
	r.raw.Reset()
	err = opmsg.Encode(&r.raw)
	r.isSet = (err == nil)
	return
}

func (r *RequestAndStats) onSent() {
	r.numPending++
	r.numSent++
}

func (r *RequestAndStats) onFailToSend(err *errors.Error) {
	r.numFailToSend++
	switch err.ErrNo() {
	case errors.KErrBusy:
		r.numFailToSendBusy++
	case errors.KErrNoConnection:
		r.numFailToSendNoConn++
	}
}

func (r *RequestAndStats) onSuccessResponse() {
	r.numSuccessResponse++
	r.numPending--
}

func (r *RequestAndStats) onErrorResponse() {
	r.numErrorResponse++
	r.numPending--
}

func (r *RequestAndStats) onTimeout() {
	r.numTimeout++
	r.numPending--
}

func (r *RequestAndStats) onIOError() {
	r.numIOError++
	r.numPending--
}

func (r *RequestAndStats) hasNoPending() bool {
	return (r.numPending == 0)
}

func (r *RequestAndStats) respSuccessForAll() bool {
	return (r.numSent+r.numFailToSend == r.numSuccessResponse)
}

func (r *RequestAndStats) getNumSuccessResponse() int {
	return int(r.numSuccessResponse)
}

func (r *RequestAndStats) getNumErrorResponse() int {
	return int(r.numErrorResponse)
}

func (r *RequestAndStats) getNumIOAndTimeout() int {
	return int(r.numIOError + r.numTimeout + r.numFailToSend)
}

func (r *RequestAndStats) getNumNoStageErrors() int {
	return int(r.numIOError + r.numFailToSendNoConn)
}

func (r *RequestAndStats) getNumErrors() int {
	return int(r.numErrorResponse + r.numIOError + r.numTimeout + r.numFailToSend)
}

func (r *OnePhaseRequestAndStats) init() {
	r.RequestAndStats.init()
	if r.successResponses == nil {
		r.successResponses = make([]ResponseWrapper, confNumZones)
	}
	if r.errorResponses == nil {
		r.errorResponses = make([]ResponseWrapper, confNumZones)
	}

	r.mostUpdatedOkResponse = nil

	r.nextSSIndex = 0
}

type CommitRequestAndStats struct {
	RequestAndStats
	opMsg                   proto.OperationalMessage
	noErrResponse           ResponseWrapper
	ssIndicesOfFailedCommit []uint32
}

func (r *CommitRequestAndStats) init() {
	r.RequestAndStats.init()
	if r.ssIndicesOfFailedCommit == nil {
		r.ssIndicesOfFailedCommit = make([]uint32, 0, confNumZones)
	} else {
		r.ssIndicesOfFailedCommit = r.ssIndicesOfFailedCommit[:0]
	}

	r.noErrResponse.ssRequest = nil
}

func (s *OnePhaseRequestAndStats) onSuccess(rc *SSRequestContext) {
	switch rc.state {
	case stSSResponseReceived:
		t := &s.successResponses[s.numSuccessResponse]
		t.ssRequest = rc
		s.onSuccessResponse()
		if rc.ssResponse == nil {
			glog.Error("SSRequestContext.ssResponse is nil")
		}
		if s.mostUpdatedOkResponse == nil {
			s.mostUpdatedOkResponse = t
		} else {
			if recordMostUpdatedThan(&t.ssRequest.ssRespOpMsg, &s.mostUpdatedOkResponse.ssRequest.ssRespOpMsg) {
				s.mostUpdatedOkResponse = t
			}
		}
	case stSSRequestIOError, stSSResponseIOError:
		s.onIOError()
	case stSSRequestTimeout:
		s.onTimeout()
	default:
		glog.Warning("Unexpected rc.state: ", rc.state)
	}
}

func (s *OnePhaseRequestAndStats) onFailure(rc *SSRequestContext) {
	switch rc.state {
	case stSSResponseReceived:
		if rc.ssResponseOpStatus == proto.OpStatusNoError {
			glog.Fatal("ok to err responses")
		}
		t := &s.errorResponses[s.numErrorResponse]
		t.ssRequest = rc
		if rc.ssResponse == nil {
			glog.Error("SSRequestContext.ssResponse is nil")
		}
		s.onErrorResponse()
	case stSSRequestIOError, stSSResponseIOError:
		s.onIOError()
	case stSSRequestTimeout:
		s.onTimeout()
	default:
		glog.Warning("Unexpected rc.state: ", rc.state)
	}
}
