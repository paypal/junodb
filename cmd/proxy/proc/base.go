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
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/config"
	"juno/cmd/proxy/replication"
	proxystats "juno/cmd/proxy/stats"
	"juno/pkg/cluster"
	"juno/pkg/debug"
	"juno/pkg/errors"
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
	stSSRequestInit ssReqContextState = iota
	stSSRequestSent
	stSSResponseReceived
	stSSRequestIOError
	stSSResponseIOError
	stSSRequestTimeout
	stRequestTimeout
	stRequestCancelled
)

type (
	ssReqContextState uint8

	// virtual functions, to trigger polymorphism, call via p.self pointer.
	IRequestProcessor interface {
		Init()
		sendInitRequests()

		OnResponseReceived(st *SSRequestContext)
		OnSSTimeout(st *SSRequestContext)
		OnSSIOError(st *SSRequestContext)

		//Return true if it has completed all the SS requests and can be cached
		Process(reqCtx io.IRequestContext) bool

		setInitSSRequest() bool
		validateSSResponse(st *SSRequestContext) bool

		needApplyUDF() bool
		applyUDF(opmsg *proto.OperationalMessage)
	}

	SSRequestContext struct {
		timeToExpire     time.Time
		timeReqSent      time.Time
		timeRespReceived time.Time // when state is changed from stSSRequestSent to others

		ssRequest          io.IRequestContext
		ssResponse         io.IResponseContext
		ssRespOpMsg        proto.OperationalMessage
		opCode             proto.OpCode
		ssResponseOpStatus proto.OpStatus
		ssIndex            uint32
		state              ssReqContextState
	}

	ProxyInResponseContext struct {
		io.InboundResponseContext
		timeReceived   time.Time
		stats          stats.ProcStat
		forReplication bool
		logData        *logging.KeyValueBuffer
		callData       *logging.KeyValueBuffer
	}
	SSGroup struct {
		processors      []*cluster.OutboundSSProcessor
		procIndices     []int
		numAvailableSSs int
		numBrokenSSs    int
	}
	ProcessorBase struct {
		ctx           context.Context
		clientRequest proto.OperationalMessage
		//repRequest         proto.OperationalMessage
		requestContext io.IRequestContext
		chSSResponse   chan io.IResponseContext
		ssGroup        SSGroup
		shardId        uint16
		requestID      string

		numSSRequestSent      int
		numSSResponseReceived int
		numSSResponseIOError  int

		ssRequestContexts []SSRequestContext

		pendingResponses     []*SSRequestContext
		pendingResponseQueue []*SSRequestContext
		responseTimer        *util.TimerWrapper
		hasRepliedClient     bool

		self IRequestProcessor
	}
)

func (g *SSGroup) init() {
	if g.processors == nil {
		g.processors = make([]*cluster.OutboundSSProcessor, confNumZones)
		g.procIndices = make([]int, confNumZones)
	} else {
		for i := 0; i < confNumZones; i++ {
			g.processors[i] = nil
			g.procIndices[i] = 0
		}
	}
	g.numAvailableSSs = 0
	g.numBrokenSSs = 0
}

func (g *SSGroup) getProcessors(key []byte) (shardId shard.ID, ok bool) {
	shardId, g.numAvailableSSs = cluster.GetShardMgr().GetSSProcessors(key, confNumWrites, g.processors, g.procIndices)
	g.numBrokenSSs = confNumZones - g.numAvailableSSs
	ok = g.numAvailableSSs >= confNumWrites
	return
}

// Overwrite InboundResponseContext::OnComplete() to
// send some stats to statelog
func (r *ProxyInResponseContext) OnComplete() {

	// Send to StateLog
	rht := time.Since(r.timeReceived)
	rhtus := int(rht / 1000)
	if cal.IsEnabled() {
		var calData []byte = nil
		if r.logData != nil {
			// Add call data only if the opertion time exceeded 50 ms or CAL status is not SUCCESS
			if rhtus > 50000 || logging.CalStatus(r.GetOpStatus()).CalStatus() != cal.StatusSuccess {
				r.logData.Buffer.Write(r.callData.Bytes())
			}
			r.logData.AddRequestHandleTime(rhtus)
			calData = r.logData.Bytes()
		}
		opcode := r.stats.Opcode
		if opcode == proto.OpCodeGet && r.stats.RequestTimeToLive > 0 {
			opcode = proto.OpCodeMockGetExtendTTL
		}
		// Log API transaction only if, OTEL is not enabled or its not a GET operation or
		// the operation failed other than nokey.
		if !otel.IsEnabled() || opcode != proto.OpCodeGet ||
			r.GetOpStatus() != proto.OpStatusNoError || r.GetOpStatus() != proto.OpStatusNoKey {
			logging.LogToCal(opcode, r.GetOpStatus(), rht, calData)
		}
	}
	if otel.IsEnabled() {
		otel.RecordOperation(r.stats.Opcode.String(), r.stats.ResponseStatus.ShortNameString(), int64(rhtus))
	}
	r.stats.OnComplete(uint32(rhtus), r.GetOpStatus())
	proxystats.SendProcState(r.stats)

	// NOTE: call this at last, as the msg will be released after this !!!
	r.InboundResponseContext.OnComplete()
}

func NewProxyInRespose(clientRequest *proto.OperationalMessage, m *proto.RawMessage, receiveTime time.Time, logData *logging.KeyValueBuffer, callData *logging.KeyValueBuffer) (r *ProxyInResponseContext) {

	opCode := clientRequest.GetOpCode()

	r = &ProxyInResponseContext{
		InboundResponseContext: io.InboundResponseContext{},
		timeReceived:           receiveTime,
		forReplication:         clientRequest.IsForReplication(),
		logData:                logData,
		callData:               callData,
	}
	r.stats.Init(clientRequest)
	rawMsg := r.GetMessage()
	*rawMsg = *m
	proto.SetOpCode(rawMsg, opCode)
	rawMsg.SetAsResponse() ///
	rawMsg.SetOpaque(clientRequest.GetOpaque())
	m.GiveUpBufferOwnership()
	return
}

func (p *ProcessorBase) Init() {
	p.chSSResponse = make(chan io.IResponseContext, confNumZones) ///TODO: ...
	if p.ssRequestContexts == nil {
		p.ssRequestContexts = make([]SSRequestContext, 4*confNumZones+confNumZones)
	}
	if p.pendingResponses == nil {
		p.pendingResponses = make([]*SSRequestContext, confNumZones)
	} else {
		for i := 0; i < confNumZones; i++ {
			if p.pendingResponses[i] != nil {
				p.pendingResponses[i].state = stSSRequestInit
				p.pendingResponses[i] = nil
			}
		}
	}
	p.ssGroup.init()
	if p.pendingResponseQueue == nil {
		p.pendingResponseQueue = make([]*SSRequestContext, 0, confNumZones*2)
	} else {
		p.pendingResponseQueue = p.pendingResponseQueue[:0]
	}
	if p.responseTimer == nil {
		p.responseTimer = util.NewTimerWrapper(confSSRequestTimeout)
	} else {
		p.responseTimer.Stop()
	}
	p.hasRepliedClient = false
	p.numSSRequestSent = 0
	p.numSSResponseReceived = 0
	p.numSSResponseIOError = 0
}

func (p *ProcessorBase) needApplyUDF() bool {
	return false
}

func (p *ProcessorBase) applyUDF(opmsg *proto.OperationalMessage) {
}

func (p *ProcessorBase) isDone() bool {
	return (p.numSSRequestSent == p.numSSResponseReceived)
}

func (p *ProcessorBase) chSSTimeout() <-chan time.Time {
	if p.responseTimer == nil {
		return nil
	} else {
		return p.responseTimer.GetTimeoutCh()
	}
}

func (p *ProcessorBase) callStateString(st *SSRequestContext) string {
	var states [3]string

	states[0] = st.opCode.ShortNameString() + strconv.Itoa(p.ssGroup.procIndices[st.ssIndex])
	if !st.timeRespReceived.IsZero() {
		states[2] = strconv.Itoa(int(st.timeRespReceived.Sub(st.timeReqSent) / 1000))
	}

	switch st.state {
	case stSSResponseReceived:
		states[1] = st.ssResponseOpStatus.ShortNameString()
	case stSSRequestSent:
		states[1] = "-"
	case stSSRequestIOError, stSSResponseIOError:
		states[1] = "IOErr"
	case stSSRequestTimeout:
		states[1] = "SsTO"
	case stRequestTimeout:
		states[1] = "RqTO"
	case stRequestCancelled:
		states[1] = "x"
	default:
		states[1] = ""
	}
	return strings.Join(states[:], ":")
}

func (p *ProcessorBase) genLogData(resp *proto.OperationalMessage) (data *logging.KeyValueBuffer, callData *logging.KeyValueBuffer) {
	data = logging.NewKVBuffer()
	callData = logging.NewKVBuffer()
	data.AddOpRequestResponseInfo(&p.clientRequest, resp)
	callData.Write([]byte("&calls="))

	first := true
	for i := 0; i < p.numSSRequestSent; i++ {
		st := &p.ssRequestContexts[i]
		if first {
			first = false
		} else {
			callData.WriteByte(',')
		}
		callData.WriteString(p.callStateString(st))
	}
	return
}

func (p *ProcessorBase) replyToClient(resp *ResponseWrapper) {
	if !p.hasRepliedClient {
		p.hasRepliedClient = true
		if resp != nil && resp.ssRequest != nil {
			opMsg := &resp.ssRequest.ssRespOpMsg
			opstatus := opMsg.GetOpStatus()
			opcode := p.clientRequest.GetOpCode()
			var logData, callData *logging.KeyValueBuffer
			m := resp.ssRequest.ssResponse.GetMessage()
			if opstatus == proto.OpStatusAlreadyFulfilled {
				opstatus = proto.OpStatusNoError
				proto.SetOpStatus(m, proto.OpStatusNoError)
			}
			if cal.IsEnabled() {
				if logData == nil {
					logData, callData = p.genLogData(opMsg)
				}
			}
			if LOG_DEBUG {
				b := logging.NewKVBufferForLog()
				b.AddOpStatus(opstatus).AddVersion(opMsg.GetVersion()).
					AddReqIdString(opMsg.GetRequestIDString()).AddTTL(opMsg.GetTimeToLive()).AddOriginator(opMsg.GetOriginatorRequestID())
				if p.clientRequest.IsForReplication() {
					glog.DebugInfof("RepClient<-: %s %v", "R"+opcode.String(), b)
				} else {
					glog.DebugInfof("Client<-: %s %v", opcode.String(), b)
				}
			}
			payload := opMsg.GetPayload()
			rhtms := uint32(time.Since(p.requestContext.GetReceiveTime()).Milliseconds())
			if payload.GetLength() != 0 && payload.GetPayloadType() == proto.PayloadTypeEncryptedByProxy && !p.clientRequest.IsForReplication() {
				reply := *opMsg
				if err := reply.GetPayload().Decrypt(); err != nil {
					errmsg := fmt.Sprintf("err=%s", err.Error())
					glog.Error(errmsg)
					if cal.IsEnabled() {
						calLogReqProcError(kDecrypt, []byte(errmsg))
					}
					if otel.IsEnabled() {
						otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Operation, kDecrypt}, {otel.Status, otel.StatusError}})
					}
					msg := p.clientRequest.CreateResponse()
					msg.SetOpStatus(proto.OpStatusInternal)
					var raw proto.RawMessage
					msg.Encode(&raw)
					resp := NewProxyInRespose(&p.clientRequest, &raw, p.requestContext.GetReceiveTime(), logData, callData)
					p.requestContext.Reply(resp)
					return

				} else {
					var raw proto.RawMessage
					reply.SetRequestHandlingTime(rhtms)
					if p.self.needApplyUDF() {
						p.self.applyUDF(&reply)
					}
					reply.Encode(&raw)
					response := NewProxyInRespose(&p.clientRequest, &raw, p.requestContext.GetReceiveTime(), logData, callData)
					p.requestContext.Reply(response)
				}

			} else {
				// please reference
				//     func (p *TwoPhaseProcessor) replyStatusToClient(st proto.OpStatus)
				// for why we need to do this here
				if resp.ssRequest.ssRespOpMsg.GetOpStatus() == proto.OpStatusInconsistent ||
					p.self.needApplyUDF() {
					var raw proto.RawMessage
					if p.self.needApplyUDF() {
						p.self.applyUDF(opMsg)
					}
					opMsg.SetRequestHandlingTime(rhtms)
					opMsg.Encode(&raw)
					m = &raw
				} else {
					// Set in RawMessage.
					proto.SetRequestHandlingTime(m, rhtms)
				}
				response := NewProxyInRespose(&p.clientRequest, m, p.requestContext.GetReceiveTime(), logData, callData)
				p.requestContext.Reply(response)
			}

			p.replicate(opstatus, resp.ssRequest)
		}
	}
}

func (p *ProcessorBase) replicate(opstatus proto.OpStatus, resp *SSRequestContext) {
	if !replication.Enabled() || p.clientRequest.IsForReplication() || resp == nil {
		return
	}

	opMsg := &resp.ssRespOpMsg
	opcode := p.clientRequest.GetOpCode()
	if (opstatus == proto.OpStatusNoError || opstatus == proto.OpStatusInconsistent) &&
		((opcode != proto.OpCodeGet && opcode != proto.OpCodeUDFGet) || p.clientRequest.GetTimeToLive() > 0) &&
		opMsg.GetCreationTime() != 0 &&
		opMsg.GetVersion() != 0 {
		if LOG_VERBOSE {
			glog.Info("replicate request")
		}

		repRequest := p.clientRequest
		if p.clientRequest.GetOpCode() == proto.OpCodeCreate {
			repRequest.SetOpCode(proto.OpCodeUpdate)
		}

		if p.clientRequest.GetOpCode() == proto.OpCodeUDFGet {
			repRequest.SetOpCode(proto.OpCodeGet)
		}

		if len(opMsg.GetOriginatorRequestID()) != 16 {
			glog.Warningf("oid not set. rid=%s", p.requestID)
		}
		repRequest.SetAsReplication()
		repRequest.SetCreationTime(opMsg.GetCreationTime())
		repRequest.SetVersion(opMsg.GetVersion())
		repRequest.SetLastModificationTime(opMsg.GetLastModificationTime())
		repRequest.SetOriginatorRequestID(opMsg.GetOriginatorRequestID())
		expTime := opMsg.GetExpirationTime()
		repRequest.SetExpirationTime(expTime)
		if confReplicationEncryptionEnabled {
			repRequest.GetPayload().Encrypt(proto.PayloadTypeEncryptedByProxy)
		}
		replication.TheReplicator.SendRequest(&repRequest) //expTime, &repMsg)
		if LOG_DEBUG {
			b := logging.NewKVBufferForLog()
			b.AddReqIdString(opMsg.GetRequestIDString()).AddVersion(opMsg.GetVersion()).AddTTL(p.clientRequest.GetTimeToLive()).AddCreationTime(opMsg.GetCreationTime())
			glog.DebugInfof("Replicate: %s %v", opcode.String(), b)
		}
	} else {
		if LOG_DEBUG {
			glog.DebugInfof("no replicaiton: opcode=%d, opstatus=%d, ttl=%d, create_t=%d, version=%d",
				opcode, opstatus, p.clientRequest.GetTimeToLive(), opMsg.GetCreationTime(), opMsg.GetVersion())
		}
	}
}

func (p *ProcessorBase) replyStatusToClient(st proto.OpStatus) {
	if !p.hasRepliedClient {
		msg := p.clientRequest.CreateResponse()
		msg.SetOpStatus(st)
		var rawMsg proto.RawMessage
		err := msg.Encode(&rawMsg)
		if err != nil {
			glog.Error("Failed to encode response: ", err)
		} else {
			var logData, callData *logging.KeyValueBuffer
			if cal.IsEnabled() {
				logData, callData = p.genLogData(msg)
			}
			resp := NewProxyInRespose(&p.clientRequest, &rawMsg, p.requestContext.GetReceiveTime(), logData, callData)
			if LOG_DEBUG {
				b := logging.NewKVBufferForLog()
				b.AddOpStatus(st).AddVersion(msg.GetVersion()).AddReqIdString(msg.GetRequestIDString())
				if p.clientRequest.IsForReplication() {
					glog.DebugInfof("RepClient<-: %s %v", "R"+msg.GetOpCodeText(), b)
				} else {
					glog.DebugInfof("Client<-: %s %v", msg.GetOpCodeText(), b)
				}

			}
			p.hasRepliedClient = true
			p.requestContext.Reply(resp)
		}
	}
}

func (p *ProcessorBase) setSSOpRequestFromClientRequest(request *proto.OperationalMessage, op proto.OpCode, version uint32, keepValue bool) {
	*request = p.clientRequest
	request.SetOpCode(op)
	request.SetVersion(version)
	request.SetShardId(p.shardId)
	if keepValue == false {
		request.ClearPayload()
	}
}

func (p *ProcessorBase) send(request *RequestAndStats, ssIndex uint32) (ok bool) {
	if err := p.sendMessage(&request.raw, ssIndex); err == nil {
		request.onSent()
		ok = true
	} else {
		request.onFailToSend(err)
	}
	return
}

func (p *ProcessorBase) sendMessage(msg *proto.RawMessage, ssIndex uint32) *errors.Error {
	op, err := proto.GetOpCode(msg)
	if (err != nil) || (!op.IsForStorage()) {
		glog.Errorf("Invalid SS Request: %s err: %s", op.String(), err)
		p.replyStatusToClient(proto.OpStatusBadParam)
		return errors.NewError(err.Error(), errors.KErrInvalid)
	}
	if debug.DEBUG {
		var opReq proto.OperationalMessage
		err = opReq.Decode(msg)
		if err != nil || opReq.GetShardId() != p.shardId {
			calLogReqProcError("corrupt", nil)
			opReq.PrettyPrint(os.Stderr)
			msg.HexDump()
			panic("shard id not set") //TO BE CHANGED/REMOVED BEFORE RELEASE
		}
	}
	if LOG_DEBUG {
		glog.DebugInfof("%s<-: %s rid=%s", p.logStrSsIdx(ssIndex), p.logStrOpCode(op), p.requestID)
	}
	if p.pendingResponses[ssIndex] != nil {
		if LOG_DEBUG {
			glog.DebugInfof("Has pending request %s -> SS[%d]. Ignore this request %s",
				p.pendingResponses[ssIndex].opCode.String(), ssIndex, op.String())
		}
		return errors.NewError("has pending request", errors.KErrInvalid)
	}

	///TODO: to check...
	// SS request has smaller timeout
	//	c, cancelFunc := context.WithTimeout(p.ctx, confSSRequestTimeout)

	st := &p.ssRequestContexts[p.numSSRequestSent]

	st.timeReqSent = time.Now()
	st.timeRespReceived = time.Time{}
	st.ssResponse = nil
	st.ssResponseOpStatus = proto.OpStatusNoError

	// Create a OutboundRequestContext
	st.timeToExpire = st.timeReqSent.Add(confSSRequestTimeout)
	///TODO refactoring may be needed for removing the duplication of SS timeout handling
	st.ssRequest = io.NewOutboundRequestContext(msg, ssIndex, p.ctx, p.chSSResponse, confSSRequestTimeout)

	st.opCode = op
	st.ssIndex = ssIndex
	//pr.state = stSSRequestInit

	if p.responseTimer.IsStopped() {
		p.responseTimer.Reset(confSSRequestTimeout)
	}

	///TODO

	if err := p.ssGroup.processors[ssIndex].SendRequest(st.ssRequest); err == nil {
		st.state = stSSRequestSent
		p.pendingResponses[ssIndex] = st
		p.pendingResponseQueue = append(p.pendingResponseQueue, st)
		p.numSSRequestSent++
	} else {
		if LOG_DEBUG {
			glog.DebugInfof("Failed to send to %s: %s rid=%s", p.logStrSsIdx(ssIndex), p.logStrOpCode(op), p.requestID)
		}
		if cal.IsEnabled() {
			buf := logging.NewKVBuffer()
			writeBasicSSRequestInfo(buf, op, int(ssIndex), p.ssGroup.processors[ssIndex].GetConnInfo(), p)
			errStr := strings.Replace(err.Error(), " ", "_", -1)
			calLogReqProcEvent(fmt.Sprintf("SS_%s", errStr), buf.Bytes())
		}
		return err
	}
	return nil
}

func (p *ProcessorBase) validateInboundRequest(r *proto.OperationalMessage) bool {
	isReplication := r.IsForReplication()
	szKey := len(r.GetKey())
	if szKey == 0 {
		glog.Warningf("invalid key length %d", szKey)
		data := logging.NewKVBuffer()
		data.AddReqIdString(r.GetRequestIDString())
		data.AddInt([]byte("len"), szKey)
		calLogReqProcEvent(kBadParamInvalidKeyLen, data.Bytes())
		if otel.IsEnabled() {
			otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Status, kBadParamInvalidKeyLen}})
		}
		return false
	}
	szNs := len(r.GetNamespace())

	if szNs <= 0 || szNs > confMaxNamespaceLength {
		glog.Warningf("invalid namespace length %d", szNs)
		data := logging.NewKVBuffer()
		data.AddReqIdString(r.GetRequestIDString())
		data.AddInt([]byte("len"), szNs)
		calLogReqProcEvent(kBadParamInvalidNsLen, data.Bytes())
		if otel.IsEnabled() {
			otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Status, kBadParamInvalidNsLen}})
		}
		return false
	}
	ttl := r.GetTimeToLive()
	if isReplication {
		if ttl == 0 && r.GetOpCode() != proto.OpCodeDestroy {
			glog.Warningf("0 TTL for replication request")
			data := logging.NewKVBuffer()
			data.AddReqIdString(r.GetRequestIDString())
			data.AddInt([]byte("ttl"), int(ttl))
			calLogReqProcEvent(kBadParamInvalidTTL, data.Bytes())
			if otel.IsEnabled() {
				otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Status, kBadParamInvalidTTL}})
			}
			return false
		}
	} else {
		limits := config.GetLimits(r.GetNamespace())
		if limits.MaxKeyLength != 0 && szKey > int(limits.MaxKeyLength) {
			data := logging.NewKVBuffer()
			data.AddReqIdString(r.GetRequestIDString())
			data.AddInt([]byte("len"), szKey)
			calLogReqProcEvent(kBadParamInvalidKeyLen, data.Bytes())
			glog.Warningf("limit exceeded: key length %d > %d", szKey, limits.MaxKeyLength)
			if otel.IsEnabled() {
				otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Status, kBadParamInvalidKeyLen}})
			}
			return false
		}
		if limits.MaxTimeToLive != 0 && ttl > limits.MaxTimeToLive {
			data := logging.NewKVBuffer()
			data.AddReqIdString(r.GetRequestIDString())
			data.AddInt([]byte("ttl"), int(ttl))
			calLogReqProcEvent(kBadParamInvalidTTL, data.Bytes())
			glog.Warningf("limit exceeded: TTL %d > %d", ttl, limits.MaxTimeToLive)
			if otel.IsEnabled() {
				otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Status, kBadParamInvalidTTL}})
			}
			return false
		}
		szValue := r.GetPayloadValueLength()
		if limits.MaxPayloadLength != 0 && szValue > limits.MaxPayloadLength {
			data := logging.NewKVBuffer()
			data.AddReqIdString(r.GetRequestIDString())
			data.AddInt([]byte("len"), int(szValue))
			calLogReqProcEvent(kBadParamInvalidValueLen, data.Bytes())
			glog.Warningf("limit exceeded: payload length %d > %d", szValue, limits.MaxPayloadLength)
			if otel.IsEnabled() {
				otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Status, kBadParamInvalidValueLen}})
			}
			return false
		}
	}
	return true
}

func (p *ProcessorBase) Process(request io.IRequestContext) bool {

	p.ctx = request.GetCtx()
	p.requestContext = request
	p.clientRequest = proto.OperationalMessage{}

	if err := p.clientRequest.Decode(request.GetMessage()); err != nil {
		glog.Error("Failed to decode inbound request: ", err)
		p.replyStatusToClient(proto.OpStatusBadMsg)
		p.OnComplete()
		return true
	}

	if p.validateInboundRequest(&p.clientRequest) == false {
		p.replyStatusToClient(proto.OpStatusBadParam)
		p.OnComplete()
		return true
	}

	p.requestID = p.clientRequest.GetRequestIDString()

	shardId, ok := p.ssGroup.getProcessors(p.clientRequest.GetKey())

	if !ok {
		p.replyStatusToClient(proto.OpStatusNoStorageServer)
		glog.Warning("Cannot get channels from Cluster Manager")
		p.OnComplete()
		return true
	}
	if cal.IsEnabled() {
		if p.clientRequest.IsForReplication() {
			logdata := logging.NewKVBuffer()
			logdata.AddRequestID(p.clientRequest.GetRequestID())
			cal.Event(cal.TxnTypeRAPI, p.clientRequest.GetOpCodeText(), cal.StatusSuccess, logdata.Bytes())
		} else {
			appName := p.clientRequest.GetAppName()
			if len(appName) > 0 {
				logdata := logging.NewKVBuffer()
				logdata.AddRequestID(p.clientRequest.GetRequestID()).Add([]byte("src"), p.clientRequest.GetSrcIP().String())

				evName := "CLIENT_INFO"
				if inbCtx, ok := request.(*io.InboundRequestContext); ok {
					if inbCtx.GetListenerType() == io.ListenerTypeTCPwSSL {
						evName = "SSL_CLIENT_INFO"
					}
				}
				cal.Event(evName, string(appName), cal.StatusSuccess, logdata.Bytes())
			}
		}
	}
	if otel.IsEnabled() {
		if p.clientRequest.IsForReplication() {
			otel.RecordCount(otel.RAPI, nil)
		}
	}
	p.shardId = shardId.Uint16()

	if err := proto.SetShardId(p.requestContext.GetMessage(), p.shardId); err != nil {
		p.replyStatusToClient(proto.OpStatusInternal) //shouldn't happen.
		glog.Error("fail to set shardId: ", err)
		return true
	}

	p.self.sendInitRequests()
	done := false

loop:
	for p.isDone() == false {
		select {
		case <-p.ctx.Done():
			if done == false {
				done = true
				if p.ctx.Err() == context.DeadlineExceeded {
					p.OnRequestTimeout()
				} else {
					p.OnCancelled()
				}
			}
			break loop
		case t := <-p.chSSTimeout():
			p.handleSSTimeout(t)
		case respFromSS := <-p.chSSResponse:
			p.onResponseReceived(respFromSS)
		}
	}
	if p.isDone() {
		p.OnComplete()
		return true
	}
	return false
}

func (p *ProcessorBase) onResponseReceived(resp io.IResponseContext) {
	st := p.preprocessAndValidateResponse(resp)
	if st != nil {
		st.timeRespReceived = time.Now()
		p.self.OnResponseReceived(st)
	} else {
		io.ReleaseOutboundResponse(resp)
	}
}

func (p *ProcessorBase) OnComplete() {
	if LOG_VERBOSE {
		p.logStats()
	}
	for i := 0; i < p.numSSRequestSent; i++ {
		st := &p.ssRequestContexts[i]
		if st.ssResponse != nil {
			io.ReleaseOutboundResponse(st.ssResponse)
			st.ssResponse = nil
		}
	}
	p.requestContext.Cancel()
	p.requestContext.OnComplete()
	p.responseTimer.Stop()
	p.self.Init()

}

func (p *ProcessorBase) OnRequestTimeout() {
	if p.hasRepliedClient == false {
		glog.Infof("Request Timeout: %s", p.clientRequest.GetOpCodeText())
		if cal.IsEnabled() {
			b := logging.NewKVBuffer()
			b.AddOpCode(p.clientRequest.GetOpCode()).AddReqIdString(p.requestID)
			calLogReqProcEvent(kReqTimeout, b.Bytes())
		}
		if otel.IsEnabled() {
			otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Status, kReqTimeout}})
		}
		p.replyStatusToClient(proto.OpStatusBusy)
	}
	now := time.Now()
	for i := 0; i < p.ssGroup.numAvailableSSs; i++ {
		if p.pendingResponses[i] != nil {
			if p.pendingResponses[i].state == stSSRequestSent {
				p.pendingResponses[i].state = stRequestTimeout
				p.pendingResponses[i].timeRespReceived = now
			}
		}
	}
}

func (p *ProcessorBase) OnCancelled() {
	if p.hasRepliedClient == false {
		glog.Warningf("Request Cancelled: %s %s", p.clientRequest.GetOpCodeText(), p.ctx.Err())
		if cal.IsEnabled() {
			b := logging.NewKVBuffer()
			b.AddOpCode(p.clientRequest.GetOpCode()).AddReqIdString(p.requestID)
			calLogReqProcEvent(kReqCancelled, b.Bytes())
		}
		if otel.IsEnabled() {
			otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Status, kReqCancelled}})
		}
		p.replyStatusToClient(proto.OpStatusBusy)
	}
	now := time.Now()
	for i := 0; i < p.ssGroup.numAvailableSSs; i++ {
		if p.pendingResponses[i] != nil {
			if p.pendingResponses[i].state == stSSRequestSent {
				p.pendingResponses[i].state = stRequestCancelled
				p.pendingResponses[i].timeRespReceived = now
			}
		}
	}
}

func (p *ProcessorBase) handleSSTimeout(now time.Time) {
	p.responseTimer.Stop()
	sz := len(p.pendingResponseQueue)
	if sz == 0 {
		return
	}
	queue := p.pendingResponseQueue
	p.pendingResponseQueue = p.pendingResponseQueue[:0]

	var i int
	for i = 0; i < sz; i++ {
		st := queue[i]
		if st.state == stSSRequestSent {
			if st.timeToExpire.After(now) {
				p.responseTimer.Reset(st.timeToExpire.Sub(now))
				break
			} else {
				//				st.cancelFunc()
				st.state = stSSRequestTimeout
				st.timeRespReceived = now
				st.ssResponseOpStatus = proto.OpStatusNoStorageServer
				glog.Infof("Timeout <-%s: %s elapsed=%s,rid=%s",
					p.logStrSsIdx(st.ssIndex),
					p.logStrOpCode(st.opCode),
					(now.Sub(st.timeToExpire) + confSSRequestTimeout).String(),
					p.clientRequest.GetRequestIDString())
				if cal.IsEnabled() {
					b := logging.NewKVBuffer()
					writeBasicSSRequestInfo(b, st.opCode, int(st.ssIndex), p.ssGroup.processors[st.ssIndex].GetConnInfo(), p)
					calLogReqProcEvent(calNameReqTimeoutFor(st.opCode), b.Bytes())
				}
				if otel.IsEnabled() {
					status := otel.SSReqTimeout + "_" + st.opCode.String()
					otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Status, status}})
				}
				if cluster.GetShardMgr().StatsEnabled() {
					zoneId, hostId := p.ssGroup.processors[st.ssIndex].GetNodeInfo()
					cluster.GetShardMgr().SendStats(zoneId, hostId, true, confSSRequestTimeout.Microseconds())
				}
				p.self.OnSSTimeout(st)
			}
		}
	}
	queue = queue[i:sz]
	queue = append(queue, p.pendingResponseQueue...) //This line is needed. OnSSTimeout() could add pending response to the pending queue
	p.pendingResponseQueue = queue
}

func (p *ProcessorBase) logStats() {

	receive_t := p.requestContext.GetReceiveTime().UnixNano()
	glog.Infof("rid=%s receive time: %d", p.clientRequest.GetRequestIDString(), receive_t)

	for i := 0; i < p.numSSRequestSent; i++ {
		st := &p.ssRequestContexts[i]

		glog.Infof("%d: %s.%s state:%d opStatus:%s ssIndex:%d sent %d ns, received %d ns",
			i, p.clientRequest.GetOpCodeText(), st.opCode.String(),
			st.state, st.ssResponseOpStatus.String(), st.ssIndex,
			st.timeReqSent.UnixNano()-receive_t, st.timeRespReceived.UnixNano()-receive_t)
	}
}

func (p *ProcessorBase) validateSSResponse(st *SSRequestContext) bool {
	return true
}

func (p *ProcessorBase) preprocessAndValidateResponse(resp io.IResponseContext) (st *SSRequestContext) {
	p.numSSResponseReceived++
	ssIndex := resp.GetMessage().GetOpaque()

	if ssIndex >= uint32(p.ssGroup.numAvailableSSs) {
		resp.GetMessage().HexDump()
		glog.Errorf("ssIndex out of bound: %d response status %d", ssIndex, resp.GetStatus())
		panic("ssIndex out of bound")
		return
	}
	st = p.pendingResponses[ssIndex]
	if st == nil {
		glog.Warningf("No request pending for the response. %s.", p.logStrSsIdx(ssIndex))
		return
	}
	if st.state != stSSRequestSent {
		if st.state != stSSRequestTimeout {
			glog.Warningf("No request pending for the response. %s. request state: %d", p.logStrSsIdx(ssIndex), st.state)
		}
		st = nil
		return
	}

	if ssIndex != st.ssIndex {
		glog.Errorf("ssIndex mismatch. pending SS[%d] - Received response from SS[%d]", st.ssIndex, ssIndex)
		st = nil
		return
	}
	p.pendingResponses[st.ssIndex] = nil

	if resp.GetStatus() != 0 {
		statusText := proto.StatusText((int)(resp.GetStatus()))
		glog.Warningf("outbound [%s] IO Error %s %s",
			p.ssGroup.processors[ssIndex].Name(),
			p.clientRequest.GetRequestIDString(), statusText)
		if cal.IsEnabled() {
			buf := logging.NewKVBuffer()
			writeBasicSSRequestInfo(buf, st.opCode, int(ssIndex), p.ssGroup.processors[ssIndex].GetConnInfo(), p)
			errStr := strings.Replace(statusText, " ", "_", -1)
			calLogReqProcEvent(fmt.Sprintf("SS_%s", errStr), buf.Bytes()) //TODO revisit log as error?
		}
		if otel.IsEnabled() {
			errStr := strings.Replace(statusText, " ", "_", -1)
			otel.RecordCount(otel.ReqProc, []otel.Tags{{otel.Status, fmt.Sprintf("SS_%s", errStr)}})
		}
		st.state = stSSResponseIOError
		st.timeRespReceived = time.Now()
		p.self.OnSSIOError(st)
		st = nil
		return
	}
	opMsg := &st.ssRespOpMsg
	*opMsg = proto.OperationalMessage{}

	err := opMsg.Decode(resp.GetMessage())

	if err != nil {
		st = nil
		glog.Errorf("Cannot decode opCode and/or opStatus from %s response: %s", p.logStrSsIdx(ssIndex), err)
		st = nil
		return
	}
	opStatus := opMsg.GetOpStatus()
	opCode := opMsg.GetOpCode()
	if opCode != st.opCode {
		p.logStats()
		glog.Errorf("numRequestSent: %d", p.numSSRequestSent)
		glog.Errorf("numResponseReceived: %d", p.numSSResponseReceived)
		glog.Errorf("numIOError: %d", p.numSSResponseIOError)
		err = fmt.Errorf("OpCode mismatch: SS[%d] %s.%s(expected, rid=%s)-%s (rid=%s)",
			ssIndex,
			p.clientRequest.GetOpCodeText(),
			st.opCode.String(),
			p.clientRequest.GetRequestIDString(),
			opCode.String(),
			opMsg.GetRequestIDString())

		panic(err)
		return
	}
	st.state = stSSResponseReceived
	st.ssResponse = resp
	st.ssResponseOpStatus = opStatus
	if LOG_DEBUG {
		b := logging.NewKVBufferForLog()
		b.AddOpStatus(opStatus).AddVersion(opMsg.GetVersion()).AddReqIdString(p.requestID).AddCreationTime(opMsg.GetCreationTime())
		b.AddLastModificationTime(opMsg.GetLastModificationTime())
		if opMsg.IsOriginatorSet() {
			//			if !opMsg.GetOriginatorRequestID().IsSet() {
			//				panic("")
			//			}
			b.AddOriginator(opMsg.GetOriginatorRequestID())
		}
		glog.DebugInfof("<-%s: %s %v", p.logStrSsIdx(ssIndex), p.logStrOpCode(opCode), b)
	}
	return
}

func (p *ProcessorBase) logStrSsIdx(ssIndex uint32) string {
	return fmt.Sprintf("SS[%d:%s]", p.ssGroup.procIndices[ssIndex], p.ssGroup.processors[ssIndex].Name())
}

func (p *ProcessorBase) logStrOpCode(opcode proto.OpCode) string {
	///xuli TODO do some tests to find out if the following is better than using simply fmt.Sprintf()
	var b bytes.Buffer
	if p.clientRequest.IsForReplication() {
		b.WriteByte('R')
	}
	if opcode.IsForStorage() {
		b.WriteString(p.clientRequest.GetOpCodeText())
		b.WriteByte('.')
		b.WriteString(opcode.String())
	} else {
		b.WriteString(opcode.String())
	}

	return b.String()
}
