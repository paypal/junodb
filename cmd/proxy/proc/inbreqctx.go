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
	"bytes"
	goio "io"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/config"
	"juno/cmd/proxy/replication"
	"juno/pkg/io"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
)

type IInbRequestContext interface {
	io.IRequestContext
	GetOpMessage() *proto.OperationalMessage
	GetReceiveTime() time.Time
	IsForReplication() bool
	GetNamespace() []byte
	GetOpCode() proto.OpCode
	ValidateRequest() bool
	ReplicateIfNeeded(st proto.OpStatus, ssresp *SSRequestContext)
	ReplyStatus(st proto.OpStatus)
}

//InboundRequestContext Proxy Inbound request context
type InboundRequestContext struct {
	io.InboundRequestContext
	proto.OperationalMessage
}

func (r *InboundRequestContext) Read(rd goio.Reader) (n int, err error) {
	if n, err = r.RequestContext.Read(rd); err != nil {
		return
	}
	err = r.Decode(r.GetMessage())

	return
}

func (r *InboundRequestContext) GetOpMessage() *proto.OperationalMessage {
	return &r.OperationalMessage
}

func (r *InboundRequestContext) ValidateRequest() bool {
	isReplication := r.IsForReplication()
	szKey := len(r.GetKey())
	if szKey == 0 {
		glog.Warningf("invalid key length %d", szKey)
		data := logging.NewKVBuffer()
		data.AddReqIdString(r.GetRequestIDString())
		data.AddInt([]byte("len"), szKey)
		calLogReqProcEvent(kBadParamInvalidKeyLen, data.Bytes())
		return false
	}
	szNs := len(r.GetNamespace())

	if szNs <= 0 || szNs > confMaxNamespaceLength {
		glog.Warningf("invalid namespace length %d", szNs)
		data := logging.NewKVBuffer()
		data.AddReqIdString(r.GetRequestIDString())
		data.AddInt([]byte("len"), szNs)
		calLogReqProcEvent(kBadParamInvalidNsLen, data.Bytes())
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
			return false
		}
		if limits.MaxTimeToLive != 0 && ttl > limits.MaxTimeToLive {
			data := logging.NewKVBuffer()
			data.AddReqIdString(r.GetRequestIDString())
			data.AddInt([]byte("ttl"), int(ttl))
			calLogReqProcEvent(kBadParamInvalidTTL, data.Bytes())
			glog.Warningf("limit exceeded: TTL %d > %d", ttl, limits.MaxTimeToLive)
			return false
		}
		szValue := r.GetPayloadValueLength()
		if limits.MaxPayloadLength != 0 && szValue > limits.MaxPayloadLength {
			data := logging.NewKVBuffer()
			data.AddReqIdString(r.GetRequestIDString())
			data.AddInt([]byte("len"), int(szValue))
			calLogReqProcEvent(kBadParamInvalidValueLen, data.Bytes())
			glog.Warningf("limit exceeded: payload length %d > %d", ttl, limits.MaxTimeToLive)
			return false
		}
	}
	return true
}

// ReplicateIfNeeded replicates request as needed
func (r *InboundRequestContext) ReplicateIfNeeded(opstatus proto.OpStatus, ssresp *SSRequestContext) {
	if !replication.Enabled() || r.IsForReplication() || ssresp == nil {
		return
	}

	// not to replicate Juno_internal:limits
	if bytes.Compare(r.GetNamespace(), []byte(config.JunoInternalNamespace())) == 0 &&
		bytes.Compare(r.GetKey(), config.JunoInternalKeyForLimits()) == 0 {
		return
	}
	opcode := r.GetOpCode()
	opmsg := &ssresp.ssRespOpMsg

	if (opstatus == proto.OpStatusNoError || opstatus == proto.OpStatusInconsistent) &&
		(opcode != proto.OpCodeGet || r.GetTimeToLive() > 0) &&
		opmsg.GetCreationTime() != 0 &&
		opmsg.GetVersion() != 0 {
		if LOG_VERBOSE {
			glog.Info("replicate request")
		}

		repRequest := r.OperationalMessage
		if opcode == proto.OpCodeCreate {
			repRequest.SetOpCode(proto.OpCodeUpdate)
		}
		if len(opmsg.GetOriginatorRequestID()) != 16 {
			glog.Warningf("oid not set. rid=%s", r.GetRequestIDString())
		}
		repRequest.SetAsReplication()
		repRequest.SetCreationTime(opmsg.GetCreationTime())
		repRequest.SetVersion(opmsg.GetVersion())
		repRequest.SetLastModificationTime(opmsg.GetLastModificationTime())
		repRequest.SetOriginatorRequestID(opmsg.GetOriginatorRequestID())
		expTime := opmsg.GetExpirationTime()
		repRequest.SetExpirationTime(expTime)
		if confReplicationEncryptionEnabled {
			repRequest.GetPayload().Encrypt(proto.PayloadTypeEncryptedByProxy)
		}
		replication.TheReplicator.SendRequest(&repRequest) //expTime, &repMsg)
		if LOG_DEBUG {
			b := logging.NewKVBufferForLog()
			b.AddReqIdString(opmsg.GetRequestIDString()).AddVersion(opmsg.GetVersion()).
				AddTTL(opmsg.GetTimeToLive()).AddCreationTime(opmsg.GetCreationTime())
			glog.DebugInfof("Replicate: %s %v", opcode.String(), b)
		}
	} else {
		if LOG_DEBUG {
			glog.DebugInfof("no replicaiton: opcode=%d, opstatus=%d, ttl=%d, create_t=%d, version=%d",
				opcode, opstatus, opmsg.GetTimeToLive(), opmsg.GetCreationTime(), opmsg.GetVersion())
		}
	}
}

func (r *InboundRequestContext) ReplyStatus(st proto.OpStatus) {
	request := r.GetOpMessage()

	msg := r.CreateResponse()
	msg.SetOpStatus(st)
	var rawMsg proto.RawMessage
	err := msg.Encode(&rawMsg)
	if err != nil {
		glog.Error("Failed to encode response: ", err)
	} else {
		var logData *logging.KeyValueBuffer
		if cal.IsEnabled() {
			logData = logging.NewKVBuffer()
			logData.AddOpRequestResponseInfo(request, msg)
		}

		resp := NewProxyInRespose(request, &rawMsg, r.GetReceiveTime(), logData)
		if LOG_DEBUG {
			b := logging.NewKVBufferForLog()
			b.AddOpStatus(st).AddVersion(msg.GetVersion()).AddReqIdString(msg.GetRequestIDString())
			if request.IsForReplication() {
				glog.DebugInfof("RepClient<-: %s %v", "R"+msg.GetOpCodeText(), b)
			} else {
				glog.DebugInfof("Client<-: %s %v", msg.GetOpCodeText(), b)
			}
		}
		r.Reply(resp)
	}
}
