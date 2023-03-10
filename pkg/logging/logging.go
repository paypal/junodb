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

package logging

import (
	"bytes"
	"fmt"
	"os"
	"strconv"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/logging/cal"
	"juno/pkg/proto"
	"juno/pkg/util"
)

type KeyValueBuffer struct {
	bytes.Buffer
	delimiter     byte
	pairDelimiter byte
}

func NewKVBufferForLog() *KeyValueBuffer {
	b := &KeyValueBuffer{
		delimiter:     '=',
		pairDelimiter: ',',
	}
	return b
}

func NewKVBuffer() *KeyValueBuffer {
	b := &KeyValueBuffer{
		pairDelimiter: '&',
		delimiter:     '=',
	}
	return b
}

var (
	logDataKeyUser                 []byte = []byte("User")
	logDataKeyOpCode               []byte = []byte("op")
	logDataKeyStatus               []byte = []byte("st")
	logDataKeyErrStatus            []byte = []byte("m_err")
	logDataKeyRid                  []byte = []byte("rid")
	logDataKeyOriginatorRid        []byte = []byte("oid")
	logDataKeyNamespace            []byte = []byte("ns")
	logDataKeyShardId              []byte = []byte("shid")
	logDataKeyKey                  []byte = []byte("key")
	logDataKeyExpiration           []byte = []byte("et")
	logDataKeyReqHandleTime        []byte = []byte("rht")
	logDataKeyCreationTime         []byte = []byte("ct")
	logDataKeyCorrelationId        []byte = []byte("corr_id_")
	logDataKeyRequestParam         []byte = []byte("RQ")
	logDataKeyResponseParam        []byte = []byte("RE")
	logDataKeyLastModificationTime []byte = []byte("mt")

	logDataKeyVersion    []byte = []byte("v")
	logDataKeyTTL        []byte = []byte("ttl")
	logDataKeyPayloadLen []byte = []byte("len")

	logDataKeyTryNo []byte = []byte("try_no")
	logDropReason   []byte = []byte("drop")
)

func (b *KeyValueBuffer) AddBytes(key []byte, value []byte) *KeyValueBuffer {
	if b.Len() > 0 {
		b.WriteByte(b.pairDelimiter)
	}
	b.Write(key)
	b.WriteByte(b.delimiter)
	b.Write(value)
	return b
}

func (b *KeyValueBuffer) Add(key []byte, value string) *KeyValueBuffer {
	if b.Len() > 0 {
		b.WriteByte(b.pairDelimiter)
	}
	b.Write(key)
	b.WriteByte(b.delimiter)
	b.WriteString(value)
	return b
}

func (b *KeyValueBuffer) AddHexKey(key []byte) *KeyValueBuffer {
	return b.Add(logDataKeyKey, util.ToHexString(key))
}

func (b *KeyValueBuffer) AddNamespace(ns []byte) *KeyValueBuffer {
	return b.AddBytes(logDataKeyNamespace, ns)
}

func (b *KeyValueBuffer) AddInt(key []byte, value int) *KeyValueBuffer {
	return b.Add(key, strconv.Itoa(value))
}

func (b *KeyValueBuffer) AddUInt64(key []byte, value uint64) *KeyValueBuffer {
	return b.Add(key, strconv.FormatUint(value, 10))
}

func (b *KeyValueBuffer) AddUser(user string) *KeyValueBuffer {
	return b.Add(logDataKeyUser, user)
}

func (b *KeyValueBuffer) AddOpCode(opcode proto.OpCode) *KeyValueBuffer {
	return b.Add(logDataKeyOpCode, opcode.String())
}

func (b *KeyValueBuffer) AddOpStatus(st proto.OpStatus) *KeyValueBuffer {
	if b.pairDelimiter == '&' {
		return b.AddErrOpStatus(st)
	}
	return b.Add(logDataKeyStatus, st.String())
}

func (b *KeyValueBuffer) AddErrOpStatus(st proto.OpStatus) *KeyValueBuffer {
	if st != proto.OpStatusNoError {
		b.Add(logDataKeyErrStatus, st.ShortNameString())
	}
	return b
}

func (b *KeyValueBuffer) AddStatus(st string) *KeyValueBuffer {
	return b.Add(logDataKeyStatus, st)
}

func (b *KeyValueBuffer) AddDropReason(reason string) *KeyValueBuffer {
	return b.Add(logDropReason, reason)
}

func (b *KeyValueBuffer) AddShardId(shardId uint16) *KeyValueBuffer {
	return b.AddInt(logDataKeyShardId, int(shardId))
}

func (b *KeyValueBuffer) AddVersion(v uint32) *KeyValueBuffer {
	if v != 0 {
		b.AddInt(logDataKeyVersion, int(v))
	}
	return b
}

func (b *KeyValueBuffer) AddOriginator(id proto.RequestId) *KeyValueBuffer {
	if id.IsNotNil() {
		b.Add(logDataKeyOriginatorRid, id.String())
	}
	return b
}

func (b *KeyValueBuffer) AddRequestID(id proto.RequestId) *KeyValueBuffer {
	if id.IsNotNil() {
		b.Add(logDataKeyRid, id.String())
	}
	return b
}

func (b *KeyValueBuffer) AddReqIdString(id string) *KeyValueBuffer {
	return b.Add(logDataKeyRid, id)
}

func (b *KeyValueBuffer) AddRequestHandleTime(rhtus int) *KeyValueBuffer {
	return b.AddInt(logDataKeyReqHandleTime, rhtus)
}

func (b *KeyValueBuffer) AddTTL(v uint32) *KeyValueBuffer {
	if v != 0 {
		b.AddInt(logDataKeyTTL, int(v))
	}
	return b
}

func (b *KeyValueBuffer) AddExpirationTime(v uint32) *KeyValueBuffer {
	if v != 0 {
		b.AddInt(logDataKeyExpiration, int(v))
	}
	return b
}

func (b *KeyValueBuffer) AddCreationTime(v uint32) *KeyValueBuffer {
	if v != 0 {
		b.AddInt(logDataKeyCreationTime, int(v))
	}
	return b
}

func (b *KeyValueBuffer) AddLastModificationTime(v uint64) *KeyValueBuffer {
	if v != 0 {
		b.AddUInt64(logDataKeyLastModificationTime, v)
	}
	return b
}

func (b *KeyValueBuffer) AddCorrelationId(id []byte) *KeyValueBuffer {
	if len(id) != 0 {
		b.AddBytes(logDataKeyCorrelationId, id)
	}
	return b
}

func (b *KeyValueBuffer) AddOpRequestResponseInfoWithUser(req, resp *proto.OperationalMessage, user string) *KeyValueBuffer {
	return b.AddUser(user).AddOpRequestResponseInfo(req, resp)
}

func (b *KeyValueBuffer) AddOpRequestResponseInfo(req, resp *proto.OperationalMessage) *KeyValueBuffer {
	opstatus := resp.GetOpStatus()
	b.AddOpStatus(opstatus).AddCorrelationId(
		req.GetCorrelationID()).Add(
		logDataKeyRid, req.GetRequestIDString()).AddNamespace(
		req.GetNamespace()).AddHexKey(
		req.GetKey())

	reqParam := &KeyValueBuffer{delimiter: ':', pairDelimiter: '|'}

	val := int(req.GetVersion())
	if val > 0 {
		reqParam.AddInt(logDataKeyVersion, val)
	}
	val = int(req.GetTimeToLive())
	if val > 0 {
		reqParam.AddInt(logDataKeyTTL, val)
	}
	val = int(req.GetPayloadLength())
	if val > 0 {
		reqParam.AddInt(logDataKeyPayloadLen, val)
	}
	if reqParam.Buffer.Len() > 0 {
		b.AddBytes(logDataKeyRequestParam, reqParam.Bytes())
	}
	if opstatus == proto.OpStatusVersionConflict {
		val = int(req.GetCreationTime())
		if val > 0 {
			reqParam.AddInt(logDataKeyCreationTime, val)
		}
	}

	param := &KeyValueBuffer{delimiter: ':', pairDelimiter: '|'}

	val = int(resp.GetVersion())
	if val > 0 {
		param.AddInt(logDataKeyVersion, val)
	}
	val = int(resp.GetTimeToLive())
	if val > 0 {
		param.AddInt(logDataKeyTTL, val)
	}
	val = int(resp.GetPayloadLength())
	if val > 0 {
		param.AddInt(logDataKeyPayloadLen, val)
	}
	if opstatus == proto.OpStatusVersionConflict {
		val = int(resp.GetCreationTime())
		if val > 0 {
			reqParam.AddInt(logDataKeyCreationTime, val)
		}
	}

	if param.Buffer.Len() > 0 {
		b.AddBytes(logDataKeyResponseParam, param.Bytes())
	}
	return b
}

// difference from AddOpRequestResponseInfo():
// not log correlation id, key, or namespace
func (b *KeyValueBuffer) AddOpRequestResponse(req, resp *proto.OperationalMessage) *KeyValueBuffer {
	opstatus := resp.GetOpStatus()
	b.AddOpStatus(opstatus).Add(logDataKeyRid, req.GetRequestIDString())

	reqParam := &KeyValueBuffer{delimiter: ':', pairDelimiter: '|'}

	val := int(req.GetVersion())
	if val > 0 {
		reqParam.AddInt(logDataKeyVersion, val)
	}
	val = int(req.GetTimeToLive())
	if val > 0 {
		reqParam.AddInt(logDataKeyTTL, val)
	}
	val = int(req.GetPayloadLength())
	if val > 0 {
		reqParam.AddInt(logDataKeyPayloadLen, val)
	}
	if reqParam.Buffer.Len() > 0 {
		b.AddBytes(logDataKeyRequestParam, reqParam.Bytes())
	}
	if opstatus == proto.OpStatusVersionConflict {
		val = int(req.GetCreationTime())
		if val > 0 {
			reqParam.AddInt(logDataKeyCreationTime, val)
		}
	}

	param := &KeyValueBuffer{delimiter: ':', pairDelimiter: '|'}

	val = int(resp.GetVersion())
	if val > 0 {
		param.AddInt(logDataKeyVersion, val)
	}
	val = int(resp.GetTimeToLive())
	if val > 0 {
		param.AddInt(logDataKeyTTL, val)
	}
	val = int(resp.GetPayloadLength())
	if val > 0 {
		param.AddInt(logDataKeyPayloadLen, val)
	}
	if opstatus == proto.OpStatusVersionConflict {
		val = int(resp.GetCreationTime())
		if val > 0 {
			reqParam.AddInt(logDataKeyCreationTime, val)
		}
	}

	if param.Buffer.Len() > 0 {
		b.AddBytes(logDataKeyResponseParam, param.Bytes())
	}
	return b
}

func (b *KeyValueBuffer) AddOpRequestInfo(request *proto.OperationalMessage) *KeyValueBuffer {
	b.AddCorrelationId(request.GetCorrelationID())
	b.Add(logDataKeyRid, request.GetRequestIDString())
	b.AddNamespace(request.GetNamespace())
	b.AddHexKey(request.GetKey())
	reqParam := &KeyValueBuffer{delimiter: ':', pairDelimiter: '|'}

	val := int(request.GetVersion())
	if val > 0 {
		reqParam.AddInt(logDataKeyVersion, val)
	}
	val = int(request.GetTimeToLive())
	if val > 0 {
		reqParam.AddInt(logDataKeyTTL, val)
	}
	val = int(request.GetPayloadLength())
	if val > 0 {
		reqParam.AddInt(logDataKeyPayloadLen, val)
	}
	if reqParam.Buffer.Len() > 0 {
		b.AddBytes(logDataKeyRequestParam, reqParam.Bytes())
	}
	return b
}

//difference from AddOpRequestInfo(): namespace and key not logged
func (b *KeyValueBuffer) AddOpRequest(request *proto.OperationalMessage) *KeyValueBuffer {
	b.Add(logDataKeyRid, request.GetRequestIDString())
	reqParam := &KeyValueBuffer{delimiter: ':', pairDelimiter: '|'}

	val := int(request.GetVersion())
	if val > 0 {
		reqParam.AddInt(logDataKeyVersion, val)
	}
	val = int(request.GetTimeToLive())
	if val > 0 {
		reqParam.AddInt(logDataKeyTTL, val)
	}
	val = int(request.GetPayloadLength())
	if val > 0 {
		reqParam.AddInt(logDataKeyPayloadLen, val)
	}
	if reqParam.Buffer.Len() > 0 {
		b.AddBytes(logDataKeyRequestParam, reqParam.Bytes())
	}
	return b
}

func (b *KeyValueBuffer) AddOpResponseInfo(resp *proto.OperationalMessage) *KeyValueBuffer {
	param := &KeyValueBuffer{delimiter: ':', pairDelimiter: '|'}

	val := int(resp.GetVersion())
	if val > 0 {
		param.AddInt(logDataKeyVersion, val)
	}
	val = int(resp.GetTimeToLive())
	if val > 0 {
		param.AddInt(logDataKeyTTL, val)
	}
	val = int(resp.GetPayloadLength())
	if val > 0 {
		param.AddInt(logDataKeyPayloadLen, val)
	}
	if param.Buffer.Len() > 0 {
		b.AddBytes(logDataKeyResponseParam, param.Bytes())
	}
	return b
}

func (b *KeyValueBuffer) AddDataTryNo(v int32) *KeyValueBuffer {
	b.AddInt(logDataKeyTryNo, int(v))
	return b
}

func LogToCal(opcode proto.OpCode, opst proto.OpStatus, rht time.Duration, data []byte) {
	calst := CalStatus(opst)
	cal.AtomicTransaction(cal.TxnTypeAPI, opcode.String(), calst.CalStatus(), rht, data)

	if (opst == proto.OpStatusInconsistent) || (calst != kStatusSuccess) {
		cal.Event("ProcErr", opcode.String()+"_"+opst.String(), cal.StatusSuccess, nil)
	}
}

func LogManagerStart() {
	pid := os.Getpid()
	glog.InfoDepth(1, fmt.Sprintf("server manager (pid: %d) started", pid))
	if cal.IsEnabled() {
		cal.Event(CalMsgTypeManager, CalMsgNameStart, cal.StatusSuccess, []byte(fmt.Sprintf("pid=%d", pid)))
	}
}

func LogWorkerStart(workerId int) {
	pid := os.Getpid()
	glog.InfoDepth(1, fmt.Sprintf("worker %d (pid: %d) started", workerId, pid))
	if cal.IsEnabled() {
		calData := []byte(fmt.Sprintf("wid=%d&pid=%d", workerId, pid))
		cal.Event(CalMsgTypeWorker, CalMsgNameStart, cal.StatusSuccess, calData)
	}
}

func LogManagerExit() {
	pid := os.Getpid()
	glog.InfoDepth(1, fmt.Sprintf("server manager (pid: %d) stopped", pid))
	if cal.IsEnabled() {
		cal.Event(CalMsgTypeManager, CalMsgNameExit, cal.StatusSuccess, []byte(fmt.Sprintf("pid=%d", pid)))
	}
}

func LogWorkerExit(workerId int) {
	pid := os.Getpid()
	glog.InfoDepth(1, fmt.Sprintf("worker %d (pid: %d) stopped", workerId, pid))
	if cal.IsEnabled() {
		calData := []byte(fmt.Sprintf("wid=%d&pid=%d", workerId, pid))
		cal.Event(CalMsgTypeWorker, CalMsgNameExit, cal.StatusSuccess, calData)
	}
}
