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

package proto

import (
	"encoding/hex"
	"fmt"
	"io"
	"net"

	uuid "github.com/satori/go.uuid"

	"juno/pkg/util"
)

type OperationalMessage struct {
	opCode          OpCode
	flags           opMsgFlagT
	shardIdOrStatus shardIdOrStatusT
	typeFlag        messageTypeFlagT
	opaque          uint32

	namespace []byte
	key       []byte
	payload   Payload

	timeToLive           timeToLiveT
	version              versionT
	creationTime         creationTimeT
	expirationTime       expirationTimeT
	lastModificationTime lastModificationTimeT
	sourceInfo           sourceInfoT
	requestID            requestIdT
	originatorRequestID  originatorT
	correlationID        correlationIdT
	requestHandlingTime  requestHandlingTimeT
	udfName              udfNameT
}

func (op *OperationalMessage) SetMessage(opcode OpCode, key []byte, namespace []byte, payload *Payload, ttl uint32) {
	//op.typeFlag = OperationalMessageType
	op.opCode = opcode
	op.key = key
	op.namespace = namespace
	if payload != nil {
		op.payload = *payload
	} else {
		op.payload.Clear()
	}
	op.timeToLive.set(ttl)
}

func (op *OperationalMessage) SetRequest(opcode OpCode, key []byte, namespace []byte, payload *Payload, ttl uint32) {
	op.SetMessage(opcode, key, namespace, payload, ttl)
	op.SetAsRequest()
}

func (op *OperationalMessage) CreateResponse() (resp *OperationalMessage) {
	resp = &OperationalMessage{}
	resp.opCode = op.opCode
	resp.opaque = op.opaque
	resp.SetAsResponse()
	resp.key = op.key
	resp.namespace = op.namespace
	resp.requestID = op.requestID
	//	if resp.originatorRequestID.IsSet() {
	//		panic("")
	//	}
	return
}

func (m *OperationalMessage) IsRequest() bool {
	return m.typeFlag.isRequest()
}

func (m *OperationalMessage) IsResponse() bool {
	return m.typeFlag.isResponse()
}

func (m *OperationalMessage) GetFlags() opMsgFlagT {
	return m.flags
}

//
//func (m *OperationalMessage) SetFlags(f uint8) {
//	m.flags = f
//}

func (m *OperationalMessage) IsForReplication() bool {
	return m.flags.IsFlagReplicationSet()
}

func (m *OperationalMessage) SetAsReplication() {
	m.flags.SetReplicationFlag()
	//m.flags |= 1
}

// For namespace migration
func (m *OperationalMessage) IsForDeleteReplication() bool {
	return m.flags.IsFlagDeleteReplicationSet()
}

func (m *OperationalMessage) SetAsDeleteReplication() {
	m.flags.SetDeleteReplicationFlag()
}

func (m *OperationalMessage) SetOpaque(opaque uint32) {
	m.opaque = opaque
}

func (m *OperationalMessage) GetOpaque() uint32 {
	return m.opaque
}

func (m *OperationalMessage) SetAsResponse() {
	m.typeFlag.setAsResponse()
}

func (m *OperationalMessage) SetAsRequest() {
	m.typeFlag.setAsRequest()
}

func (m *OperationalMessage) SetOpCode(op OpCode) {
	m.opCode = op
}

func (m *OperationalMessage) GetOpCode() OpCode {
	return m.opCode
}

func (m *OperationalMessage) GetOpCodeText() string {
	return m.opCode.String()
}

func (m *OperationalMessage) SetOpStatus(s OpStatus) {
	if m.typeFlag.isResponse() {
		m.shardIdOrStatus[1] = uint8(s)
	} else {
		///log
	}
}
func (m *OperationalMessage) GetOpStatus() OpStatus {
	if m.typeFlag.isResponse() {
		return OpStatus(m.shardIdOrStatus[1])
	} else {
		///log
		return OpStatusNoError
	}
}

func (m *OperationalMessage) GetOpStatusText() string {
	if m.typeFlag.isResponse() {
		return OpStatus(m.shardIdOrStatus[1]).String()
	} else {
		return ""
	}
}

func (m *OperationalMessage) SetShardId(id uint16) {
	if m.typeFlag.isResponse() {
		///
		return
	}
	EncByteOrder.PutUint16(m.shardIdOrStatus[:], id)
}

func (m *OperationalMessage) GetShardId() uint16 {
	if m.typeFlag.isResponse() {
		///
		return 0
	}
	return EncByteOrder.Uint16(m.shardIdOrStatus[:])
}

func (m *OperationalMessage) SetKey(key []byte) {
	m.key = key
}

func (m *OperationalMessage) GetKey() []byte {
	return m.key
}

func (m *OperationalMessage) GetKeyHexString() string {
	return hex.EncodeToString(m.key)
}

func (m *OperationalMessage) GetVersion() uint32 {
	return m.version.value()
}
func (m *OperationalMessage) SetVersion(version uint32) {
	m.version.set(version)
}
func (m *OperationalMessage) SetNamespace(namespace []byte) {
	m.namespace = namespace
}

func (m *OperationalMessage) GetNamespace() []byte {
	return m.namespace
}

func (m *OperationalMessage) SetPayload(payload *Payload) {
	m.payload.Set(payload)
}

func (m *OperationalMessage) ClearPayload() {
	m.payload.Clear()
}

func (m *OperationalMessage) GetPayload() *Payload {
	return &m.payload
}

//func (m *OperationalMessage) SetValue(value []byte) {
//	m.value = value
//}
//
//func (m *OperationalMessage) GetValue() []byte {
//	return m.value
//}

func (m *OperationalMessage) GetTimeToLive() uint32 {
	ttl := m.timeToLive.value()
	if ttl == 0 {
		ttl = util.GetTimeToLive(m.expirationTime.value())
	}
	return ttl
}

func (m *OperationalMessage) SetTimeToLive(value uint32) {
	m.timeToLive.set(value)
}

func (m *OperationalMessage) GetCreationTime() uint32 {
	return m.creationTime.value()
}

func (m *OperationalMessage) SetCreationTime(value uint32) {
	m.creationTime.set(value)
}

func (m *OperationalMessage) GetExpirationTime() uint32 {
	expTime := m.expirationTime.value()
	if expTime == 0 {
		expTime = util.GetExpirationTime(m.timeToLive.value())
	}
	return expTime
}

func (m *OperationalMessage) GetRequestHandlingTime() uint32 {
	return m.requestHandlingTime.value()
}

func (m *OperationalMessage) SetRequestHandlingTime(value uint32) {
	if value == 0 {
		value = 1 // round up
	}
	m.requestHandlingTime.set(value)
}

func (m *OperationalMessage) SetLastModificationTime(value uint64) {
	m.lastModificationTime.set(value)
}

func (m *OperationalMessage) GetLastModificationTime() uint64 {
	return m.lastModificationTime.value()
}

func (m *OperationalMessage) SetExpirationTime(value uint32) {
	m.expirationTime.set(value)
}

func (m *OperationalMessage) SetSource(ip net.IP, port uint16, appName []byte) {
	m.sourceInfo.ip = ip
	m.sourceInfo.port = port
	m.sourceInfo.appName = appName
}

func (m *OperationalMessage) UnSetRequestID() {
	m.requestID.idSet = false
	m.requestID.RequestId = NilRequestId
}

func (m *OperationalMessage) SetRequestID(id RequestId) {
	m.requestID.setWithID(id)
}

func (m *OperationalMessage) SetNewRequestID() {
	uuid := uuid.NewV1()
	m.requestID.set(uuid.Bytes())
}

func (m *OperationalMessage) SetUDFName(name []byte) {
	m.udfName.set(name)
}

func (m *OperationalMessage) GetOriginatorRequestID() RequestId {
	return m.originatorRequestID.RequestId
}

func (m *OperationalMessage) GetRequestIDString() string {
	return m.requestID.String()
}

func (m *OperationalMessage) IsOriginatorSet() bool {
	return m.originatorRequestID.isSet()
}

func (m *OperationalMessage) SetOriginatorRequestID(id RequestId) {
	m.originatorRequestID.setWithID(id)
}

func (m *OperationalMessage) SetCorrelationID(id []byte) {
	m.correlationID.set(id)
}

func (m *OperationalMessage) GetCorrelationID() []byte {
	return m.correlationID.value()
}

func (m *OperationalMessage) GetRequestID() RequestId {
	return m.requestID.RequestId
}

func (m *OperationalMessage) IsRequestIDSet() bool {
	return m.requestID.isSet()
}

func (m *OperationalMessage) GetOriginatorRequestIDString() string {
	return m.originatorRequestID.String()
}

func (m *OperationalMessage) GetAppName() []byte {
	return m.sourceInfo.appName
}

func (m *OperationalMessage) GetSrcIP() net.IP {
	return m.sourceInfo.ip
}

func (m *OperationalMessage) GetSrcPort() uint16 {
	return m.sourceInfo.port
}

func (m *OperationalMessage) GetPayloadLength() uint32 {
	return m.payload.GetLength()
}

func (m *OperationalMessage) GetPayloadValueLength() uint32 {
	return m.payload.GetValueLength()
}

func (m *OperationalMessage) GetUDFName() []byte {
	return m.udfName.value()
}

func (m *OperationalMessage) IsUDFNameSet() bool {
	return m.udfName.isSet()
}

func (m *OperationalMessage) PrettyPrint(w io.Writer) {
	fmt.Fprintf(w, "OPaque        : %#v\n", m.opaque)
	fmt.Fprintf(w, "OpCode        : %#v\t%s\n", m.opCode, m.opCode.String())
	fmt.Fprintf(w, "MsgType       : %#v\t%s ", m.typeFlag, m.typeFlag.String())
	if m.IsForReplication() {
		fmt.Fprint(w, " (Replication)")
	}
	fmt.Fprint(w, "\n")
	if m.typeFlag.isResponse() {
		fmt.Fprintf(w, "OpStatus      : %#v\t%s\n", m.GetOpStatus(), m.GetOpStatus().String())
	} else {
		fmt.Fprintf(w, "ShardId       : %#v\n", m.GetShardId())
	}
	fmt.Fprintf(w, "Key           : %s\n", util.ToPrintableAndHexString(m.key))
	fmt.Fprintf(w, "Namespace     : %s\n", util.ToPrintableAndHexString(m.namespace))
	fmt.Fprintf(w, "RequestID     : %s\n", m.requestID)
	if m.originatorRequestID.isSet() {
		fmt.Fprintf(w, "Originator RID: %s\n", m.originatorRequestID)
	}
	if len(m.sourceInfo.appName) > 0 {
		fmt.Fprintf(w, "App name      : %s\n", string(m.sourceInfo.appName))
	}
	m.payload.PrettyPrint(w)

	if m.version.isSet() {
		fmt.Fprintf(w, "Version        : %d\n", m.version.value())
	}
	if m.creationTime.isSet() {
		fmt.Fprintf(w, "Creation Time  : %d\n", m.creationTime.value())
	}
	if m.timeToLive.isSet() {
		fmt.Fprintf(w, "Lifetime       : %d\n", m.timeToLive.value())
	}
	if m.expirationTime.isSet() {
		fmt.Fprintf(w, "Expiration Time: %d\n", m.expirationTime.value())
	}
	if m.lastModificationTime.isSet() {
		fmt.Fprintf(w, "Last Modified  : %d\n", m.lastModificationTime.value())
	}
	if m.correlationID.isSet() {
		fmt.Fprintf(w, "correlation id      : %s\n", string(m.correlationID.value()))
	}
	if m.udfName.isSet() {
		fmt.Fprintf(w, "UDF Name      : %s\n", string(m.udfName.value()))
	}
}
