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

package mayfly

import (
	"encoding/binary"
	"net"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/paypal/junodb/pkg/proto"
)

func opCodeToJuno(mfop OpCode) (opcode proto.OpCode, err error) {
	switch mfop {
	case OpCodeNOP:
		opcode = proto.OpCodeNop
	case OpCodeCreate:
		opcode = proto.OpCodeCreate
	case OpCodeGet:
		opcode = proto.OpCodeGet
	case OpCodeUpdate:
		opcode = proto.OpCodeUpdate
	case OpCodeDestroy:
		opcode = proto.OpCodeDestroy
	case OpCodeSet:
		opcode = proto.OpCodeSet
	default:
		err = errUnsupportedOpCode
	}

	return
}

func toMayflyOpCode(jop proto.OpCode) (opcode OpCode, err error) {
	switch jop {
	case proto.OpCodeNop:
		opcode = OpCodeNOP
	case proto.OpCodeCreate:
		opcode = OpCodeCreate
	case proto.OpCodeGet:
		opcode = OpCodeGet
	case proto.OpCodeUpdate:
		opcode = OpCodeUpdate
	case proto.OpCodeSet:
		opcode = OpCodeSet
	case proto.OpCodeDestroy:
		opcode = OpCodeDestroy
	default:
		err = errUnsupportedOpCode
	}
	return
}

// /TODO to be reviewed
var opStatusJunoToMayflyMapping []OpStatus = []OpStatus{
	OpStatusdNoError,             //Ok,                   0
	OpStatusBadMsg,               //BadMsg,               1
	OpStatusServiceDenied,        //ServiceDenied,        2
	OpStatusNoKey,                //NoKey,                3
	OpStatusDupKey,               //DupKey,               4
	OpStatusDataExpired,          //DataExpired,          5
	OpStatusOutOfMem,             //OutOfMem,             6
	OpStatusBadParam,             //BadParam,             7
	OpStatusRecordLocked,         //RecordLocked,         8
	OpStatusVersionTooOld,        //VersionTooOld,        9
	OpStatusNoUncommitted,        //NoUncommitted,        10
	OpStatusBadRequestID,         //BadRequestID,         11
	OpStatusNoStorageServer,      //NoStorageServer,      12
	OpStatusDuplicateRequest,     //DuplicateRequest,     13
	OpStatusStorageServerTimeout, //StorageServerTimeout, 14
	OpStatusInserting,            //Inserting,            15
	OpStatusInvalidNamespace,     //InvalidNamespace,     16
	OpStatusAlreadyFulfilled,     //AlreadyFulfilled,     17
	OpStatusNotSameRecord,        //NotSameRecord,        18
	OpStatusVersionConflict,      //VersionConflict,      19
	OpStatusNotAppendable,        //NotAppendable,        20
	OpStatusdNoError,             //Reserved,             21
	OpStatusdNoError,             //Reserved,             22
	OpStatusdNoError,             //Reserved,             23
	OpStatusStorageServerTimeout, //RequestProcTimeout,   24
	OpStatusNoStorageServer,      //CommitFailure,        25
	OpStatusdNoError,             //InconsistentState,    26
}

var numStatusMapEntries = len(opStatusJunoToMayflyMapping)

func toMayflyOpStatus(jstatus proto.OpStatus) OpStatus {
	i := int(jstatus)
	if i >= numStatusMapEntries {
		glog.Errorf("mapping of %s not defined", jstatus.String())
		return OpStatusServiceDenied
	}
	return opStatusJunoToMayflyMapping[i]
}

func ToJunoMsg(jmsg *proto.OperationalMessage, mmsg *Msg) error {
	opcode, err := opCodeToJuno(mmsg.opMsg.opcode)
	if err != nil {
		return err
	}
	jmsg.SetOpCode(opcode)
	switch mmsg.header.direction {
	case kMessageTypeRequest:
		jmsg.SetAsRequest()
	case kMessageTypeResponse:
		jmsg.SetAsResponse()

	default:
		if opcode == proto.OpCodeNop {
			jmsg.SetAsRequest()
		} else {
			glog.Warningf("unknown mayfly message type: %d", mmsg.header.direction)
			return errUnsupportedMessageType
		}
	}
	/*
		Juno makes use of NOP message with replication flag set to keep replication connection persistent
		Right now, when mayflyreplicatorserv makes connection to the target, it sends a NOP message with
		senderType 2 to keep the connection persistent and there is no field indicating it is a replication
		message
	*/
	if mmsg.opMsg.isForReplication ||
		((opcode == proto.OpCodeNop) && (mmsg.header.senderType == kSenderTypeDirectoryServer)) {
		jmsg.SetAsReplication()
	}
	jmsg.SetOpaque(mmsg.header.siteId)
	jmsg.SetNamespace(mmsg.opMsg.recordInfo.namespace)
	jmsg.SetKey(mmsg.opMsg.recordInfo.key)
	jmsg.GetPayload().SetWithClearValue(mmsg.opMsg.value)
	jmsg.SetTimeToLive(mmsg.opMsg.recordInfo.lifetime)
	jmsg.SetVersion(uint32(mmsg.opMsg.recordInfo.version))
	jmsg.SetCreationTime(mmsg.opMsg.recordInfo.creationTime)
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, mmsg.header.senderIP)
	jmsg.SetSource(ip, mmsg.header.senderPort, mmsg.appName)
	rid := make([]byte, 16)
	if err := mmsg.opMsg.requestId.encode(rid); err != nil {
		return err
	}
	var requestID proto.RequestId
	requestID.SetFromBytes(rid)
	jmsg.SetRequestID(requestID)
	//	jmsg.PrettyPrint(os.Stdout)

	return nil
}

func ToMayflyMsg(mmsg *Msg, jmsg *proto.OperationalMessage) (err error) {
	mmsg.opMsg.opcode, err = toMayflyOpCode(jmsg.GetOpCode()) ///TODO

	if jmsg.IsRequest() {
		mmsg.header.direction = kMessageTypeRequest
	} else if jmsg.IsResponse() {
		mmsg.header.direction = kMessageTypeResponse
		mmsg.opMsg.opstatus = toMayflyOpStatus(jmsg.GetOpStatus())
	}
	if mmsg.header.senderIP == 0 {
		mmsg.header.senderIP = gRequestIdIPUint32
	}
	if mmsg.header.senderPort == 0 {
		mmsg.header.senderPort = uint16(gRequestIdPid) //Though it is not a real port, it does not matter
	}
	if mmsg.header.senderType == 0 {
		mmsg.header.senderType = kSenderTypeDirectoryServer
	}
	mmsg.opMsg.isForReplication = jmsg.IsForReplication()
	mmsg.header.siteId = jmsg.GetOpaque()
	mmsg.opMsg.recordInfo.namespace = jmsg.GetNamespace()
	mmsg.opMsg.recordInfo.key = jmsg.GetKey()
	mmsg.opMsg.value, err = jmsg.GetPayload().GetClearValue()
	mmsg.opMsg.recordInfo.lifetime = jmsg.GetTimeToLive()
	mmsg.opMsg.recordInfo.version = uint16(jmsg.GetVersion()) ///TODO check overflow
	mmsg.opMsg.recordInfo.creationTime = jmsg.GetCreationTime()

	return
}
