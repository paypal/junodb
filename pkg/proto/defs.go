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
	"encoding/binary"
)

type IMessage interface {
}

type (
	OpCode           uint8
	OpStatus         uint8
	messageTypeFlagT uint8
	shardIdOrStatusT [2]uint8
)

type ProtocolError struct {
	what string
}

const (
	kOperationalMessageType = iota
	kAdminMessageType
	kClusterControlMessageType
)

var (
	JunoMagic [2]byte = [2]byte{0x50, 0x50}
)

const (
	kMessageMagic                 uint16 = 0x5050
	kCurrentVersion               uint8  = 1
	kMessageHeaderSize                   = 12
	kOperationalMessageHeaderSize        = 16
	kOpMsgSubHeaderSize                  = (kOperationalMessageHeaderSize - kMessageHeaderSize)
	kMaxMetaCompHeaderSize               = 264
	kMaxSourceInfoFieldSize              = 4 + 16 + 128
	kPayloadCompHeaderSize               = 12
	kMaxMetaComponentSize                = 2048 //Temporary
)

const (
	OpCodeNop         = OpCode(0)
	OpCodeCreate      = OpCode(1)
	OpCodeGet         = OpCode(2)
	OpCodeUpdate      = OpCode(3)
	OpCodeSet         = OpCode(4)
	OpCodeDestroy     = OpCode(5)
	OpCodeUDFGet      = OpCode(6)
	OpCodeUDFSet      = OpCode(7)
	OpCodeLastProxyOp = OpCode(8) // add proxy op before this

	OpCodePrepareCreate = OpCode(0x81)
	OpCodeRead          = OpCode(0x82)
	OpCodePrepareUpdate = OpCode(0x83)
	OpCodePrepareSet    = OpCode(0x84)
	OpCodePrepareDelete = OpCode(0x85)
	OpCodeDelete        = OpCode(0x86)

	OpCodeCommit     = OpCode(0xC1)
	OpCodeAbort      = OpCode(0xC2)
	OpCodeRepair     = OpCode(0xC3)
	OpCodeMarkDelete = OpCode(0xC4)

	OpCodeClone        = OpCode(0xE1)
	OpCodeVerHandshake = OpCode(0xE2)

	OpCodeMockGetExtendTTL = OpCode(0xFD)
	OpCodeMockSetParam     = OpCode(0xFE)
	OpCodeMockReSet        = OpCode(0xFF)
)

const (
	OpStatusNoError            = OpStatus(0)
	OpStatusBadMsg             = OpStatus(1)
	OpStatusServiceDenied      = OpStatus(2)
	OpStatusBadParam           = OpStatus(7)
	OpStatusNoKey              = OpStatus(3)
	OpStatusDupKey             = OpStatus(4)
	OpStatusRecordLocked       = OpStatus(8)
	OpStatusVersionConflict    = OpStatus(19)
	OpStatusNoStorageServer    = OpStatus(12)
	OpStatusInserting          = OpStatus(15)
	OpStatusAlreadyFulfilled   = OpStatus(17)
	OpStatusNoUncommitted      = OpStatus(10)
	OpStatusBusy               = OpStatus(14)
	OpStatusSSError            = OpStatus(21)
	OpStatusSSOutofResource    = OpStatus(22)
	OpStatusSSReadTTLExtendErr = OpStatus(23)
	OpStatusKeyMarkedDelete    = OpStatus(27)
	OpStatusCommitFailure      = OpStatus(25)
	OpStatusInconsistent       = OpStatus(26)
	OpStatusReqProcTimeout     = OpStatus(24)
	OpStatusNotSupported       = OpStatus(28)
)

const (
	OpStatusInternal = OpStatus(255)
)

var (
	EncByteOrder = binary.BigEndian
)

var (
	opCodeNameMap map[OpCode]string = map[OpCode]string{
		OpCodeNop:     "Nop",
		OpCodeCreate:  "Create",
		OpCodeGet:     "Get",
		OpCodeUpdate:  "Update",
		OpCodeSet:     "Set",
		OpCodeDestroy: "Destroy",
		OpCodeUDFGet:  "UDFGet",
		OpCodeUDFSet:  "UDFSet",

		OpCodePrepareCreate: "PrepareCreate",
		OpCodeRead:          "Read",
		OpCodePrepareUpdate: "PrepareUpdate",
		OpCodePrepareSet:    "PrepareSet",
		OpCodePrepareDelete: "PrepareDelete",
		OpCodeDelete:        "Delete",
		OpCodeCommit:        "Commit",
		OpCodeAbort:         "Abort",
		OpCodeRepair:        "Repair",
		OpCodeMarkDelete:    "MarkDelete",
		OpCodeClone:         "Clone",
		OpCodeVerHandshake:  "VerHandshake",

		OpCodeMockGetExtendTTL: "GetE",
		OpCodeMockSetParam:     "OpCodeMockSetParam",
		OpCodeMockReSet:        "OpCodeMockReSet",
	}
	opCodeShortNameMap map[OpCode]string = map[OpCode]string{
		OpCodeNop:     "N",
		OpCodeCreate:  "C",
		OpCodeGet:     "G",
		OpCodeUpdate:  "U",
		OpCodeSet:     "S",
		OpCodeDestroy: "D",
		OpCodeUDFGet:  "UG",
		OpCodeUDFSet:  "US",

		OpCodePrepareCreate: "P",
		OpCodeRead:          "R",
		OpCodePrepareUpdate: "P",
		OpCodePrepareSet:    "P",
		OpCodePrepareDelete: "P",
		OpCodeDelete:        "D",
		OpCodeMarkDelete:    "MD",
		OpCodeCommit:        "C",
		OpCodeAbort:         "A",
		OpCodeRepair:        "RR",
		OpCodeClone:         "CL",
		OpCodeVerHandshake:  "VH",
	}
)

var (
	opStatusNameMap map[OpStatus]string = map[OpStatus]string{
		OpStatusNoError:            "Ok",                         //0
		OpStatusBadMsg:             "BadMsg",                     //1
		OpStatusServiceDenied:      "ServiceDenied",              //2
		OpStatusNoKey:              "NoKey",                      //3
		OpStatusDupKey:             "DupKey",                     //4
		OpStatusBadParam:           "BadParam",                   //7
		OpStatusRecordLocked:       "RecordLocked",               //8
		OpStatusNoUncommitted:      "NoUncommitted",              //10
		OpStatusNoStorageServer:    "NoStorageServer",            //12
		OpStatusBusy:               "Busy",                       //14
		OpStatusInserting:          "Inserting",                  //15
		OpStatusAlreadyFulfilled:   "AlreadyFulfilled",           //17
		OpStatusVersionConflict:    "VersionConflict",            //19
		OpStatusSSError:            "StorageServerErr",           //21
		OpStatusSSOutofResource:    "SSOutofResource",            //22
		OpStatusSSReadTTLExtendErr: "OpStatusSSReadTTLExtendErr", //23
		OpStatusReqProcTimeout:     "RequestProcTimeout",         //24
		OpStatusCommitFailure:      "CommitFailure",              //25
		OpStatusInconsistent:       "InconsistentState",          //26
		OpStatusKeyMarkedDelete:    "MarkedDelete",               //27
		OpStatusNotSupported:       "OpNotSupported",             //28
		OpStatusInternal:           "Internal",                   //255
	}

	opStatusShortNameMap map[OpStatus]string = map[OpStatus]string{
		OpStatusNoError:            "Ok",      //0
		OpStatusBadMsg:             "BadMsg",  //1
		OpStatusServiceDenied:      "DoS",     //2
		OpStatusNoKey:              "NoKey",   //3
		OpStatusDupKey:             "DupKey",  //4
		OpStatusBadParam:           "BadPar",  //7
		OpStatusRecordLocked:       "RecLck",  //8
		OpStatusNoUncommitted:      "NoUnc",   //10
		OpStatusNoStorageServer:    "NoSS",    //12
		OpStatusBusy:               "Busy",    //14
		OpStatusInserting:          "Insr",    //15
		OpStatusAlreadyFulfilled:   "Done",    //17
		OpStatusVersionConflict:    "CoV",     //19
		OpStatusSSError:            "SSErr",   //21
		OpStatusSSOutofResource:    "SSNoRes", //22
		OpStatusSSReadTTLExtendErr: "TTLErr",  //23
		OpStatusReqProcTimeout:     "Rtot",    //24
		OpStatusCommitFailure:      "CmtF",    //25
		OpStatusInconsistent:       "InConst", //26
		OpStatusKeyMarkedDelete:    "MDel",    //27
		OpStatusNotSupported:       "BadOp",   //28
		OpStatusInternal:           "Intl",    //255
	}
)

func (op OpCode) String() string {
	return opCodeNameMap[op]
}

func (op OpCode) ShortNameString() string {
	return opCodeShortNameMap[op]
}

func (s OpStatus) String() string {
	if name, ok := opStatusNameMap[s]; ok {
		return name
	}
	return "UnSpecified Status"
}

func (s OpStatus) ShortNameString() string {
	if sname, ok := opStatusShortNameMap[s]; ok {
		return sname
	}
	return "UnSpec"
}

func (op OpCode) IsForStorage() bool {
	switch op {
	case OpCodePrepareCreate, OpCodeRead, OpCodePrepareUpdate, OpCodePrepareSet, OpCodePrepareDelete,
		OpCodeDelete,
		OpCodeCommit, OpCodeAbort, OpCodeRepair, OpCodeClone, OpCodeVerHandshake, OpCodeMarkDelete,
		OpCodeMockSetParam, OpCodeMockReSet:
		return true
	}
	return false
}

var (
	ErrNotSupportedMessage   = &ProtocolError{"Message type not supported"}
	ErrInvalidBufferSize     = &ProtocolError{"Buffer size"}
	ErrInvalidMessageHeader  = &ProtocolError{"Invalid Message Header"}
	ErrInvalidMessageSize    = &ProtocolError{"Invalid Message Size"}
	ErrInvalidMessage        = &ProtocolError{"Invalid Message"}
	ErrInvalidComponentTag   = &ProtocolError{"Invalid Component Tag"}
	ErrInvalidMetaCompHeader = &ProtocolError{"Invalid Meta Component Header"}
	ErrBufferTooShort        = &ProtocolError{"Input buffer too short"}
	ErrInvalidRequestID      = &ProtocolError{"Invalid request ID"}
	ErrInvalidSourceInfo     = &ProtocolError{"Invalid source info"}
	ErrInvalidByteSequence   = &ProtocolError{"Invalid byte sequence"}
	ErrInvalidIPAddress      = &ProtocolError{"Invalid IP Address"}
	ErrInvalidMetaFieldSize  = &ProtocolError{"Invalid Meta Field Size"}
)

func NewProtocolError(err error) *ProtocolError {
	return &ProtocolError{
		what: err.Error(),
	}
}

func (e *ProtocolError) Error() string {
	return "ProtocolError: " + e.what
}

var (
	msgTypeNames = []string{
		"OperationalMessage",
		"AdminMessage",
		"ClusterControlMessage",
	}
)

func (f messageTypeFlagT) String() (str string) {
	t := f & 0x3F
	if int(t) >= len(msgTypeNames) {
		str = "Unsupported"
	} else {
		str = msgTypeNames[int(t)]
	}
	rq := ((f >> 6) & 0x3)

	switch rq {
	case 0:
		str += "(Response)"
	case 1:
		str += "(Request)"
	case 3:
		str += "(Oneway Request)"
	}
	return
}

func (f messageTypeFlagT) isRequest() bool {
	return (f&0xC0 == (0x1 << 6))
}

func (f messageTypeFlagT) isResponse() bool {
	return (f&0xC0 == 0)
}

func (f *messageTypeFlagT) setAsRequest() {
	(*f) &= 0x3F
	(*f) |= 0x40
}

func (f *messageTypeFlagT) setAsResponse() {
	(*f) &= 0x3F
}

func (f messageTypeFlagT) getMessageType() uint8 {
	return uint8(f & 0x3F)
}
