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
	"io"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/util"
)

const (
	offsetStatusWithinOpbHeader  = 3
	offsetShardIdWithinOpbHeader = 2
)

type Decoder struct {
	r io.Reader
}

func NewDecoder(r io.Reader) *Decoder {
	return &Decoder{r: r}
}

func (dec *Decoder) Decode(op *OperationalMessage) error {
	var header messageHeaderT
	var buf [kMessageHeaderSize]byte
	rawHeader := buf[:]
	if n, err := io.ReadFull(dec.r, rawHeader); err != nil {
		if n == 0 {
			return err
		} else {
			return NewProtocolError(err)
		}
	}
	if err := header.Decode(rawHeader); err != nil {
		return err
	}
	szBufNeeded := header.msgSize - kMessageHeaderSize

	pool := util.GetBufferPool(int(szBufNeeded))
	buffer := pool.Get()
	defer pool.Put(buffer)

	buffer.Resize(int(szBufNeeded))
	raw := buffer.Bytes()

	if _, err := io.ReadFull(dec.r, raw); err != nil {
		return NewProtocolError(err)
	}
	return op.decode(raw, &header, true)
}

//Caller's responsibility to have op zeroed
func (op *OperationalMessage) decode(raw []byte, msgHeader *messageHeaderT, copyData bool) error {
	offset := 0
	szBuf := len(raw)
	if szBuf < kOperationalMessageHeaderSize {
		return ErrBufferTooShort
	}
	if szBuf+kMessageHeaderSize < int(msgHeader.msgSize) {
		return ErrInvalidMessageSize
	}
	op.SetOpaque(msgHeader.opaque) //might have done this

	var header operationalHeaderT
	if err := header.decode(raw[0:kOpMsgSubHeaderSize]); err != nil {
		return err
	}

	op.opCode = header.opCode
	op.flags = header.flags
	op.shardIdOrStatus = header.shardIdOrStatus
	op.typeFlag = msgHeader.typeFlag

	if msgHeader.msgSize == kOperationalMessageHeaderSize {
		return nil
	}

	offset += kOpMsgSubHeaderSize

	for offset+5 < szBuf {
		var szComponent uint32
		szComponent = EncByteOrder.Uint32(raw[offset : offset+4])
		if szComponent <= 5 {
			return ErrInvalidMessage
		} else if int(szComponent)+offset > szBuf {
			return ErrInvalidMessage
		}
		rawComp := raw[offset : offset+int(szComponent)]

		switch rawComp[4] {
		case kCompTagMeta:
			op.decodeMetaComponent(rawComp, szComponent, copyData)
		case kCompTagPayload:
			op.decodePayloadComponent(rawComp, szComponent, copyData)
		default:
			glog.Warningf("skipping unknown compoment. tag: %x", rawComp[4])
			//
		}
		offset += int(szComponent)
	}
	return nil
}

func (op *OperationalMessage) decodeMetaField(tag byte, szField uint8, raw []byte, copyData bool) (err error) {
	switch tag {
	case kFieldTagLastModificationTime:
		if err = op.lastModificationTime.decode(raw); err != nil {
			return
		}
	case kFieldTagTimeToLive:
		if err = op.timeToLive.decode(raw); err != nil {
			return
		}
	case kFieldTagVersion:
		if err = op.version.decode(raw); err != nil {
			return
		}
	case kFieldTagCreationTime:
		if err = op.creationTime.decode(raw); err != nil {
			return
		}
	case kFieldTagExpirationTime:
		if err = op.expirationTime.decode(raw); err != nil {
			return
		}
	case kFieldTagRequestID:
		if err = op.requestID.decode(szField, raw, copyData); err != nil {
			return
		}
	case kFieldTagSourceInfo:
		if err = op.sourceInfo.decode(szField, raw, copyData); err != nil {
			return
		}
	case kFieldTagOriginatorRequestID:
		if err = op.originatorRequestID.decode(szField, raw, copyData); err != nil {
			return
		}
	case kFieldTagCorrelationID:
		if err = op.correlationID.decode(szField, raw, copyData); err != nil {
			return
		}
	case kFieldTagRequestHandlingTime:
		if err = op.requestHandlingTime.decode(raw); err != nil {
			return
		}
	case kFieldTagUDFName:
		if err = op.udfName.decode(szField, raw, copyData); err != nil {
			return
		}
	default:

	}
	return
}

func (op *OperationalMessage) decodeMetaComponent(raw []byte, szComp uint32, copyData bool) error {
	offset := 5
	if len(raw) != int(szComp) {
		return ErrInvalidMessage
	}
	var numFields uint8 = uint8(raw[offset])
	offset++

	headerTags := raw[offset : offset+int(numFields)]
	offset += int(numFields)

	szHeader := 4 + 1 + 1 + numFields
	szHeaderPadding := uint8((4 - szHeader%4) % 4)
	szHeader += szHeaderPadding

	offset += int(szHeaderPadding)

	szComponent := int(szHeader)

	var i uint8 = 0
	for ; i < numFields; i++ {
		sizeType := headerTags[i] >> 5
		tag := headerTags[i] & 0x1F
		var szField uint8 = 0
		if sizeType == 0 {
			szField = raw[offset]
		} else {
			szField = 1 << (sizeType + 1)
		}
		szComponent += int(szField)

		if err := op.decodeMetaField(tag, szField, raw[offset:offset+int(szField)], copyData); err != nil {
			return err
		} else {
			offset += int(szField)
		}

	}
	szPadding := uint8((8 - szComponent%8) % 8)
	if int(szComp) != offset+int(szPadding) {
		return ErrInvalidMessage
	}
	return nil
}

func (op *OperationalMessage) decodePayloadComponent(raw []byte, szComp uint32, copyData bool) error {
	offset := 0
	//tagComp := raw[4]
	szNamespace := raw[5]
	szKey := EncByteOrder.Uint16(raw[6:8])
	szValue := EncByteOrder.Uint32(raw[8:12])

	offset += 12
	if copyData == true {
		op.namespace = make([]byte, szNamespace)
		copy(op.namespace, raw[offset:offset+int(szNamespace)])
		offset += int(szNamespace)
		op.key = make([]byte, szKey)
		copy(op.key, raw[offset:offset+int(szKey)])
		offset += int(szKey)

	} else {
		op.namespace = raw[offset : offset+int(szNamespace)]
		offset += int(szNamespace)
		op.key = raw[offset : offset+int(szKey)]
		offset += int(szKey)
	}

	if szValue > 1 {
		op.payload.Decode(raw[offset:offset+int(szValue)], copyData)
	} else {
		op.payload.Clear()
	}

	return nil
}

func (h *messageHeaderT) Decode(raw []byte) error {
	if len(raw) != kMessageHeaderSize {
		return ErrInvalidMessageHeader
	}

	h.magic = EncByteOrder.Uint16(raw[0:2])
	h.version = raw[2]
	h.typeFlag = messageTypeFlagT(raw[3])

	h.msgSize = EncByteOrder.Uint32(raw[4:8])
	h.opaque = EncByteOrder.Uint32(raw[8:12])

	return nil
}

func (h *operationalHeaderT) decode(raw []byte) error {
	if len(raw) != kOpMsgSubHeaderSize {
		return ErrInvalidMessage
	}
	h.opCode = OpCode(raw[0])
	h.flags = opMsgFlagT(raw[1])
	h.shardIdOrStatus[0] = raw[2]
	h.shardIdOrStatus[1] = raw[3]

	return nil
}

func GetOpCodeAndOpStatus(wmsg *RawMessage) (opCode OpCode, opStatus OpStatus, err error) {
	if wmsg.typeFlag != kOperationalMessageType || len(wmsg.body) < kOpMsgSubHeaderSize {
		if !wmsg.typeFlag.isResponse() {
			err = &ProtocolError{"get OpStatus from non-response Operational message"}
		} else {
			err = &ProtocolError{"not Operational Message"}
			return
		}
	}
	opCode = OpCode(wmsg.body[0])
	opStatus = OpStatus(wmsg.body[offsetStatusWithinOpbHeader])
	return
}

func GetOpCode(wmsg *RawMessage) (opCode OpCode, err error) {
	if wmsg.getMsgType() != kOperationalMessageType || len(wmsg.body) < kOpMsgSubHeaderSize {
		err = &ProtocolError{"not Operational Message"}
		return
	}
	opCode = OpCode(wmsg.body[0])
	return
}

func GetOpCodeAndFlag(wmsg *RawMessage) (opCode OpCode, flag uint8, err error) {
	if wmsg.getMsgType() != kOperationalMessageType || len(wmsg.body) < kOpMsgSubHeaderSize {
		err = &ProtocolError{"not Operational Message"}
		return
	}
	opCode = OpCode(wmsg.body[0])
	flag = uint8(wmsg.body[1])
	return
}

func GetOpStatus(wmsg *RawMessage) (opStatus OpStatus, err error) {
	if wmsg.typeFlag.getMessageType() != kOperationalMessageType || len(wmsg.body) < kOpMsgSubHeaderSize {
		if !wmsg.typeFlag.isResponse() {
			err = &ProtocolError{"get OpStatus from non-response Operational message"}
		} else {
			err = &ProtocolError{"not Operational Message"}
		}
		return
	}
	opStatus = OpStatus(wmsg.body[offsetStatusWithinOpbHeader])
	return
}

func (op *OperationalMessage) Decode(wmsg *RawMessage) error {
	return op.decode(wmsg.body, &wmsg.messageHeaderT, false)
}
