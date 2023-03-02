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
package proto

import (
	"io"
)

type Encoder struct {
	w      io.Writer
	rawMsg RawMessage
}

func NewEncoder(w io.Writer) *Encoder {
	return &Encoder{w: w}
}

func (enc *Encoder) Encode(msg IMessage) (err error) {
	switch msg.(type) {
	case *OperationalMessage:
		err = msg.(*OperationalMessage).Encode(&enc.rawMsg)
	default:
		err = ErrNotSupportedMessage
	}
	if err == nil {
		_, err = enc.rawMsg.Write(enc.w)
	}
	return
}

// !!! When making change to getInfoForMetaHeader(), double check encodeMetaFields() to  !!!
// !!! make sure the sequence of the meta fields are the same                            !!!
func (m *OperationalMessage) getInfoForMetaHeader(tagAndSizeTypes []uint8) (numFields int, totalSize int) {
	if m.requestHandlingTime.isSet() {
		tagAndSizeTypes[numFields] = m.requestHandlingTime.tagAndSizeTypeByte()
		totalSize += m.requestHandlingTime.size()
		numFields++
	}
	if m.requestID.isSet() {
		tagAndSizeTypes[numFields] = m.requestID.tagAndSizeTypeByte()
		totalSize += m.requestID.size()
		numFields++
	}
	if m.originatorRequestID.isSet() {
		tagAndSizeTypes[numFields] = m.originatorRequestID.tagAndSizeTypeByte()
		totalSize += m.originatorRequestID.size()
		numFields++
	}
	if m.lastModificationTime.isSet() {
		tagAndSizeTypes[numFields] = m.lastModificationTime.tagAndSizeTypeByte()
		totalSize += m.lastModificationTime.size()
		numFields++
	}
	if m.timeToLive.isSet() {
		tagAndSizeTypes[numFields] = m.timeToLive.tagAndSizeTypeByte()
		totalSize += m.timeToLive.size()
		numFields++
	}
	if m.version.isSet() {
		tagAndSizeTypes[numFields] = m.version.tagAndSizeTypeByte()
		totalSize += m.version.size()
		numFields++
	}
	if m.creationTime.isSet() {
		tagAndSizeTypes[numFields] = m.creationTime.tagAndSizeTypeByte()
		totalSize += m.creationTime.size()
		numFields++
	}
	if m.expirationTime.isSet() {
		tagAndSizeTypes[numFields] = m.expirationTime.tagAndSizeTypeByte()
		totalSize += m.expirationTime.size()
		numFields++
	}
	if m.correlationID.isSet() {
		tagAndSizeTypes[numFields] = m.correlationID.tagAndSizeTypeByte()
		totalSize += m.correlationID.size()
		numFields++
	}

	if m.sourceInfo.isSet() {
		tagAndSizeTypes[numFields] = m.sourceInfo.tagAndSizeTypeByte()
		totalSize += m.sourceInfo.size()
		numFields++
	}

	if m.udfName.isSet() {
		tagAndSizeTypes[numFields] = m.udfName.tagAndSizeTypeByte()
		totalSize += m.udfName.size()
		numFields++
	}

	return
}

// !!! When making change to encodeMetaFields(), double check getInfoForMetaHeader() to  !!!
// !!! make sure the sequence of the meta fields are the same
// !!! Also SetRequestHandlingTime assumes it is the first metafield.                            !!!
func (m *OperationalMessage) encodeMetaFields(szComp int, buf []byte) (err error) {
	if szComp > len(buf) {
		return ErrInvalidBufferSize
	}
	off := 0
	fsz := 0

	if m.requestHandlingTime.isSet() {
		if fsz, err = m.requestHandlingTime.encode(buf[off:]); err != nil {
			return
		}
		off += fsz
	}

	if m.requestID.isSet() {
		if fsz, err = m.requestID.encode(buf[off:]); err != nil {
			return
		}
		off += fsz
	}
	if m.originatorRequestID.isSet() {
		if fsz, err = m.originatorRequestID.encode(buf[off:]); err != nil {
			return
		}
		off += fsz
	}
	if m.lastModificationTime.isSet() {
		if fsz, err = m.lastModificationTime.encode(buf[off:]); err != nil {
			return
		}
		off += fsz
	}

	if m.timeToLive.isSet() {
		if fsz, err = m.timeToLive.encode(buf[off:]); err != nil {
			return
		}
		off += fsz
	}
	if m.version.isSet() {
		if fsz, err = m.version.encode(buf[off:]); err != nil {
			return
		}
		off += fsz
	}
	if m.creationTime.isSet() {
		if fsz, err = m.creationTime.encode(buf[off:]); err != nil {
			return
		}
		off += fsz
	}
	if m.expirationTime.isSet() {
		if fsz, err = m.expirationTime.encode(buf[off:]); err != nil {
			return
		}
		off += fsz
	}
	if m.correlationID.isSet() {
		if fsz, err = m.correlationID.encode(buf[off:]); err != nil {
			return
		}
		off += fsz
	}
	if m.sourceInfo.isSet() {
		if fsz, err = m.sourceInfo.encode(buf[off:]); err != nil {
			return
		}
		off += fsz
	}
	if m.udfName.isSet() {
		if fsz, err = m.udfName.encode(buf[off:]); err != nil {
			return
		}
		off += fsz
	}

	for ; off < szComp; off++ {
		buf[off] = 0
	}
	return
}

func (op *OperationalMessage) Encode(wMsg *RawMessage) (err error) {
	wMsg.magic = kMessageMagic
	wMsg.version = kCurrentVersion
	wMsg.typeFlag = op.typeFlag
	wMsg.opaque = op.GetOpaque()
	header := operationalHeaderT{
		opCode:          op.opCode,
		flags:           op.flags,
		shardIdOrStatus: op.shardIdOrStatus,
	}

	// Always include requestHandlingTime in some responses.
	if op.typeFlag.isResponse() &&
		(op.opCode == OpCodeRead || op.opCode == OpCodeCommit) &&
		!op.requestHandlingTime.isSet() {
		op.requestHandlingTime.set(1)
	}

	var fieldTagAndSizeTypes [kNumSupportedFields]uint8
	numFields, szMetaComp := op.getInfoForMetaHeader(fieldTagAndSizeTypes[:])
	meta := metaComponentT{
		metaComponentHeaderT: metaComponentHeaderT{
			componentHeaderT: componentHeaderT{tagComp: kCompTagMeta},
			numFields:        uint8(numFields),
		},
	}
	szMetaHeader := 4 + 1 + 1 + numFields
	szMetaHeaderPadding := uint8((4 - szMetaHeader%4) % 4)
	szMetaHeader += int(szMetaHeaderPadding)
	szMetaComp += szMetaHeader
	szMetaPadding := uint8((8 - szMetaComp%8) % 8)
	meta.szComp = uint32(szMetaComp) + uint32(szMetaPadding)
	meta.szHeaderPadding = szMetaHeaderPadding
	meta.szCompPadding = szMetaPadding

	szPayload := op.payload.GetLength()
	payload := payloadComponentT{
		payloadComponentHeaderT: payloadComponentHeaderT{
			componentHeaderT: componentHeaderT{tagComp: kCompTagPayload},
			szNamespace:      uint8(len(op.GetNamespace())),
			szKey:            uint16(len(op.GetKey())),
			szValue:          szPayload},
		key:       op.GetKey(),
		namespace: op.GetNamespace(),
		payload:   op.payload}
	szPayloadCompSize := 4 + 1 + 4 + 2 + 1 + len(op.GetNamespace()) + len(op.GetKey()) + int(szPayload)
	szPayloadCompPadding := uint8((8 - szPayloadCompSize%8) % 8)
	payload.szCompPadding = szPayloadCompPadding
	payload.szComp = uint32(szPayloadCompSize) + uint32(szPayloadCompPadding)
	wMsg.msgSize = kOperationalMessageHeaderSize + payload.szComp + meta.szComp

	wMsg.allocateBuffer(int(wMsg.msgSize - kMessageHeaderSize))
	bytes := wMsg.GetBody()
	if err = header.encode(bytes[0:kOpMsgSubHeaderSize]); err != nil {
		return
	}
	offset := kOpMsgSubHeaderSize

	if err = meta.encodeHeader(bytes[offset:], fieldTagAndSizeTypes[:]); err != nil {
		return
	}

	offComp := offset + szMetaHeader
	if err = op.encodeMetaFields(int(meta.szComp), bytes[offComp:]); err != nil {
		return
	}

	///TODO: check size again
	offset += int(meta.szComp)
	err = payload.encode(bytes[offset : offset+int(payload.szComp)])
	return
}

func (h *messageHeaderT) encode(buf []byte) error {
	if len(buf) != kMessageHeaderSize {
		return ErrInvalidBufferSize
	}
	EncByteOrder.PutUint16(buf[0:2], h.magic)
	buf[2] = h.version
	buf[3] = byte(h.typeFlag)
	EncByteOrder.PutUint32(buf[4:8], h.msgSize)
	EncByteOrder.PutUint32(buf[8:12], h.opaque)
	return nil
}

func (h *operationalHeaderT) encode(buf []byte) error {
	if len(buf) != kOpMsgSubHeaderSize {
		return ErrInvalidBufferSize
	}

	buf[0] = uint8(h.opCode)
	buf[1] = uint8(h.flags)
	buf[2] = h.shardIdOrStatus[0]
	buf[3] = h.shardIdOrStatus[1]

	return nil
}

func (m *metaComponentT) encodeHeader(buf []byte, infieldTagsAndSizeTypes []uint8) error {
	if m.szComp != 0 {
		sz := 4 + 1 + 1 + int(m.numFields) + int(m.szHeaderPadding)

		if len(buf) < sz {
			return ErrInvalidBufferSize
		}
		offset := 0
		EncByteOrder.PutUint32(buf[0:4], m.szComp)
		offset += 4
		buf[offset] = m.tagComp
		offset++
		buf[offset] = m.numFields
		offset++
		copy(buf[offset:offset+int(m.numFields)], infieldTagsAndSizeTypes[0:int(m.numFields)]) //m.fieldTagAndSizeTypes)
		offset += int(m.numFields)
		for ; offset < sz; offset++ {
			buf[offset] = 0
		}
	}
	return nil
}

func (m *payloadComponentT) encode(buf []byte) error {
	if m.szComp != 0 {
		if len(buf) != int(m.szComp) {
			return ErrInvalidBufferSize
		}
		EncByteOrder.PutUint32(buf[0:4], m.szComp)
		buf[4] = m.tagComp
		buf[5] = m.szNamespace
		EncByteOrder.PutUint16(buf[6:8], m.szKey)
		EncByteOrder.PutUint32(buf[8:12], m.szValue)
		offset := 12
		copy(buf[offset:offset+int(m.szNamespace)], m.namespace)
		offset += int(m.szNamespace)
		copy(buf[offset:offset+int(m.szKey)], m.key)
		offset += int(m.szKey)

		if m.szValue != 0 {
			buf[offset] = byte(m.payload.tag)
			copy(buf[offset+1:offset+int(m.szValue)], m.payload.data)
			offset += int(m.szValue)
		}
		for i := 0; i < int(m.szCompPadding); i++ {
			buf[offset] = 0
			offset++
		}
	}
	return nil
}

func SetOpCode(wmsg *RawMessage, opcode OpCode) (err error) {
	if wmsg.getMsgType() != kOperationalMessageType || len(wmsg.body) < kOpMsgSubHeaderSize {
		err = &ProtocolError{"Not Operational Message"}
		return
	}
	wmsg.body[0] = byte(opcode)
	return
}

func SetOpStatus(wmsg *RawMessage, st OpStatus) (err error) {
	if wmsg.typeFlag != kOperationalMessageType || len(wmsg.body) < kOpMsgSubHeaderSize {
		if !wmsg.typeFlag.isResponse() {
			err = &ProtocolError{"try to set OpStatus for Operational request"}
		} else {
			err = &ProtocolError{"Not Operational Message"}
		}
		return
	}
	wmsg.body[offsetStatusWithinOpbHeader] = byte(st)
	return
}

func SetShardId(wmsg *RawMessage, vid uint16) (err error) {
	if wmsg.typeFlag.getMessageType() != kOperationalMessageType || len(wmsg.body) < kOpMsgSubHeaderSize {
		err = &ProtocolError{"Not Operational Message"}
		return
	}
	if wmsg.typeFlag.isResponse() {
		err = &ProtocolError{"try to set vbucket Id for operational response"}
		return
	}
	EncByteOrder.PutUint16(wmsg.body[offsetShardIdWithinOpbHeader:offsetShardIdWithinOpbHeader+2], vid)
	return
}

// Set RequestHandlingTime in meta component
// First meta field should be RequestHandlingTime if any.
func SetRequestHandlingTime(raw *RawMessage, t uint32) {

	bytes := raw.GetBody()
	if raw.typeFlag.getMessageType() != kOperationalMessageType ||
		len(bytes) < kOpMsgSubHeaderSize {
		return
	}

	// meta header
	header := bytes[kOpMsgSubHeaderSize:]
	if len(header) < 16 || uint8(header[4]) != kCompTagMeta {
		return
	}

	numFields := uint8(header[5])

	// First field tag should be RequestHandlingTime if any
	var tag requestHandlingTimeT
	if uint8(header[6]) != tag.tagAndSizeTypeByte() {
		return
	}

	szMetaHeader := 4 + 1 + 1 + numFields
	padding := uint8(4 - szMetaHeader%4)
	szMetaHeader += padding
	if len(header) < int(szMetaHeader+4) {
		return
	}

	// First field after the header.
	buf := header[szMetaHeader:]

	var rht uint32T
	if t == 0 {
		t = 1 // round up
	}
	rht.set(t)
	rht.encode(buf)
}
