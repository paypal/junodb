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
package mayfly

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"juno/pkg/net/netutil"
	"juno/pkg/util"
)

type (
	opMsgHeaderT struct {
		recordInfoLen    uint32
		requestIdLen     uint32
		optionalDataLen  uint32
		payloadLen       uint32
		opcode           OpCode
		opmode           OpMode
		replicationState uint8
		opstatus         OpStatus
	}
	tecordInfoT struct {
		creationTime uint32
		lifetime     uint32
		version      uint16
		namespace    []byte
		key          []byte
	}
	requestIdT struct {
		ip             uint32
		pid            uint32
		requestingTime uint32
		sequence       uint16
	}
	opMsgT struct {
		recordInfo       tecordInfoT
		requestId        requestIdT
		value            []byte
		optionalData     []byte
		opcode           OpCode
		opstatus         OpStatus
		opmode           OpMode
		isForReplication bool
	}
)

const (
	offsetRecordInfoLen   = 0
	szRecordInfoLen       = 4
	offsetRequestIdLen    = offsetRecordInfoLen + szRecordInfoLen
	szRequestIdLen        = 4
	offsetOptionalDataLen = offsetRequestIdLen + szRequestIdLen
	szOptionalDataLen     = 4
	offsetPayloadLen      = offsetOptionalDataLen + szOptionalDataLen
	szPayloadLen          = 4

	offsetOpType           = offsetPayloadLen + szPayloadLen
	szOpType               = 2
	offsetOpMode           = offsetOpType + szOpType
	szOpMode               = 1
	offsetReplicationState = offsetOpMode + szOpMode
	szReplicationState     = 1
	offsetOpStatus         = offsetReplicationState + szReplicationState
	szOpStatus             = 2
)

var (
	gRequestIdIP       net.IP
	gRequestIdIPUint32 uint32
	gRequestIdPid      uint32
	gRequestIdSequence uint32
)

func init() {
	gRequestIdIP = netutil.GetLocalIPv4Address()
	gRequestIdIPUint32 = binary.BigEndian.Uint32(gRequestIdIP)
	gRequestIdPid = uint32(os.Getpid())
}

func (h *opMsgHeaderT) prettyPrint(w io.Writer) {
	fmt.Fprintf(w, "recordInfoLen   : %d\n", h.recordInfoLen)
	fmt.Fprintf(w, "requestIdLen    : %d\n", h.requestIdLen)
	fmt.Fprintf(w, "optionalDataLen : %d\n", h.optionalDataLen)
	fmt.Fprintf(w, "payloadLen      : %d\n", h.payloadLen)
	fmt.Fprintf(w, "opType          : %d\n", h.opcode)
	fmt.Fprintf(w, "opMode          : %d\n", h.opmode)
	fmt.Fprintf(w, "replicationState: %d\n", h.replicationState)
	fmt.Fprintf(w, "opStatus        : %d\n", h.opstatus)
}

func (h *opMsgHeaderT) decode(raw []byte) error {
	sz := len(raw)
	if sz < kOpMsgHeaderLength {
		return invalidMessageError(errInvalidBufferLength.Error())
	}
	h.recordInfoLen = binary.BigEndian.Uint32(raw[offsetRecordInfoLen : offsetRecordInfoLen+szRecordInfoLen])
	h.requestIdLen = binary.BigEndian.Uint32(raw[offsetRequestIdLen : offsetRequestIdLen+szRequestIdLen])

	h.optionalDataLen = binary.BigEndian.Uint32(raw[offsetOptionalDataLen : offsetOptionalDataLen+szOptionalDataLen])
	h.payloadLen = binary.BigEndian.Uint32(raw[offsetPayloadLen : offsetPayloadLen+szPayloadLen])

	h.opcode = OpCode(binary.BigEndian.Uint16(raw[offsetOpType : offsetOpType+szOpType]))
	h.opmode = OpMode(raw[offsetOpMode])
	h.replicationState = raw[offsetReplicationState]

	h.opstatus = OpStatus(binary.BigEndian.Uint16(raw[offsetOpStatus : offsetOpStatus+szOpStatus]))
	return nil
}

func (r *tecordInfoT) isEmpty() bool {
	return len(r.key) == 0
}

func (r *tecordInfoT) encode(raw []byte) (err error) {
	if r.isEmpty() {
		return
	}

	szNamespace := len(r.namespace)
	szKey := len(r.key)
	binary.BigEndian.PutUint32(raw[0:4], r.creationTime)
	binary.BigEndian.PutUint32(raw[4:8], r.lifetime)
	binary.BigEndian.PutUint16(raw[8:10], r.version)
	binary.BigEndian.PutUint16(raw[10:12], uint16(szNamespace))
	binary.BigEndian.PutUint16(raw[12:14], uint16(szKey))
	raw[14] = 0
	raw[15] = 0

	offset := 16
	if szNamespace > 0 {
		raw[offset] = kDataTagNamespace
		offset++
		copy(raw[offset:offset+szNamespace], r.namespace)
		offset += szNamespace
	}
	if szKey > 0 {
		raw[offset] = kDataTagKey
		offset++
		copy(raw[offset:offset+szKey], r.key)
		offset += szKey
	}

	raw[offset] = kDataTagEndOfRecordInfo
	return
}

func (r *tecordInfoT) encodingSize() (size uint32) {
	if len(r.key) == 0 {
		return
	}
	size = kRecordInfoHeaderLength
	l := uint32(len(r.namespace))
	if l != 0 {
		size++
		size += l
	}
	l = uint32(len(r.key))
	if l != 0 {
		size++
		size += l
	}
	size++
	return
}

func (r *tecordInfoT) PrettyPrint(w io.Writer) {
	fmt.Fprintf(w, "creationTime: %d\n", r.creationTime)
	fmt.Fprintf(w, "lifetime    : %d\n", r.lifetime)
	fmt.Fprintf(w, "version     : %d\n", r.version)
	fmt.Fprintf(w, "Key         : %s\n", util.ToPrintableAndHexString(r.key))
	fmt.Fprintf(w, "namespace   : %s\n", util.ToPrintableAndHexString(r.namespace))
}

func (r *tecordInfoT) decode(raw []byte) error {
	lenRaw := len(raw)
	if lenRaw < kRecordInfoHeaderLength {
		return invalidMessageError(errInvalidBufferLength.Error())
	}
	r.creationTime = binary.BigEndian.Uint32(raw[0:4])
	r.lifetime = binary.BigEndian.Uint32(raw[4:8])
	r.version = binary.BigEndian.Uint16(raw[8:10])
	szNamespace := binary.BigEndian.Uint16(raw[10:12])
	szKey := binary.BigEndian.Uint16(raw[12:14])

	szExpected := kRecordInfoHeaderLength
	numSegs := 0
	if szNamespace != 0 {
		szExpected += int(szNamespace) + 1
		numSegs++
	}
	if szKey != 0 {
		szExpected += int(szKey) + 1
		numSegs++
	}
	offset := kRecordInfoHeaderLength

	if len(raw) < szExpected {
		return fmt.Errorf("invalid message. RecordInfo buffer size (%d) < expected (%d)", len(raw), szExpected)
	}

	var tag uint8

	for i := 0; i < numSegs; i++ {
		tag = raw[offset]
		offset++
		switch tag {
		case kDataTagEndOfRecordInfo:
			///TOD
		case kDataTagNamespace:
			if szNamespace != 0 {
				r.namespace = make([]byte, szNamespace)
				copy(r.namespace, raw[offset:offset+int(szNamespace)])
				offset += int(szNamespace)
			} else {
				return ErrInvalidMessage
			}
		case kDataTagKey:
			if szKey != 0 {
				r.key = make([]byte, szKey)
				copy(r.key, raw[offset:offset+int(szKey)])
				offset += int(szKey)
			} else {
				return ErrInvalidMessage
			}
		default:
			return fmt.Errorf("invalid message. unknown tag %x", tag)
		}
	}
	tag = raw[offset]
	if tag != kDataTagEndOfRecordInfo {
		return fmt.Errorf("invalid message. unknown tag %x. 0 expected", tag)
	}
	return nil
}

func (r *requestIdT) reset() {
	r.ip = gRequestIdIPUint32
	r.pid = gRequestIdPid
	r.requestingTime = uint32(time.Now().Unix())
	r.sequence = uint16(atomic.AddUint32(&gRequestIdSequence, 1))
}

func (r *requestIdT) String() string {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, r.ip)
	return fmt.Sprintf("%s-%d-%d-%d", ip, r.pid, r.requestingTime, r.sequence)
}

func (r *requestIdT) Bytes() (raw []byte) {
	raw = make([]byte, 16)
	r.encode(raw)
	return
}

func (r *requestIdT) encodingSize() uint32 {
	return kRequestIDLength
}

func (r *requestIdT) encode(raw []byte) (err error) {
	binary.BigEndian.PutUint16(raw[0:2], 16)
	binary.BigEndian.PutUint32(raw[2:6], r.ip)
	binary.BigEndian.PutUint32(raw[6:10], r.pid)
	binary.BigEndian.PutUint32(raw[10:14], r.requestingTime)
	binary.BigEndian.PutUint16(raw[14:16], r.sequence)
	return
}

func (r *requestIdT) decode(raw []byte) error {
	szRaw := len(raw)

	if szRaw < 16 {
		return invalidMessageError(errInvalidBufferLength.Error())
	}

	r.ip = binary.BigEndian.Uint32(raw[2 : 2+4])
	r.pid = binary.BigEndian.Uint32(raw[2+4 : 2+4+4])
	r.requestingTime = binary.BigEndian.Uint32(raw[2+4+4 : 2+4+4+4])
	r.sequence = binary.BigEndian.Uint16(raw[14:16])
	return nil
}

func (r *requestIdT) PrettyPrint(w io.Writer) {
	ip := make(net.IP, 4)
	binary.BigEndian.PutUint32(ip, r.ip)
	fmt.Fprintf(w, "  IP       : %s\n", ip.String())
	fmt.Fprintf(w, "  PID      : %d\n", r.pid)
	fmt.Fprintf(w, "  Timestamp: %d\n", r.requestingTime)
	fmt.Fprintf(w, "  sequence : %d\n", r.sequence)
}

func (m *opMsgT) encodingSize() (size uint32) {
	if m.opcode == OpCodeNOP {
		return
	}

	size = kOpMsgHeaderLength

	size++
	size += m.recordInfo.encodingSize()

	size++
	size += m.requestId.encodingSize()
	l := uint32(len(m.optionalData))

	if l != 0 {
		size++
		size += l
	}

	l = uint32(len(m.value))
	if l != 0 {
		size++
		size += l
	}
	size++
	return
}

func (h *opMsgHeaderT) encode(raw []byte) (err error) {
	szRaw := len(raw)
	if szRaw < kOpMsgHeaderLength {
		return errNotEoughBuffer
	}

	binary.BigEndian.PutUint32(raw[0:4], h.recordInfoLen)
	binary.BigEndian.PutUint32(raw[4:8], h.requestIdLen)
	binary.BigEndian.PutUint32(raw[8:12], h.optionalDataLen)
	binary.BigEndian.PutUint32(raw[12:16], h.payloadLen)
	binary.BigEndian.PutUint16(raw[16:18], uint16(h.opcode))
	raw[18] = byte(h.opmode)
	raw[19] = byte(h.replicationState)
	binary.BigEndian.PutUint16(raw[20:22], uint16(h.opstatus))
	raw[22] = 0
	raw[23] = 0
	return
}

func (o *opMsgT) PrettyPrint(w io.Writer) {
	o.recordInfo.PrettyPrint(w)
	szValue := len(o.value)
	if szValue == 0 {
		fmt.Fprintln(w, "[]")
	} else if szValue < 24 {
		fmt.Fprintln(w, util.ToPrintableAndHexString(o.value))
	} else {
		fmt.Fprintf(w, "(first 24 bytes) %s\n", util.ToPrintableAndHexString(o.value[:24]))
	}
}

func (o *opMsgT) encode(raw []byte) (err error) {
	szRaw := len(raw)
	if szRaw != int(o.encodingSize()) {
		panic("aa")
	}
	h := &opMsgHeaderT{
		recordInfoLen:   o.recordInfo.encodingSize(),
		requestIdLen:    o.requestId.encodingSize(),
		optionalDataLen: uint32(len(o.optionalData)),
		payloadLen:      uint32(len(o.value)),
		opcode:          o.opcode,
		opmode:          o.opmode,
		opstatus:        o.opstatus,
	}
	if o.isForReplication {
		h.replicationState = 1
	} else {
		h.replicationState = 0
	}

	err = h.encode(raw[0:kOpMsgHeaderLength])
	if err != nil {
		return
	}
	var offset uint32 = kOpMsgHeaderLength
	raw[offset] = kDataTagRecordInfo
	offset++
	err = o.recordInfo.encode(raw[offset : offset+h.recordInfoLen])
	if err != nil {
		return
	}

	offset += h.recordInfoLen
	raw[offset] = kDataTagRequestID
	offset++
	if err = o.requestId.encode(raw[offset : offset+h.requestIdLen]); err != nil {
		return
	}
	offset += h.requestIdLen

	if h.optionalDataLen != 0 {
		raw[offset] = kDataTagOptionalData
		offset++
		copy(raw[offset:offset+h.optionalDataLen], o.optionalData)
		offset += h.optionalDataLen
	}

	if h.payloadLen != 0 {
		raw[offset] = kDataTagPayload
		offset++
		copy(raw[offset:offset+h.payloadLen], o.value)
		offset += h.payloadLen
	}
	raw[offset] = kDataTagEndOfOpMsg
	offset++

	return nil
}

func (o *opMsgT) validate() error {
	if o.opcode >= kNumOpCodes {
		return fmt.Errorf("unsupported OpCode %d", o.opcode)
	}
	if o.opmode >= kNumOpModes {
		return fmt.Errorf("unsupported OpMode %d", o.opmode)
	}

	if o.recordInfo.isEmpty() {
		return fmt.Errorf("key not set")
	}
	if o.requestId.requestingTime == 0 {
		return fmt.Errorf("request ID not set")
	}
	return nil
}

func (o *opMsgT) Decode(raw []byte) error {
	var header opMsgHeaderT
	if err := header.decode(raw); err != nil {
		return err
	}
	o.opcode = header.opcode
	o.opmode = header.opmode
	o.opstatus = header.opstatus
	o.isForReplication = (header.replicationState != 0)

	var szExpected uint32 = kOpMsgHeaderLength
	numDataSegments := 0
	if header.recordInfoLen != 0 {
		szExpected += header.recordInfoLen + 1
		numDataSegments++
	}
	if header.requestIdLen != 0 {
		szExpected += header.requestIdLen + 1
		numDataSegments++
	}
	if header.optionalDataLen != 0 {
		szExpected += header.optionalDataLen + 1
		numDataSegments++
	}
	if header.payloadLen != 0 {
		szExpected += header.payloadLen + 1
		numDataSegments++
	}

	if len(raw) < int(szExpected) {
		return fmt.Errorf("invalid message. OpMsg buffer size (%d) < expected (%d)", len(raw), szExpected)
	}
	var tag uint8
	offset := kOpMsgHeaderLength

	for i := 0; i < numDataSegments; i++ {
		tag = raw[offset]
		offset++
		switch tag {
		case kDataTagRecordInfo:
			if header.recordInfoLen != 0 {
				if err := o.recordInfo.decode(raw[offset : offset+int(header.recordInfoLen)]); err != nil {
					return err
				}
				offset += int(header.recordInfoLen)
			} else {
				return ErrInvalidMessage
			}
		case kDataTagRequestID:
			if header.requestIdLen != 0 {
				if err := o.requestId.decode(raw[offset : offset+kRequestIDLength]); err != nil {
					return err
				}
				offset += int(header.requestIdLen)
			} else {
				return ErrInvalidMessage
			}
		case kDataTagPayload:
			if header.payloadLen != 0 {
				o.value = make([]byte, header.payloadLen)
				copy(o.value, raw[offset:offset+int(header.payloadLen)])
				offset += int(header.payloadLen)
			} else {
				return ErrInvalidMessage
			}
		case kDataTagOptionalData:
			if header.optionalDataLen != 0 {
				o.optionalData = make([]byte, header.optionalDataLen)
				copy(o.optionalData, raw[offset:offset+int(header.optionalDataLen)])
				offset += int(header.optionalDataLen)
			} else {
				return ErrInvalidMessage
			}
		case kDataTagEndOfOpMsg:
			//TODO
		default:
			//TODO

		}
	}
	if err := o.validate(); err != nil {
		return invalidMessageError(err.Error())
	}
	return nil
}

func NewRequestIdFromString(idStr string) (rid *requestIdT, err error) {
	//10.13.71.111-3851-1552666349-27130
	var fields []string
	if fields = strings.Split(idStr, "-"); len(fields) == 4 {
		ip := binary.BigEndian.Uint32(net.ParseIP(fields[0]).To4())
		var pid, tm, seq int
		if pid, err = strconv.Atoi(fields[1]); err != nil {
			return
		}
		if tm, err = strconv.Atoi(fields[2]); err != nil {
			return
		}
		if seq, err = strconv.Atoi(fields[3]); err != nil {
			return
		}

		rid = &requestIdT{
			ip:             ip,
			pid:            uint32(pid),
			requestingTime: uint32(tm),
			sequence:       uint16(seq),
		}
	}
	return
}

func NewRequestIdFromBytes(raw []byte) (rid *requestIdT, err error) {
	var id requestIdT
	if err := id.decode(raw); err == nil {
		rid = &id
	}
	return
}
