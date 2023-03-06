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
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"juno/pkg/util"
)

type Msg struct {
	header  headerT
	opMsg   opMsgT
	appName []byte
}

type headerT struct {
	magic         [4]byte
	szMessage     uint32
	szHeader      uint32
	szOpMsg       uint32
	szAppName     uint32
	senderIP      uint32
	recipientIP   uint32
	senderPort    uint16
	recipientPort uint16
	senderType    uint16
	direction     uint16
	siteId        uint32
}

const (
	offsetSzMessage         = 4
	offsetSzHeader          = 8
	offsetSzOpMsg           = 12
	offsetSzAppName         = 16
	offsetSenderIP          = 20
	offsetRecipientIP       = 24
	offsetSenderPort        = 28
	offsetRecipientPort     = 30
	offsetSenderType        = 32
	offsetDirection         = 34
	offsetSiteID            = 36
	offsetWhatFollowsHeader = 40
)

func (h *headerT) encode(raw []byte) error {
	szRaw := len(raw)
	if szRaw < kHeaderLength {
		return errNotEoughBuffer
	}
	copy(raw[0:4], h.magic[:])
	binary.BigEndian.PutUint32(raw[4:8], h.szMessage)
	binary.BigEndian.PutUint32(raw[8:12], h.szHeader)
	binary.BigEndian.PutUint32(raw[12:16], h.szOpMsg)
	binary.BigEndian.PutUint32(raw[16:20], h.szAppName)
	binary.BigEndian.PutUint32(raw[20:24], h.senderIP)
	binary.BigEndian.PutUint32(raw[24:28], h.recipientIP)
	binary.BigEndian.PutUint16(raw[28:30], h.senderPort)
	binary.BigEndian.PutUint16(raw[30:32], h.recipientPort)
	binary.BigEndian.PutUint16(raw[32:34], h.senderType)
	binary.BigEndian.PutUint16(raw[34:36], h.direction)
	binary.BigEndian.PutUint32(raw[36:40], h.siteId)
	return nil
}

func (h *headerT) decode(rawHeader []byte) error {
	if len(rawHeader) < kHeaderLength {
		return invalidMessageError(errInvalidHeaderLength.Error())
	}
	if bytes.Compare(rawHeader[:4], MayflyMagic[:]) != 0 {
		return invalidMessageError(errInvalidMagic.Error())
	}
	h.szMessage = binary.BigEndian.Uint32(rawHeader[offsetSzMessage:offsetSzHeader])
	h.szHeader = binary.BigEndian.Uint32(rawHeader[offsetSzHeader:offsetSzOpMsg])
	h.szOpMsg = binary.BigEndian.Uint32(rawHeader[offsetSzOpMsg:offsetSzAppName])
	h.szAppName = binary.BigEndian.Uint32(rawHeader[offsetSzAppName:offsetSenderIP])
	h.senderIP = binary.BigEndian.Uint32(rawHeader[offsetSenderIP:offsetRecipientIP])
	h.recipientIP = binary.BigEndian.Uint32(rawHeader[offsetRecipientIP:offsetSenderPort])
	h.senderPort = binary.BigEndian.Uint16(rawHeader[offsetSenderPort:offsetRecipientPort])
	h.recipientPort = binary.BigEndian.Uint16(rawHeader[offsetRecipientPort:offsetSenderType])
	h.senderType = binary.BigEndian.Uint16(rawHeader[offsetSenderType:offsetDirection])
	h.direction = binary.BigEndian.Uint16(rawHeader[offsetDirection:offsetSiteID])
	h.siteId = binary.BigEndian.Uint32(rawHeader[offsetSiteID:offsetWhatFollowsHeader])
	if err := h.validate(); err != nil {
		return invalidMessageError(err.Error())
	}
	return nil
}

func (h *headerT) validate() error {
	if h.szHeader != kHeaderLength {
		return fmt.Errorf("invalid header size (%d != %d)", h.szHeader, kHeaderLength)
	}
	szMessage := h.szHeader + 1
	if h.szOpMsg != 0 {
		szMessage += h.szOpMsg + 1
	}
	if h.szAppName != 0 {
		szMessage += h.szAppName + 1
	}
	if h.szMessage != szMessage {
		return fmt.Errorf("invalid message size (%d != %d)", h.szMessage, szMessage)
	}

	if h.senderType >= kNumSendTypes {
		return fmt.Errorf("unknown sender type %d", h.senderType)
	}

	if h.senderIP == 0 {
		return fmt.Errorf("bad sender ip (0).")
	}
	if h.senderPort == 0 {
		return fmt.Errorf("bad sender port (0).")
	}
	if h.recipientIP == 0 {
		return fmt.Errorf("bad recipient ip (0).")
	}
	if h.recipientPort == 0 {
		return fmt.Errorf("bad recipient port (0).")
	}
	return nil
}

func (h *headerT) PrettyPrint(w io.Writer) {
	fmt.Fprint(w, "Message Header:")
	fmt.Fprintf(w, " szMessage    : %d\n", h.szMessage)
	fmt.Fprintf(w, " szHeader     : %d\n", h.szHeader)
	fmt.Fprintf(w, " szOpMsg      : %d\n", h.szOpMsg)
	fmt.Fprintf(w, " szAppName    : %d\n", h.szAppName)
	fmt.Fprintf(w, " senderIP     : %d\n", h.senderIP)
	fmt.Fprintf(w, " recipientIP  : %d\n", h.recipientIP)
	fmt.Fprintf(w, " senderPort   : %d\n", h.senderPort)
	fmt.Fprintf(w, " recipientPort: %d\n", h.recipientPort)
	fmt.Fprintf(w, " senderType   : %d\n", h.senderType)
	fmt.Fprintf(w, " direction    : %d\n", h.direction)
	fmt.Fprintf(w, " site id      : %d\n", h.siteId)
}

func (m *Msg) encodingSize() uint32 {
	szOpMsg := m.opMsg.encodingSize()
	var szMsg uint32
	szMsg = kHeaderLength
	if szOpMsg != 0 {
		szMsg += szOpMsg + 1
	}
	szAppName := uint32(len(m.appName))
	if szAppName != 0 {
		szMsg += szAppName
		szMsg++
	}
	szMsg++
	return szMsg
}

func (m *Msg) EncodeToPPBuffer() (pool util.BufferPool, buf *util.PPBuffer, err error) {
	sz := m.encodingSize()
	pool = util.GetBufferPool(int(sz))
	buf = pool.Get()
	buf.Resize(int(sz))
	raw := buf.Bytes()

	copy(m.header.magic[:], MayflyMagic[:])
	m.header.szMessage = sz
	m.header.szHeader = kHeaderLength
	m.header.szOpMsg = m.opMsg.encodingSize()
	m.header.szAppName = uint32(len(m.appName))

	m.header.encode(raw[:kHeaderLength])

	offset := kHeaderLength

	if m.header.szAppName != 0 {
		raw[offset] = kDataTagAppName
		offset++
		copy(raw[offset:offset+int(m.header.szAppName)], m.appName)
		offset += int(m.header.szAppName)
	}

	if m.header.szOpMsg != 0 {
		raw[offset] = kDataTagOpaqueData
		offset++
		if err = m.opMsg.encode(raw[offset : offset+int(m.header.szOpMsg)]); err != nil {
			return
		}
		offset += int(m.header.szOpMsg)
	}
	raw[offset] = kDataTagEndOfMsg

	return
}

func (m *Msg) Encode() (raw []byte, err error) {
	sz := m.encodingSize()
	raw = make([]byte, sz)

	copy(m.header.magic[:], MayflyMagic[:])
	m.header.szMessage = sz
	m.header.szHeader = kHeaderLength
	m.header.szOpMsg = m.opMsg.encodingSize()
	m.header.szAppName = uint32(len(m.appName))

	m.header.encode(raw[:kHeaderLength])

	offset := kHeaderLength

	if m.header.szAppName != 0 {
		raw[offset] = kDataTagAppName
		offset++
		copy(raw[offset:offset+int(m.header.szAppName)], m.appName)
		offset += int(m.header.szAppName)
	}

	if m.header.szOpMsg != 0 {
		raw[offset] = kDataTagOpaqueData
		offset++
		if err = m.opMsg.encode(raw[offset : offset+int(m.header.szOpMsg)]); err != nil {
			return
		}
		offset += int(m.header.szOpMsg)
	}
	raw[offset] = kDataTagEndOfMsg

	return
}

func (m *Msg) Decode(raw []byte) (err error) {
	h := &m.header
	if err = h.decode(raw); err != nil {
		return
	}
	if len(raw) < int(h.szMessage) {
		err = invalidMessageError(fmt.Sprintf("input buffer size %d < expected size %d", len(raw), h.szMessage))
		return
	}

	var numDataSegments int
	if h.szAppName > 0 {
		numDataSegments++
	}
	if h.szOpMsg > 0 {
		numDataSegments++
	}
	var tag byte
	offset := kHeaderLength
	for i := 0; i < numDataSegments; i++ {
		tag = raw[offset]
		offset++
		switch tag {
		case kDataTagAppName:
			if h.szAppName > 0 {
				m.appName = make([]byte, h.szAppName)
				copy(m.appName, raw[offset:offset+int(h.szAppName)])
				offset += int(h.szAppName)
			} else {
				err = invalidMessageError("application name expected")
				return
			}
		case kDataTagOpaqueData:
			if h.szOpMsg > 0 {
				if err = m.opMsg.Decode(raw[offset:]); err != nil {
					return
				}
				offset += int(h.szOpMsg)
			} else {
				err = invalidMessageError("operational message expected")
				return
			}
		default:
			err = invalidMessageError(fmt.Sprintf("bad data tag %x", tag))
			return
		}
	}
	tag = raw[offset]
	if tag != kDataTagEndOfMsg {
		err = invalidMessageError(fmt.Sprintf("invalid message ending tag %x", tag))
	}
	return
}

func (m *Msg) PrettyPrint(w io.Writer) {
	h := &m.header
	if h.direction == kMessageTypeRequest {
		fmt.Fprintln(w, "Request: {")
	}
	fmt.Fprintln(w, "}")
}

func ReadRawMsg(r io.Reader) (raw []byte, err error) {
	var buf [8]byte
	var n int
	n, err = io.ReadFull(r, buf[:])
	if err != nil {
		return
	}
	if bytes.Compare(MayflyMagic[:4], buf[:4]) != 0 {
		err = invalidMessageError("not mayfly request")
		return
	}
	szMessage := binary.BigEndian.Uint32(buf[4:8])
	raw = make([]byte, szMessage)

	copy(raw[:8], buf[:])
	n, err = io.ReadFull(r, raw[8:])
	if err != nil {
		return
	}
	if n != int(szMessage-8) {
		err = invalidMessageError("wrong message size")
	}
	return
}

func (m *Msg) SetRecipient(ip uint32, port uint16) {
	m.header.recipientIP = ip
	m.header.recipientPort = port
}

func (m *Msg) SetSender(ip uint32, port uint16) {
	m.header.senderIP = ip
	m.header.senderPort = port
}

func (m *Msg) SetOpaque(opaque uint32) {
	m.header.siteId = opaque
}

func (m *Msg) ResetRequestId() {
	m.opMsg.requestId.reset()
}

func (m *Msg) SetAppName(name string) {
	m.appName = []byte(name)
}

func (m *Msg) GetRequestIDString() string {
	return m.opMsg.requestId.String()
}

func (m *Msg) IsSenderNonPersistentClient() bool {
	return (m.header.senderType == kSenderTypeClient)
}

func (m *Msg) InitResponseFromRequest(req *Msg) {
	if req != nil {
		m.header.magic = req.header.magic
		m.header.senderIP = req.header.recipientIP
		m.header.senderPort = req.header.recipientPort
		m.header.recipientIP = req.header.senderIP
		m.header.recipientPort = req.header.senderPort
		m.header.senderType = kSenderTypeDirectoryServer ///...
		m.header.siteId = req.header.siteId
		m.header.direction = kMessageTypeResponse
		m.opMsg.opcode = req.opMsg.opcode
		m.opMsg.requestId = req.opMsg.requestId
		//m.appName = req.appName
	}
}

func (m *Msg) InitNOPRequest() { //not all fields initialized in this function
	m.header.direction = kMessageTypeRequest
	m.header.senderIP = gRequestIdIPUint32
	m.header.senderPort = uint16(gRequestIdPid) //Though it is not a real port, it does not matter
	m.header.senderType = kSenderTypeDirectoryServer
}
