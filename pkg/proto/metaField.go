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

/*
	  ==============================
	  *** Predefined Field Types ***
	  ==============================

	  Tag/ID |  Field     | SizeType
	  -------+--------------------------------------+------
	    0x01 | TimeToLive                           | 0x01
	    0x02 | Version                              | 0x01
	    0x03 | Creation Time                        | 0x01
	    0x04 | Expiration Time                      | 0x01
	    0x05 | RequestID/UUID                       | 0x03
	    0x06 | Source Info                          | 0
	    0x07 | Last Modification time (nano second) | 0x02
	    0x08 | Originator RequestID/UUID            | 0x03
	    0x09 | Correlation ID                       | 0
	    0x0a | RequestHandlingTime                  | 0x01
		0x0b | UDF Name			                    | 0
	  -------+--------------------------------------+------


	  Tag/ID: 0x06
	  +-----------------------------------------------------------------------------------------------+
	  | 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7| 0| 1| 2| 3| 4| 5| 6| 7|
	  |                      0|                      1|                      2|                      3|
	  +-----------+-----------+--------------------+--+-----------------------+-----------------------+
	  | size (include padding)| app name length    | T| Port                                          |
	  +-----------------------+--------------------+--+-----------------------------------------------+
	  | IPv4 address if T is 0 or IPv6 address if T is 1                                              |
	  +-----------------------------------------------------------------------------------------------+
	  | application name, padding to 4-byte aligned                                                   |
	  +-----------------------------------------------------------------------------------------------+

	  Tag/ID: 0x09; 0x0b
	  +----+-------------------------------------------
	  |  0 | field size (including padding)
	  +----+-------------------------------------------
	  |  1 | octet sequence length
	  +----+-------------------------------------------
	  |    | octet sequence, padding to 4-byte aligned
	  +----+-------------------------------------------
*/
package proto

import (
	"net"

	"github.com/paypal/junodb/third_party/forked/golang/glog"
)

// Meta Component Field Tag
const (
	_ = iota
	kFieldTagTimeToLive
	kFieldTagVersion
	kFieldTagCreationTime
	kFieldTagExpirationTime
	kFieldTagRequestID
	kFieldTagSourceInfo
	kFieldTagLastModificationTime
	kFieldTagOriginatorRequestID
	kFieldTagCorrelationID
	kFieldTagRequestHandlingTime
	kFieldTagUDFName
	kNumSupportedFields
)

// Meta Component File Size Type
const (
	kMetaFieldVariableSize = iota
	kMetaField_4Bytes      = iota << 5
	kMetaField_8Bytes
	kMetaField_16Bytes
)

type (
	uint32T uint32
	uint64T uint64

	requestIdBaseT struct {
		RequestId
		idSet bool
	}
	byteSequenceT []byte

	versionT              struct{ uint32T }
	timeToLiveT           struct{ uint32T }
	creationTimeT         struct{ uint32T }
	expirationTimeT       struct{ uint32T }
	requestHandlingTimeT  struct{ uint32T }
	lastModificationTimeT struct{ uint64T }
	requestIdT            struct{ requestIdBaseT }
	originatorT           struct{ requestIdBaseT }

	sourceInfoT struct {
		ip      net.IP
		port    uint16
		appName []byte
	}

	correlationIdT struct{ byteSequenceT }
	udfNameT       struct{ byteSequenceT }
)

func (t uint32T) isSet() bool {
	return t != 0
}

func (t uint32T) size() int {
	return 4
}

func (t uint32T) value() uint32 {
	return uint32(t)
}

func (t *uint32T) set(v uint32) {
	*t = uint32T(v)
}

func (t uint32T) encode(buf []byte) (int, error) {
	if len(buf) < 4 {
		return 0, ErrInvalidBufferSize
	}
	EncByteOrder.PutUint32(buf, uint32(t))
	return 4, nil
}

func (t *uint32T) decode(buf []byte) error {
	if len(buf) < 4 {
		return ErrInvalidBufferSize
	}
	*t = uint32T(EncByteOrder.Uint32(buf))
	return nil
}

func (t versionT) tagAndSizeTypeByte() uint8 {
	return kFieldTagVersion | kMetaField_4Bytes
}

func (t timeToLiveT) tagAndSizeTypeByte() uint8 {
	return kFieldTagTimeToLive | kMetaField_4Bytes
}

func (t creationTimeT) tagAndSizeTypeByte() uint8 {
	return kFieldTagCreationTime | kMetaField_4Bytes
}

func (t expirationTimeT) tagAndSizeTypeByte() uint8 {
	return kFieldTagExpirationTime | kMetaField_4Bytes
}

func (t requestHandlingTimeT) tagAndSizeTypeByte() uint8 {
	return kFieldTagRequestHandlingTime | kMetaField_4Bytes
}

// uint64 meta field
func (t uint64T) isSet() bool {
	return t != 0
}

func (t uint64T) size() int {
	return 8
}

func (t uint64T) value() uint64 {
	return uint64(t)
}

func (t *uint64T) set(v uint64) {
	*t = uint64T(v)
}

func (t uint64T) encode(buf []byte) (n int, err error) {
	if len(buf) < 8 {
		err = ErrInvalidBufferSize
		return
	}
	EncByteOrder.PutUint64(buf, uint64(t))
	n = 8
	return
}

func (t *uint64T) decode(buf []byte) error {
	if len(buf) < 8 {
		return ErrInvalidBufferSize
	}
	*t = uint64T(EncByteOrder.Uint64(buf))
	return nil
}

func (t lastModificationTimeT) tagAndSizeTypeByte() uint8 {
	return kFieldTagLastModificationTime | kMetaField_8Bytes
}

// 16-byte meta field
func (t *requestIdBaseT) value() []byte {
	return t.Bytes()
}

func (t *requestIdBaseT) setWithID(id RequestId) {
	t.idSet = true
	t.RequestId = id
}

func (t *requestIdBaseT) set(v []byte) {
	t.SetFromBytes(v)
	t.idSet = true
}

func (t *requestIdBaseT) size() int {
	return 16
}

func (t *requestIdBaseT) encode(buf []byte) (int, error) {
	if !t.idSet {
		return 0, ErrInvalidRequestID
	}
	if len(buf) < 16 {
		return 0, ErrInvalidBufferSize
	}
	copy(buf, t.Bytes())
	return 16, nil
}

func (t *requestIdBaseT) isSet() bool {
	return t.idSet
}

func (t *requestIdBaseT) decode(sz uint8, buf []byte, copyData bool) error {
	if int(sz) != 16 || int(sz) > len(buf) {
		return ErrInvalidMetaFieldSize
	}
	t.SetFromBytes(buf)
	t.idSet = true
	//	if t.RequestId.Equal(NilRequestId) {
	//		panic("")
	//	}
	//	if t.RequestId.IsSet() == false {
	//		panic("")
	//
	//	}
	return nil
}

func (t *requestIdT) tagAndSizeTypeByte() uint8 {
	return kFieldTagRequestID | kMetaField_16Bytes
}

func (t *originatorT) tagAndSizeTypeByte() uint8 {
	return kFieldTagOriginatorRequestID | kMetaField_16Bytes
}

// sourceinfo
func (t *sourceInfoT) isSet() bool {
	if (len(t.ip) != 0) || (t.port != 0) || (len(t.appName) != 0) {
		return true
	}
	return false
}

func (t *sourceInfoT) encode(buf []byte) (n int, err error) {
	sz := t.size()

	if len(buf) < sz {
		err = ErrInvalidBufferSize
		glog.Error(err)
		return
	}
	buf[0] = uint8(sz)
	szAppName := len(t.appName)
	var lenAndTag uint8 = uint8(len(t.appName))
	szIP := len(t.ip)
	if szIP == 16 {
		lenAndTag |= 0x80
	} else if szIP != 4 {
		err = ErrInvalidIPAddress
		glog.Error(err)
		return
	}
	buf[1] = lenAndTag
	EncByteOrder.PutUint16(buf[2:4], t.port)

	copy(buf[4:szIP+4], t.ip)

	if szAppName != 0 {
		copy(buf[4+szIP:4+szIP+szAppName], t.appName)
	}
	offset := 4 + szIP + szAppName
	for ; offset < sz; offset++ {
		buf[offset] = 0
	}
	n = offset
	return
}

func (t *sourceInfoT) size() int {
	sz := (4 + len(t.ip) + len(t.appName))
	szPadding := (4 - sz%4) % 4
	return sz + szPadding
}

func (t *sourceInfoT) decode(sz uint8, buf []byte, copyData bool) error {
	if int(sz) != len(buf) {
		return ErrInvalidMetaFieldSize
	}
	offset := 0

	size := int(buf[0])
	if size == 0 || size != int(sz) { //not necessary. to change
		return ErrInvalidSourceInfo
	}

	var varUint8 uint8 = buf[1]

	szAppName := varUint8 & 0x7F

	t.port = EncByteOrder.Uint16(buf[2:4])
	offset += 4

	if (varUint8 & 0x80) != 0 {
		if copyData {
			t.ip = make([]byte, 16, 16)
			copy(t.ip, buf[offset:offset+16])
		} else {
			t.ip = buf[offset : offset+16]
		}
		offset += 16
	} else {
		if copyData {
			t.ip = make([]byte, 4, 4)
			copy(t.ip, buf[offset:offset+4])
		} else {
			t.ip = buf[offset : offset+4]
		}
		offset += 4
	}

	if szAppName != 0 {
		if copyData {
			t.appName = make([]byte, szAppName, szAppName)
			copy(t.appName, buf[offset:offset+int(szAppName)])
		} else {
			t.appName = buf[offset : offset+int(szAppName)]
		}
		offset += int(szAppName)
	}
	szActual := 4 + len(t.ip) + len(t.appName)
	szPadding := (4 - szActual%4) % 4
	if offset != szActual {
		return ErrInvalidMetaFieldSize
	}
	if szActual+szPadding != size {
		return ErrInvalidMetaFieldSize
	}
	return nil
}

func (t sourceInfoT) tagAndSizeTypeByte() uint8 {
	return kFieldTagSourceInfo | kMetaFieldVariableSize
}

// byte sequence
func (t byteSequenceT) isSet() bool {
	return len(t) != 0
}

func (t byteSequenceT) size() int {
	sz := 2 + len(t)
	szPadding := (4 - sz%4) % 4
	sz += szPadding
	return sz
}

func (t byteSequenceT) value() []byte {
	return []byte(t)
}

func (t *byteSequenceT) set(v []byte) {
	*t = byteSequenceT(v)
}

func (t byteSequenceT) encode(buf []byte) (int, error) {
	sz := t.size()

	if len(buf) < sz {
		return 0, ErrInvalidBufferSize
	}
	buf[0] = byte(sz)

	lenSeq := len(t)
	buf[1] = byte(lenSeq)

	if lenSeq > 0 {
		copy(buf[2:2+lenSeq], t)
	}

	for i := 2 + lenSeq; i < sz; i++ {
		buf[i] = 0
	}

	return sz, nil
}

func (t *byteSequenceT) decode(sz uint8, buf []byte, copyData bool) error {
	if int(sz) != len(buf) {
		return ErrInvalidMetaFieldSize
	}
	offset := 0

	size := int(buf[0])
	if size == 0 || size != int(sz) { //not necessary. to change
		return ErrInvalidByteSequence
	}

	lenSeq := int(buf[1])

	offset += 2
	if lenSeq != 0 {
		if copyData {
			(*t) = make([]byte, lenSeq, lenSeq)
			copy((*t), buf[offset:offset+lenSeq])
		} else {
			(*t) = buf[offset : offset+lenSeq]
		}
	}
	offset += lenSeq
	szActual := 2 + len(*t)
	szPadding := (4 - szActual%4) % 4
	if offset != szActual {
		return ErrInvalidMetaFieldSize
	}
	if szActual+szPadding != size {
		return ErrInvalidMetaFieldSize
	}
	return nil
}

func (t correlationIdT) tagAndSizeTypeByte() uint8 {
	return kFieldTagCorrelationID | kMetaFieldVariableSize
}

func (t udfNameT) tagAndSizeTypeByte() uint8 {
	return kFieldTagUDFName | kMetaFieldVariableSize
}
