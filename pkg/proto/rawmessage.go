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
	"bytes"
	"io"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/debug"
	"juno/pkg/util"
)

type RawMessage struct {
	messageHeaderT
	body []byte
	buf  *util.PPBuffer
	pool util.BufferPool
}

func (m *RawMessage) Reset() {
	m.messageHeaderT.reset()
	m.body = nil
	m.buf = nil
	m.pool = nil
}

func (m *RawMessage) ShallowCopy(from *RawMessage) {
	m.messageHeaderT = from.messageHeaderT
	m.body = from.body
	m.buf = nil
	m.pool = nil
}

func (m *RawMessage) DeepCopy(from *RawMessage) {
	m.messageHeaderT = from.messageHeaderT
	if m.pool != nil {
		m.buf.Reset()
		m.pool.Put(m.buf)
		m.buf = nil
	}
	sz := len(from.body)
	m.allocateBuffer(sz)
	copy(m.body, from.body)
}

func (m *RawMessage) GetBody() []byte {
	return m.body
}

func (m *RawMessage) HexDump() {
	var buf bytes.Buffer
	m.Write(&buf)
	util.HexDump(buf.Bytes())
}

func (m *RawMessage) ToHexString() string {
	var buf bytes.Buffer
	m.Write(&buf)
	return util.ToHexString(buf.Bytes())
}

func (m *RawMessage) PrintBytesForTest() {
	var buf bytes.Buffer
	m.Write(&buf)
	util.PrintBytesForTest(buf.Bytes())
}

func (m *RawMessage) allocateBuffer(size int) {
	if m.buf == nil {
		if m.pool == nil {
			m.pool = util.GetBufferPool(size)
		}
		m.buf = m.pool.Get()
	}
	m.buf.Resize(size)
	m.body = m.buf.Bytes()
}

//
// Note: read timeout is set at conn level
//
func (msg *RawMessage) Read(r io.Reader) (n int, err error) {

	var hBuffer [kMessageHeaderSize]byte
	header := hBuffer[:]

	n, err = io.ReadFull(r, header)
	if err != nil {
		return
	}
	if n == 0 { //might not need
		err = io.EOF
		msg.Reset()
		return
	}

	var nbody int
	if nbody, err = msg.ReadWithHeader(header, r); err == nil {
		n += nbody
	}
	return
}

func (msg *RawMessage) ReadWithHeader(header []byte, r io.Reader) (n int, err error) {
	if err = msg.messageHeaderT.Decode(header); err != nil {
		msg.Reset()
		return
	}

	szBufNeeded := msg.msgSize - kMessageHeaderSize

	bufferPool := util.GetBufferPool(int(szBufNeeded))
	buffer := bufferPool.Get()
	buffer.Resize(int(szBufNeeded))

	n, err = io.ReadFull(r, buffer.Bytes())
	if err != nil {
		msg.Reset()
		bufferPool.Put(buffer)
		return
	}

	msg.buf = buffer
	msg.pool = bufferPool
	msg.body = buffer.Bytes()

	if debug.DEBUG {
		if int(msg.msgSize) != kMessageHeaderSize+len(msg.body) {
			glog.Errorf("%v", msg)
			panic("mem corrupt???")
		}
	}
	return
}

//
// Note: this api is not thread safe
//
func (m *RawMessage) Write(w io.Writer) (n int, err error) {
	var mheader [kMessageHeaderSize]byte
	raw := mheader[:]
	m.messageHeaderT.encode(raw)
	n, err = w.Write(raw)
	if n != kMessageHeaderSize || err != nil {
		return
	}

	var k int
	k, err = w.Write(m.body)
	n += k

	if debug.DEBUG {
		if int(m.msgSize) != n {
			glog.Errorf("%v", m)
			panic("mem corrupt???")
		}
	}
	return
}

func (m *RawMessage) ReleaseBuffer() {
	if debug.DEBUG {
		if m.body != nil {
			if int(m.msgSize) != kMessageHeaderSize+len(m.body) {
				glog.Errorf("%v", m)
				panic("mem corrupt???")
			}
		} else {
			if m.msgSize != 0 {
				glog.Errorf("%v", m)
				panic("mem corrupt?  msgSize should be 0")
			}
		}
	}
	m.messageHeaderT.reset()
	m.body = nil
	if m.buf != nil {
		if m.pool != nil {
			m.buf.Reset()
			m.pool.Put(m.buf)
			m.buf = nil
		}
		m.pool = nil
	}
}

func (m *RawMessage) GiveUpBufferOwnership() {
	m.buf = nil
	m.pool = nil
}
