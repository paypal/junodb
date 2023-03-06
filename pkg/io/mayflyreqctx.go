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
  
package io

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
	"juno/pkg/proto/mayfly"
	"juno/pkg/util"
)

type (
	mayflyInboundRequestContext struct {
		InboundRequestContext
		pool util.BufferPool
		buf  *util.PPBuffer
		msg  mayfly.Msg
		conn *Connector
	}

	mayflyInboundResponseContext struct {
		status uint32 ///TODO
		pool   util.BufferPool
		buf    *util.PPBuffer
		//conn is not nil only if the connection needs to be closed in OnComplete()
		conn *Connector
	}
)

func (resp *mayflyInboundResponseContext) GetStatus() uint32 {
	return resp.status
}

func (resp *mayflyInboundResponseContext) GetMessage() *proto.RawMessage {
	return nil
}

func (resp *mayflyInboundResponseContext) GetMsgSize() uint32 {
	if resp.buf == nil {
		return 0
	}
	return uint32(resp.buf.Len())
}

func (resp *mayflyInboundResponseContext) OnComplete() {
	if resp.conn != nil {
		resp.conn.Stop()
		resp.conn = nil
	}
	if resp.buf != nil {
		if resp.pool != nil {
			resp.pool.Put(resp.buf)
		}
		resp.buf = nil
	}
}

func (resp *mayflyInboundResponseContext) Read(r io.Reader) (n int, err error) {
	return
}

func (resp *mayflyInboundResponseContext) Write(w io.Writer) (n int, err error) {
	if resp.buf == nil {
		n = 0
		return
	}
	return w.Write(resp.buf.Bytes())
}

func (req *mayflyInboundRequestContext) OnComplete() {
	req.InboundRequestContext.OnComplete()
	if req.buf != nil {
		if req.pool != nil {
			req.pool.Put(req.buf)
			req.pool = nil
		}
		req.buf = nil
	}
}

func (req *mayflyInboundRequestContext) readRawMsg(r io.Reader) (err error) {
	var buf [8]byte
	var n int
	n, err = io.ReadFull(r, buf[:])
	if err != nil {
		return
	}
	if bytes.Compare(mayfly.MayflyMagic[:4], buf[:4]) != 0 {
		err = errors.New("not mayfly request")
		return
	}
	szMessage := binary.BigEndian.Uint32(buf[4:8])
	req.pool = util.GetBufferPool(int(szMessage))
	req.buf = req.pool.Get()
	req.buf.Resize(int(szMessage))
	raw := req.buf.Bytes()

	copy(raw[:8], buf[:])
	n, err = io.ReadFull(r, raw[8:])
	if err != nil {
		return
	}
	if n != int(szMessage-8) {
		err = errors.New("wrong message size")
	}
	return
}

func (req *mayflyInboundRequestContext) Read(r io.Reader) (n int, err error) {
	err = req.readRawMsg(r)
	if err != nil {
		return
	}
	if err = req.msg.Decode(req.buf.Bytes()); err != nil {
		glog.Errorf("fail to decode mayfly request. %s", err.Error())
		return
	}
	var jmsg proto.OperationalMessage
	mayfly.ToJunoMsg(&jmsg, &req.msg)
	if err = jmsg.Encode(&req.message); err != nil {
		glog.Errorf("fail to map mayfly request to juno. %s", err.Error())
		return
	}
	n = req.buf.Len()
	req.timeReceived = time.Now()
	if cal.IsEnabled() {
		if jmsg.GetOpCode() != proto.OpCodeNop {
			b := logging.NewKVBuffer()
			b.Add([]byte("juno_rid"), jmsg.GetRequestIDString())
			b.Add([]byte("mayfly_rid"), req.msg.GetRequestIDString())
			if jmsg.IsForReplication() {
				cal.Event(logging.CalMsgTypeRidMapping, logging.CalMsgNameInboundReplication, cal.StatusSuccess, b.Bytes())
			} else {
				cal.Event(logging.CalMsgTypeRidMapping, logging.CalMsgNameInbound, cal.StatusSuccess, b.Bytes())
			}
		}
	}
	return
}

func (req *mayflyInboundRequestContext) Reply(resp IResponseContext) {
	var opMsg proto.OperationalMessage
	opMsg.Decode(resp.GetMessage())
	var mayflyMsg mayfly.Msg
	mayflyMsg.InitResponseFromRequest(&req.msg)
	mayfly.ToMayflyMsg(&mayflyMsg, &opMsg)
	pool, buf, err := mayflyMsg.EncodeToPPBuffer()
	if err != nil {
		panic("aa") ///TODO
	}
	resp.OnComplete()
	ctx := &mayflyInboundResponseContext{
		status: uint32(opMsg.GetOpStatus()), ///TODO
		pool:   pool,
		buf:    buf,
	}
	if req.msg.IsSenderNonPersistentClient() {
		ctx.conn = req.conn
	}
	req.chResponse <- ctx
}

func newMayflyInboundRequestContext(c *Connector) (r *mayflyInboundRequestContext) {
	r = &mayflyInboundRequestContext{
		InboundRequestContext: InboundRequestContext{
			RequestContext: RequestContext{
				chResponse: c.chResponse,
			},
			lsnrType: c.lsnrType,
		},
		conn: c,
	}
	return
}
