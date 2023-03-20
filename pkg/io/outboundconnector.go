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
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/io/ioutil"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
	"juno/pkg/proto/mayfly"
	"juno/pkg/util"
)

const (
	kMinSizeWriteBuffer = 64 * 1024
)

////////////////////////////////////
// Start of OutboundConnector
////////////////////////////////////

type StateType int32

const (
	WAITING    = StateType(0)
	CONNECTING = StateType(1)
	SERVING    = StateType(2)
	DRAINING   = StateType(3)
)

type (
	IHandshaker interface {
		GetName() string
		GetHandshakeTimeout() util.Duration
		GetPingRequest() IRequestContext
		ExpectResponse() bool
		OnPingResponse(ctx IResponseContext) bool
		GetPingIP() string
	}

	OutboundConnector struct {
		id     int
		conn   net.Conn // tcp connection to downstream server
		reader *bufio.Reader

		reqCh      chan IRequestContext
		reqPending *util.RingBuffer // lock-free ring buffer

		doneCh      chan struct{}
		monitorCh   chan int
		wg          sync.WaitGroup
		closeOnce   sync.Once
		config      *OutboundConfig
		state       int32
		hshaker     IHandshaker
		displayName string
	}
)

// constructor/factory
// each OutboundConnector object will have two go routines
// one for Read; one for Write
func NewOutboundConnector(id int, c net.Conn, reqCh chan IRequestContext, monCh chan int,
	config *OutboundConfig) (p *OutboundConnector) {

	if config.IOBufSize == 0 {
		config.IOBufSize = 64000
	}
	p = &OutboundConnector{
		id:     id,
		conn:   c,
		reader: util.NewBufioReader(c, config.IOBufSize),
		reqCh:  reqCh,
		reqPending: util.NewRingBufferWithExtra(uint32(config.MaxPendingQueSize-1),
			uint32(config.PendingQueExtra)),
		doneCh:    make(chan struct{}),
		monitorCh: monCh,
		config:    config,
	}

	// set bigger bufio size
	p.displayName = fmt.Sprintf("id=%d, laddr=%s raddr=%s",
		p.id, p.conn.LocalAddr(), p.conn.RemoteAddr())

	return p
}

func (p *OutboundConnector) Start() {

	p.wg.Add(2)

	// start go routine to read requests from request channel, and send data to SS
	go p.writeLoop()

	// start go routine to read response from SS, and send data back to result channel
	go p.readLoop()

}

func (p *OutboundConnector) Close() {
	p.closeOnce.Do(func() {
		glog.Debugf("connector %s closed", p.displayName)
		close(p.doneCh)
		//p.conn.Close()
		p.monitorCh <- p.id
	})
}

func (p *OutboundConnector) SetHandshaker(h IHandshaker) {
	p.hshaker = h
	if h != nil {
		p.displayName = fmt.Sprintf("id=%d, t=%s, laddr=%s raddr=%s",
			p.id, h.GetName(), p.conn.LocalAddr(), p.conn.RemoteAddr())
	}
}

// wait for all go routine to finish
func (p *OutboundConnector) Shutdown() {
	p.Close()
	p.wg.Wait()
	p.cleanPending()
}

func (p *OutboundConnector) SetState(s StateType) {
	atomic.StoreInt32(&p.state, int32(s))
}

func (p *OutboundConnector) AllowRestart() bool {
	s := atomic.LoadInt32(&p.state)
	return (s == int32(WAITING))
}

func (p *OutboundConnector) IsActive() bool {
	s := atomic.LoadInt32(&p.state)
	return (s == int32(SERVING))
}

// run one time
func (p *OutboundConnector) Handshake() bool {
	if p.hshaker != nil {
		// send request.
		var buf bytes.Buffer
		p.hshaker.GetPingRequest().WriteWithOpaque(0, &buf)
		if _, err := buf.WriteTo(p.conn); err != nil {
			glog.Error(err)
			return false
		}

		if !p.hshaker.ExpectResponse() {
			return true
		}

		p.conn.SetReadDeadline(time.Now().Add(p.hshaker.GetHandshakeTimeout().Duration))
		resp, err := p.newResponseContext()

		if err != nil {
			glog.Errorf("connector %s, handshake failed: err=%v", p.displayName, err)
			return false
		}

		if !p.hshaker.OnPingResponse(resp) {
			glog.Errorf("connector %s, handshake failed", p.displayName)
			return false
		}
		glog.Debugf("connector %s, handshake succeed", p.displayName)
	}
	return true
}

// go routines
// WriteLoop go routine in charge of
// 1. connectivity setup/tear down to the downstream
// 2. sends client requests to the downstream
// 3. error handling when losing connections to the downstream
func (p *OutboundConnector) writeLoop() {
	defer func() {
		p.Close()
		p.wg.Done()
		glog.Debugf("connector %s write loop exit", p.displayName)
	}()

	var chHavingDataToWrite chan bool
	var chClosedForNotifyingFlush chan bool = make(chan bool)
	close(chClosedForNotifyingFlush)

	var buf bytes.Buffer
	szBuf := p.config.IOBufSize
	if szBuf < kMinSizeWriteBuffer {
		szBuf = kMinSizeWriteBuffer
	}
	buf.Grow(szBuf)
	maxBufSizeAllocated := szBuf

	funBufferForWrite := func(req IRequestContext) (n int, err error) {
		glog.Verbosef("bufferForWrite")
		if req.GetCtx() != nil {
			select {
			case <-req.GetCtx().Done():
				glog.Verbosef("bufferForWrite 1")
				return
			default:
			}
		}

		req.SetInUse(true)
		var reqId uint32
		reqId, err = p.reqPending.EnQueue(req)
		if err != nil {
			ReplyError(req, proto.StatusNoCapacity)
			glog.Verbosef("bufferForWrite 2")
			return 0, nil
		}

		n, err = req.WriteWithOpaque(reqId, &buf)
		req.SetInUse(false)

		if buf.Cap() > maxBufSizeAllocated {
			glog.Debugf("buf Cap : %d  len: %d", buf.Cap(), buf.Len())
			maxBufSizeAllocated = buf.Cap()
		}
		return
	}

	// TODO: remove later
	max_buf_size := p.config.MaxBufferedWriteSize

	var reqCh chan IRequestContext
	var chTimeout <-chan time.Time = nil
	queCheckTimer := util.NewTimerWrapper(1 * time.Millisecond)

	for {
		if p.reqPending.IsFull() {
			reqCh = nil
			queCheckTimer.Reset(1 * time.Millisecond)
			chTimeout = queCheckTimer.GetTimeoutCh()
		} else {
			reqCh = p.reqCh
			chTimeout = nil
		}

		select {
		case <-p.doneCh:
			return

		case req, ok := <-reqCh:
			if !ok {
				glog.Debugf("reqCh closed")
				return
			}

			n := 0
			nreqs := 0
			if req != nil {
				if k, err := funBufferForWrite(req); err != nil {
					cal.Event("OutBoundErr", "WriteErr1", cal.StatusSuccess, nil)
					glog.Error(err)
					return
				} else {
					n += k
					nreqs++
				}
			}

			// keep reading till max buf size reached or no more requests
		LOOP:
			for n < max_buf_size {
				select {
				case req, ok := <-p.reqCh:
					if !ok {
						glog.Debugf("chReq closed")
						return
					}

					if req != nil {
						if k, err := funBufferForWrite(req); err != nil {
							cal.Event("OutBoundErr", "WriteErr1", cal.StatusSuccess, nil)
							glog.Error(err)
							return
						} else {
							n += k
							nreqs++
						}
					}

				default:
					break LOOP
				}
			}

			if buf.Len() >= kMinSizeWriteBuffer {
				//glog.Infof("%s sending request1", p.displayName)
				if _, err := buf.WriteTo(p.conn); err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
						continue
					} else {
						cal.Event("OutBoundErr", "WriteErr2", cal.StatusSuccess, nil)
						glog.Error(err)
						return
					}
				}
			}
			//glog.Infof("here szBuf: %d", buf.Len())
			if buf.Len() != 0 {
				chHavingDataToWrite = chClosedForNotifyingFlush
			} else {
				chHavingDataToWrite = nil
			}
		case <-chHavingDataToWrite:
			//glog.Infof("%s sending request2", p.displayName)

			if _, err := buf.WriteTo(p.conn); err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
					glog.Verbosef("nerr: %s", netErr)
					continue
				} else {
					cal.Event("OutBoundErr", "WriteErr2", cal.StatusSuccess, nil)
					glog.Error(err)
					return
				}
			}
			if buf.Len() == 0 {
				chHavingDataToWrite = nil
			}
		case <-chTimeout:
			continue
		}
	}
}

// ReadLoop go routine in charge of
// 1. receive the response
// 2. sends the response back to the result channel
// 3. periodically clean up the ringbuffer's expired entries

func (p *OutboundConnector) readLoop() {
	ticker := time.NewTicker(1 * time.Second)
	doneTimer := util.NewTimerWrapper(2 * p.config.GracefulShutdownTime.Duration)
	var chTimeout <-chan time.Time = nil
	var shutdown bool = false

	defer func() {
		ticker.Stop()
		p.Close()
		p.conn.Close()
		util.PutBufioReader(p.reader)
		doneTimer.Stop()
		p.wg.Done()
		glog.Debugf("connector %s read loop exit", p.displayName)
	}()

	doneCh := p.doneCh
	for {
		select {
		case <-doneCh:
			if p.reqPending.IsEmpty() {
				return
			}
			doneCh = nil

			// graceful shutdown, make sure no pending request
			// to be safe, graceful shutdown timer is set to 2 times request timeout
			doneTimer.Reset(2 * p.config.GracefulShutdownTime.Duration)
			chTimeout = doneTimer.GetTimeoutCh()
			shutdown = true

		case <-ticker.C:
			p.reqPending.CleanUp()

		case <-chTimeout:
			glog.Debugf("connector %s reader graceful shutdown timeout", p.displayName)
			return

		default:
			if shutdown && p.reqPending.IsEmpty() {
				glog.Debugf("connector %s reader graceful shutdown", p.displayName)
				return
			}

			p.conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			if _, err := p.reader.Peek(4); err == nil {
				// the response has arrived, go ahead with ressponse
				p.conn.SetReadDeadline(time.Now().Add(1000 * time.Millisecond))
				if resp, err := p.newResponseContext(); err == nil {
					//glog.Debugf("%s receiving response", p.displayName)
					p.sendResponse(resp)
				} else {
					glog.Debugln("Outbound::readLoop: ", err)
					return
				}
			} else {
				if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
					continue
				} else {
					ioutil.LogError(err)
					return
				}
			}
		}
	}
}

func (p *OutboundConnector) newResponseContext() (ctx IResponseContext, err error) {
	var b [40]byte
	if _, err = io.ReadFull(p.reader, b[:12]); err != nil {
		return
	}

	if bytes.Compare(b[:2], proto.JunoMagic[:]) == 0 {
		resp := NewOutboundResponse()
		rawMsg := resp.GetMessage()
		_, err = rawMsg.ReadWithHeader(b[:12], p.reader)
		if err == nil {
			ctx = resp
		}
	} else if bytes.Compare(b[:4], mayfly.MayflyMagic[:]) == 0 {
		// only needed for legacy enviroment. cleanup when migrations are done.
		szMessage := binary.BigEndian.Uint32(b[4:8])
		raw := make([]byte, szMessage)
		copy(raw[:12], b[:12])
		if _, err = io.ReadFull(p.reader, raw[12:]); err != nil {
			return
		}
		var mayflyMsg mayfly.Msg
		if err = mayflyMsg.Decode(raw); err != nil {
			return
		}
		var junoMsg proto.OperationalMessage
		if err = mayfly.ToJunoMsg(&junoMsg, &mayflyMsg); err != nil {
			return
		}
		resp := NewOutboundResponse()
		rawMsg := resp.GetMessage()
		if err = junoMsg.Encode(rawMsg); err != nil {
			return
		}
		ctx = resp
	} else {
		err = fmt.Errorf("protocol not supported. magic: %v", b[:4])
		return
	}
	return
}

func (p *OutboundConnector) sendResponse(resp IResponseContext) {

	opaque := resp.GetMessage().GetOpaque()
	item, err := p.reqPending.Remove(opaque)

	if glog.LOG_VERBOSE {
		opcode, statuscode, _ := proto.GetOpCodeAndOpStatus(resp.GetMessage())
		glog.Verbosef("receive response: remote=%s opaque=%d reqid=%d op=%s status=%s size=%d",
			p.conn.RemoteAddr().String(),
			resp.GetMessage().GetOpaque(), opaque,
			opcode.String(), statuscode.String(), resp.GetMessage().GetMsgSize())
	}

	if err != nil || item == nil {
		opcode, statuscode, _ := proto.GetOpCodeAndOpStatus(resp.GetMessage())
		glog.Debugf("missing request for the response, remote=%s reqId=%d op=%s status=%s size=%d",
			p.conn.RemoteAddr().String(),
			opaque, opcode.String(), statuscode.String(), resp.GetMessage().GetMsgSize())
		return
	}

	req, ok := item.(IRequestContext)
	if !ok {
		return
	}

	req.Reply(resp)
}

func (p *OutboundConnector) cleanPending() {
	// drain the ring buffer
	p.reqPending.CleanAll()
}

func (p *OutboundConnector) GetId() int {
	return p.id
}

func (p *OutboundConnector) WriteStats(w io.Writer, indent int) {
	indentStr := strings.Repeat(" ", indent)
	fmt.Fprintf(w, "%sring buffer {", indentStr)
	p.reqPending.WriteStats(w)
	w.Write([]byte("}\n"))
}

func (p *OutboundConnector) Recycle() {
	go p.Shutdown()
}

func (p *OutboundConnector) SetNewConn(c net.Conn) {
	p.conn.Close()
	util.PutBufioReader(p.reader)

	p.conn = c
	p.reader = util.NewBufioReader(c, p.config.IOBufSize)
}

func (p *OutboundConnector) GetPingIP() string {
	if p.hshaker != nil {
		return p.hshaker.GetPingIP()
	} else {
		return ""
	}
}

func ReplyError(req IRequestContext, status uint32) {

	if glog.LOG_VERBOSE {
		glog.Verbosef("replyError: status=%d", status)
	}
	resp := NewErrorOutboundResponse(status)
	req.Reply(resp)

}

////////////////////////////////////
// End of OutboundConnector
////////////////////////////////////
