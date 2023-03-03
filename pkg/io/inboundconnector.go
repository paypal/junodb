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
package io

import (
	"bufio"
	"bytes"
	"context"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/debug"
	"juno/pkg/io/ioutil"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
	"juno/pkg/util"
)

type Connector struct {
	conn          net.Conn
	reader        *bufio.Reader
	ctx           context.Context
	cancelCtx     context.CancelFunc
	chResponse    chan IResponseContext
	chStop        chan struct{}
	stopOnce      sync.Once
	closeOnce     sync.Once
	config        InboundConfig
	pendingReq    int32               // local automic counter, for graceful shutdown
	reqCounter    *util.AtomicCounter // global counter
	reqHandler    IRequestHandler
	connMgr       *InboundConnManager
	reqCtxCreator InboundRequestContextCreator
	lsnrType      ListenerType
}

func (c *Connector) Start() {
	glog.Verbosef("start connector...")
	c.connMgr.TrackConn(c, true)
	go c.doRead()
	go c.doWrite()
}

func (c *Connector) Stop() {
	c.stopOnce.Do(func() {
		close(c.chStop)
	})
}

func (c *Connector) Close() {
	c.closeOnce.Do(func() {
		raddr := c.conn.RemoteAddr().String()
		addr := "raddr=" + raddr + "&laddr=" + c.conn.LocalAddr().String()
		glog.Debugf("close: %s", addr)

		c.Stop()
		c.conn.Close()
		c.connMgr.TrackConn(c, false)
		if cal.IsEnabled() {
			if rhost, _, e := net.SplitHostPort(raddr); e == nil {
				cal.Event(cal.TxnTypeClose, rhost, cal.StatusSuccess, []byte(addr))
			}
		}
		c.cancelCtx()
	})
}

func (c *Connector) doRead() {
	glog.Verbosef("start reader")
	idleTimer := util.NewTimerWrapper(c.config.IdleTimeout.Duration)

	defer func() {
		// note, reader does not close the tcp connection; writer will
		util.PutBufioReader(c.reader)
		idleTimer.Stop()
		glog.Verboseln("reader exit")
		c.Stop()
	}()

	var magic []byte
	var err error
	for {
		select {
		case <-c.chStop:
			// server shutdown or terminate if there's other fatal error
			glog.Verbosef("chStop")
			return

		default:
			idleTimer.Reset(c.config.IdleTimeout.Duration)
			glog.Verbosef("idleTimeout:%s", c.config.IdleTimeout.Duration.String())

		Loop: // waiting for one request
			for {
				select {
				case <-idleTimer.GetTimeoutCh():
					glog.Debugf("idle timeout")
					return

				case <-c.chStop:
					// server shutdown or terminate if there's other fatal error
					glog.Verbosef("chStop")
					return

				default:
					c.conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
					if magic, err = c.reader.Peek(4); err == nil {
						// the request has arrived, go ahead with request
						break Loop
					} else {
						if nerr, ok := err.(net.Error); ok && nerr.Timeout() {
							//						glog.Verbosef("nerr: %s", nerr)
							continue
						} else {
							ioutil.LogError(err)
							return
						}
					}
				}
				glog.Verbosef("Exiting loop")
			}
			glog.Verbosef("got magic")

			// read and process one request
			c.conn.SetReadDeadline(time.Now().Add(c.config.ReadTimeout.Duration))
			if c.reqCtxCreator == nil {
				glog.Fatal("nil request context creator")
				return
			}
			r, err := c.reqCtxCreator(magic, c)
			if err != nil {
				ioutil.LogError(err)
				return
			}
			r.SetTimeout(c.ctx, c.config.RequestTimeout.Duration)
			if raw := r.GetMessage(); raw != nil {
				if opcode, flag, e := proto.GetOpCodeAndFlag(raw); e == nil {
					if opcode == proto.OpCodeNop && ((flag & 1) != 0) {
						c.reqHandler.OnKeepAlive(c, r)
						continue
					}
				}
			}

			if debug.DEBUG {
				var vOpmsg proto.OperationalMessage
				if err = vOpmsg.Decode(r.GetMessage()); err != nil {
					glog.Error(err)
					r.GetMessage().HexDump()
					vOpmsg.PrettyPrint(os.Stdout)
					panic(err)
				}
			}

			if glog.LOG_VERBOSE {
				msg := r.GetMessage()
				opcode, _ := proto.GetOpCode(r.GetMessage())
				glog.Verbosef("receiving req: opaque=%d op=%s size=%d",
					msg.GetOpaque(), opcode.String(), msg.GetMsgSize())
				glog.Verbosef("request timeout: %s", c.config.RequestTimeout.Duration.String())
			}

			// counter ++
			atomic.AddInt32(&c.pendingReq, 1)
			if c.reqCounter != nil {
				c.reqCounter.Add(1)
			}
			go c.reqHandler.Process(r)
		}
	}
}

func (c *Connector) doWrite() {
	type pendingWriteT struct {
		resp     IResponseContext
		nToWrite int64
	}
	var pendingWrites []pendingWriteT
	var chHavingDataToWrite chan bool
	var chClosedForNotifyingFlush chan bool = make(chan bool)
	close(chClosedForNotifyingFlush)

	var wBuf *bytes.Buffer
	szWBuf := c.config.IOBufSize
	if szWBuf < kMinSizeWriteBuffer {
		szWBuf = kMinSizeWriteBuffer
	}

	wBuf = &bytes.Buffer{}
	wBuf.Grow(szWBuf)
	maxBufSizeAllocated := szWBuf

	funOnWrite := func(n int64) {
		for i, _ := range pendingWrites {
			pending := &pendingWrites[i]
			if n >= pending.nToWrite {
				pending.resp.OnComplete()
				n -= pending.nToWrite
			} else {
				pending.nToWrite -= n
				if i != 0 {
					pendingWrites = pendingWrites[i-1:]
				}
				return
			}
		}
		pendingWrites = pendingWrites[:0]
	}
	funBufferForWrite := func(response IResponseContext) (n int, err error) {
		if response != nil {
			if n, err = response.Write(wBuf); err == nil {
				if debug.DEBUG {
					if n != int(response.GetMsgSize()) {
						glog.Error("n != msgSize")
						cal.Event("InConnectError", "corrupt_n", cal.StatusError, nil)
						panic("")
					}
				}

				if wBuf.Cap() > maxBufSizeAllocated {
					//glog.Infof("buf Cap : %d  len: %d", wBuf.Cap(), wBuf.Len())
					maxBufSizeAllocated = wBuf.Cap()
				}
				pendingWrites = append(pendingWrites, pendingWriteT{response, int64(response.GetMsgSize())})
			} else {
				glog.Errorf("write error :%s", err)
			}
			if c.reqCounter != nil {
				c.reqCounter.Add(-1)
			}
			atomic.AddInt32(&c.pendingReq, -1)
		}
		return
	}

	var timer *util.TimerWrapper
	defer func() {
		c.Close()
		if timer != nil {
			timer.Stop()
		}
		glog.Verboseln("writer exit")
	}()

	var chTimeout <-chan time.Time = nil
	var shutdown bool = false

	// instantiate a timer, but not started yet
	timer = util.NewTimerWrapper(2 * c.config.RequestTimeout.Duration)

	maxWBufSize := c.config.MaxBufferedWriteSize

	for {
		select {
		case <-c.chStop:
			if atomic.LoadInt32(&c.pendingReq) <= 0 {
				return
			}
			c.chStop = nil

			// to be safe, graceful shutdown timer is set to 2 times request timeout
			timer.Reset(2 * c.config.RequestTimeout.Duration)
			chTimeout = timer.GetTimeoutCh()
			shutdown = true

		case v, ok := <-c.chResponse:
			if !ok {
				glog.Debugf("response channel closed")
				return
			}
			n := 0
			nResp := 0
			if k, err := funBufferForWrite(v); err != nil {
				glog.Error(err)
				return
			} else {
				n += k
				nResp++
			}
		loop:
			for n < maxWBufSize {
				select {
				case resp, ok := <-c.chResponse:
					if !ok {
						glog.Debugf("response channel closed")
						return
					}

					if resp != nil {
						if k, err := funBufferForWrite(resp); err != nil {
							glog.Error(err)
							return
						} else {
							n += k
							nResp++
						}
					}

				default:
					break loop
				}
			}
			if wBuf.Len() >= kMinSizeWriteBuffer {
				if n, err := wBuf.WriteTo(c.conn); err != nil {
					if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
						continue
					} else {
						glog.Error(err)
						return
					}
				} else {
					funOnWrite(n)
				}

			}
			if wBuf.Len() != 0 {
				chHavingDataToWrite = chClosedForNotifyingFlush
			} else {
				chHavingDataToWrite = nil
			}

			if shutdown && atomic.LoadInt32(&c.pendingReq) <= 0 {
				return
			}
		case <-chHavingDataToWrite:
			glog.Verbosef("write to connection")
			if n, err := wBuf.WriteTo(c.conn); err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
					continue
				} else {
					glog.Error(err)
					return
				}
			} else {
				funOnWrite(n)
			}
			if wBuf.Len() == 0 {
				chHavingDataToWrite = nil
			}
		case <-chTimeout:
			glog.Debugf("in_conn: writer graceful shutdown timeout, pending req=%d",
				atomic.LoadInt32(&c.pendingReq))
			return
		}
	}
}

func (c *Connector) OnKeepAlive() {
	c.config.IdleTimeout.Duration = 3600 * 24 * time.Second //may introduce a config variable later
}
