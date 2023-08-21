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

package cli

import (
	"crypto/tls"
	"io"

	"math/rand"
	"net"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"juno/third_party/forked/golang/glog"

	junoio "juno/pkg/io"
	"juno/pkg/proto"
	"juno/pkg/util"
)

type (
	Connection struct {
		tracker          *PendingTracker
		conn             net.Conn
		chReaderResponse <-chan *ReaderResponse
		beingRecycle     bool
	}
)

var (
	connCount int64
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

func (c *Connection) Close() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

func (c *Connection) CloseWrite() {
	if c.conn != nil {
		if i, ok := c.conn.(interface {
			CloseWrite() error
		}); ok {
			i.CloseWrite()
		} else {
			c.conn.Close()
		}
		c.conn = nil
	}
}

func (c *Connection) Shutdown() {
	if c.conn != nil {
		if i, ok := c.conn.(interface {
			CloseRead() error
		}); ok {
			i.CloseRead()
		} else {
			c.conn.Close()
		}
		c.conn = nil
		c.beingRecycle = false
		glog.Debugf("Close connCount=%d", atomic.AddInt64(&connCount, -1))
	}
}

func (c *Connection) GetReqTimeoutCh() <-chan time.Time {
	if c.tracker == nil {
		return nil
	}
	return c.tracker.GetTimeoutCh()
}

func startResponseReader(r io.ReadCloser) <-chan *ReaderResponse {
	chReaderResponse := make(chan *ReaderResponse, 2)
	go func() {
		defer func() {
			close(chReaderResponse)
			glog.Verbosef("reader exits")
			r.Close()
		}()

		for {
			var raw proto.RawMessage
			var err error

			if _, err = raw.Read(r); err == nil {
				resp := &proto.OperationalMessage{}
				if err = resp.Decode(&raw); err == nil {
					chReaderResponse <- NewReaderResponse(resp)
				}
			}
			if err != nil {
				chReaderResponse <- NewErrorReaderResponse(err)
				if nerr, ok := err.(net.Error); ok {
					if nerr.Timeout() {
						glog.Warningln(err)
						return
					}
				}

				if opErr, ok := err.(*net.OpError); ok {
					if sErr, ok := opErr.Err.(*os.SyscallError); ok {
						if sErr.Err == syscall.ECONNRESET {
							glog.Debugln(err)
							return
						}
					}
					if opErr.Error() != "use of closed network connection" { ///READLLY hate this way
						return
					}
				}

				if err == io.EOF {
					glog.Debugln(err)
				} else {
					glog.Warningln(err)
				}
				return
			}
		}
	}()
	return chReaderResponse
}

func StartRequestProcessor(
	server junoio.ServiceEndpoint,
	sourceName string,
	connDone chan<- bool,
	connIndex int,
	connectTimeout time.Duration,
	responseTimeout time.Duration,
	getTLSConfig func() *tls.Config,
	chDone <-chan bool,
	chRequest <-chan *RequestContext) (chProcessorDone <-chan bool) {

	ch := make(chan bool)
	go doRequestProcess(server, sourceName, connDone, connIndex,
		connectTimeout, responseTimeout, getTLSConfig, chDone, ch, chRequest)

	return ch
}

func resetRecycleTimer(
	server junoio.ServiceEndpoint,
	connIndex int,
	responseTimeout time.Duration,
	connRecycleTimer *util.TimerWrapper) {
	if connRecycleTimeout <= 0 {
		return
	}

	t := connRecycleTimeout
	// Reduced by a random value between 0 to 20%.
	t = t * time.Duration(1000-rand.Intn(200)) / 1000
	if t < 2*responseTimeout {
		t = 2 * responseTimeout
	}
	glog.Debugf("connection=%d addr=%s", connIndex, server.Addr)
	connRecycleTimer.Reset(t)
}

func doRequestProcess(
	server junoio.ServiceEndpoint,
	sourceName string,
	connDone chan<- bool,
	connIndex int,
	connectTimeout time.Duration,
	responseTimeout time.Duration,
	getTLSConfig func() *tls.Config,
	chDone <-chan bool,
	chDoneNotify chan<- bool,
	chRequest <-chan *RequestContext) {

	glog.Debugf("Start connection %d", connIndex)

	connRecycleTimer := util.NewTimerWrapper(connRecycleTimeout)
	active := &Connection{}
	recycled := &Connection{}

	connect := func() error {
		var conn net.Conn
		var err error
		if server.SSLEnabled && getTLSConfig != nil {
			conn, err = Dial(server.Addr, connectTimeout, getTLSConfig)
		} else {
			conn, err = junoio.Connect(&server, connectTimeout)
		}
		if err != nil {
			return err
		}
		active.conn = conn
		active.tracker = newPendingTracker(responseTimeout)
		active.chReaderResponse = startResponseReader(conn)
		resetRecycleTimer(
			server,
			connIndex,
			responseTimeout,
			connRecycleTimer)
		glog.Debugf("Open connCount=%d", atomic.AddInt64(&connCount, 1))
		return nil
	}

	var sequence uint32
	defer close(chDoneNotify)

	var err error
	connect()
	connDone <- true

	for {
		select {
		case <-chDone:
			glog.Verbosef("proc done channel got notified")
			active.Shutdown() ///TODO to revisit
			return
		case _, ok := <-connRecycleTimer.GetTimeoutCh():
			connRecycleTimer.Stop()
			if ok {
				glog.Debug("connection recycle timer fired")
				recycled.Shutdown()
				recycled = active
				active = &Connection{}
				err = connect()
				if err != nil {
					glog.Error(err)
					active = recycled // reuse current one.
					recycled = &Connection{}
					resetRecycleTimer(
						server,
						connIndex,
						responseTimeout,
						connRecycleTimer)
				} else {
					recycled.beingRecycle = true
					if recycled.tracker != nil &&
						len(recycled.tracker.pendingQueue) == 0 {
						recycled.Shutdown()
					}
				}
			} else {
				glog.Errorf("connection recycle timer not ok")
			}

		case now, ok := <-active.GetReqTimeoutCh():
			if ok {
				active.tracker.OnTimeout(now)
			} else {
				glog.Error("error to get from active request timeout channel")
			}
		case now, ok := <-recycled.GetReqTimeoutCh():
			if ok {
				recycled.tracker.OnTimeout(now)
				if len(recycled.tracker.pendingQueue) == 0 {
					// close write for the recybled connection as it has handled all the pending request(s)")
					recycled.Shutdown()
				} else {
					glog.Debugf("being recycled request timeout")

				}
			} else {
				glog.Error("error to read from recycled request timeout channel")
			}

		case r, ok := <-chRequest:
			if !ok { // shouldn't happen
				continue // ignore
			}
			glog.Debugf("connection %d got request", connIndex)
			var err error

			if active.conn == nil {
				err = connect()
			}
			if err == nil {
				conn := active.conn
				saddr := conn.LocalAddr().(*net.TCPAddr)
				req := r.GetRequest()
				if req == nil {
					glog.Error("nil request")
					continue // ignore
				}
				req.SetSource(saddr.IP, uint16(saddr.Port), []byte(sourceName))
				sequence++
				var raw proto.RawMessage
				if err = req.Encode(&raw); err != nil {
					glog.Errorf("encoding error %s", err)
					continue // ignore
				}
				raw.SetOpaque(sequence)

				if _, err = raw.Write(conn); err == nil {
					active.tracker.OnRequestSent(r, sequence)
				} else {
					r.ReplyError(err)
					active.Close()
				}
			} else {
				ErrConnect.SetError(err.Error())
				r.ReplyError(ErrConnect)
			}
		case readerResp, ok := <-active.chReaderResponse:
			if ok {
				active.tracker.OnResonseReceived(readerResp)
			} else {
				glog.Debug("active reader response channel closed")
				active.tracker.OnResponseReaderClosed()
				active.Close()
				active = &Connection{}
			}
		case readerResp, ok := <-recycled.chReaderResponse:
			if ok {
				recycled.tracker.OnResonseReceived(readerResp)

			} else {
				glog.Debug("recycled reader response channel closed")
				recycled.tracker.OnResponseReaderClosed()
				recycled = &Connection{}
			}
		}
	}
}
