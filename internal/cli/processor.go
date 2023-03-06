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
	"fmt"
	"os"
	"sync"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/io"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
)

type IOError struct {
	Err error
}

func (e *IOError) Retryable() bool { return true }

func (e *IOError) Error() string {
	return "IOError: " + e.Err.Error()
}

var (
	kMaxRequestChanBufferSize = 1024

	kCalTxnType    = "JUNO_CLIENT"
	kCalSslTxnType = "JUNO_SSL_CLIENT"
)

type Processor struct {
	server     io.ServiceEndpoint
	sourceName string

	connectTimeout     time.Duration
	requestTimeout     time.Duration
	connRecycleTimeout time.Duration

	chDone     chan bool
	chProcDone <-chan bool
	chRequest  chan *RequestContext
	startOnce  sync.Once
}

func NewProcessor(
	server io.ServiceEndpoint,
	sourceName string,
	connectTimeout time.Duration,
	requestTimeout time.Duration,
	connRecycleTimeout time.Duration) *Processor {

	c := &Processor{
		server:             server,
		sourceName:         sourceName,
		connectTimeout:     connectTimeout,
		requestTimeout:     requestTimeout,
		connRecycleTimeout: connRecycleTimeout,
		chDone:             make(chan bool),
		chRequest:          make(chan *RequestContext, kMaxRequestChanBufferSize),
	}
	return c
}

func (c *Processor) Start() {
	c.startOnce.Do(func() {
		c.chProcDone = StartRequestProcessor(
			c.server, c.sourceName, c.connectTimeout, c.requestTimeout, c.connRecycleTimeout, c.chDone, c.chRequest)
	})
}

///TODO revisit
func (c *Processor) Close() {
	close(c.chDone)
	<-c.chProcDone
}

func (c *Processor) sendWithResponseChannel(chResponse chan IResponseContext, m *proto.OperationalMessage) (ok bool) {
	select {
	case c.chRequest <- NewRequestContext(m, chResponse):
		ok = true
	default:
		ok = false
	}
	if glog.LOG_VERBOSE {
		opcode := m.GetOpCode()
		buf := logging.NewKVBufferForLog()
		if opcode != proto.OpCodeNop {
			buf.AddReqIdString(m.GetRequestIDString())
		}
		if ok {
			glog.Verbosef("proc <- %s %s", opcode.String(), buf.String())
		} else {
			glog.Verbosef("Failed: proc <- %s %s", opcode.String(), buf.String())
		}

	}
	return
}

func (c *Processor) send(request *proto.OperationalMessage) (chResponse <-chan IResponseContext, ok bool) {
	ch := make(chan IResponseContext)
	chResponse = ch
	ok = c.sendWithResponseChannel(ch, request)
	return
}

func (c *Processor) ProcessRequest(request *proto.OperationalMessage) (resp *proto.OperationalMessage, err error) {
	timeStart := time.Now()

	glog.Verbosef("process request rid=%s", request.GetRequestIDString())
	if ch, sent := c.send(request); sent {
		if r, ok := <-ch; ok {
			resp = r.GetResponse()
			err = r.GetError()
		} else {
			resp = nil
			err = fmt.Errorf("response channel closed by request processor")
		}
	} else {
		err = fmt.Errorf("fail to send request")
	}
	if err != nil {
		err = &IOError{err}
	}
	if cal.IsEnabled() {
		var txnType string
		if c.server.SSLEnabled {
			txnType = kCalSslTxnType
		} else {
			txnType = kCalTxnType
		}
		rht := time.Since(timeStart)

		// Get user name from OS to log in CAL
		username := os.Getenv("USER")

		if err == nil {
			status := resp.GetOpStatus()
			b := logging.NewKVBuffer()
			b.AddOpRequestResponseInfoWithUser(request, resp, username)
			cal.AtomicTransaction(txnType, request.GetOpCode().String(), logging.CalStatus(status).CalStatus(), rht, b.Bytes())
		} else {
			cal.AtomicTransaction(txnType, request.GetOpCode().String(), cal.StatusError, rht, []byte(err.Error())) ///TODO to change: data to cal
		}
	}

	return
}

func (c *Processor) ProcessBatchRequests(requests []*proto.OperationalMessage) (responses []*proto.OperationalMessage, err error) {
	numRequests := len(requests)
	if numRequests == 0 {
		err = fmt.Errorf("zero requests passed in")
		return
	}
	chResponse := make(chan IResponseContext)

	responses = make([]*proto.OperationalMessage, numRequests, numRequests)
	for i := 0; i < numRequests; i++ {
		requests[i].SetOpaque(uint32(i))
	}
	numSent := 0
	numReceived := 0
	chWrite := c.chRequest

	ctx := NewRequestContext(requests[numSent], chResponse)
	chTicker := time.Tick(20 * time.Second)
	for numSent < numRequests || numSent != numReceived {
		select {
		case chWrite <- ctx:
			numSent++
			if numSent >= numRequests {
				chWrite = nil
				ctx = nil
			} else {
				ctx = NewRequestContext(requests[numSent], chResponse)
			}
		case r := <-chResponse:
			if r.GetError() == nil {
				responses[r.GetOpaque()] = r.GetResponse()
			} else {
				glog.Errorln(r.GetError())
			}
			numReceived++

		///TODO timeout .. double guarantee
		case <-chTicker:
			glog.Debugf("numSent = %d		numReceived = %d\n", numSent, numReceived)
		}
	}
	return
}
