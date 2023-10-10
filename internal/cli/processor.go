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
	"fmt"
	"sync"
	"sync/atomic"
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

var (
	connRecycleTimeout = time.Duration(0 * time.Second)
)

func SetConnectRecycleTimeout(recycleTimeout time.Duration) {
	if recycleTimeout < 0 {
		return
	}
	if recycleTimeout > 0 {
		if recycleTimeout < time.Duration(5*time.Second) {
			recycleTimeout = time.Duration(5 * time.Second)
		}
		if recycleTimeout > time.Duration(90*time.Second) {
			recycleTimeout = time.Duration(90 * time.Second)
		}
	}
	connRecycleTimeout = recycleTimeout
	glog.Debugf("Set conn_recycle_timeout=%v", connRecycleTimeout)
}

func SetDefaultRecycleTimeout(defaultTimeout time.Duration) {
	if connRecycleTimeout > 0 {
		glog.Infof("Default conn_recycle_timeout=%v", connRecycleTimeout)
		return // already set
	}
	connRecycleTimeout = defaultTimeout
}

func (e *IOError) Retryable() bool { return true }

func (e *IOError) Error() string {
	return "IOError: " + e.Err.Error()
}

var (
	kMaxRequestChanBufferSize = 1024

	kCalTxnType    = "JUNO_CLIENT"
	kCalSslTxnType = "JUNO_SSL_CLIENT"

	poolMap   = make(map[string]*Processor, 10)
	execCount int64
	mutex     sync.Mutex
)

type Processor struct {
	PoolSize   int
	server     io.ServiceEndpoint
	sourceName string
	connIndex  int

	connectTimeout  time.Duration
	responseTimeout time.Duration
	getTLSConfig    func() *tls.Config

	chDone     chan bool
	chProcDone <-chan bool
	chRequest  chan *RequestContext
	startOnce  sync.Once
	moreConns  []*Processor
}

func addProcessorExecutor(c *Processor) {
	if c.PoolSize == 1 {
		atomic.AddInt64(&execCount, 1)
		return
	}
	poolMap[c.server.Addr] = c
}

func getProcessor(addr string) *Processor {
	c, found := poolMap[addr]
	if !found {
		return nil
	}
	return c
}

func decrementExecutor() {
	atomic.AddInt64(&execCount, -1)
}

func ShowProcStats() {
	glog.Infof("pool_count=%d", len(poolMap))
	mutex.Lock()
	defer mutex.Unlock()

	for k, v := range poolMap {
		glog.Infof("addr=%s, pool_size=%d", k, v.PoolSize)
	}

	count := atomic.LoadInt64(&execCount)
	glog.Infof("exec_count=%d", count)
}

func NewProcessor(
	server io.ServiceEndpoint,
	sourceName string,
	connPoolSize int,
	connectTimeout time.Duration,
	responseTimeout time.Duration,
	getTLSConfig func() *tls.Config) *Processor {

	if connPoolSize < 1 {
		connPoolSize = 2
	}
	if connPoolSize > 20 {
		connPoolSize = 20
	}
	if connPoolSize > 1 {
		mutex.Lock()
		defer mutex.Unlock()
		proc := getProcessor(server.Addr)
		if proc != nil {
			return proc
		} // else need to create processor
	}

	if connectTimeout == 0 {
		connectTimeout = time.Duration(2000 * time.Millisecond)
	}

	glog.Debugf("addr=%s pool_size=%d connect_timeout=%dms response_timeout=%dms",
		server.Addr, connPoolSize, connectTimeout.Nanoseconds()/int64(1e6),
		responseTimeout.Nanoseconds()/int64(1e6))
	chDone := make(chan bool)
	chRequest := make(chan *RequestContext, kMaxRequestChanBufferSize)

	c := &Processor{
		PoolSize:        connPoolSize,
		server:          server,
		sourceName:      sourceName,
		connIndex:       0,
		connectTimeout:  connectTimeout,
		responseTimeout: responseTimeout,
		getTLSConfig:    getTLSConfig,
		chDone:          chDone,
		chRequest:       chRequest,
		moreConns:       make([]*Processor, connPoolSize-1),
	}
	for i := 0; i < connPoolSize-1; i++ {
		c.moreConns[i] = &Processor{
			PoolSize:        connPoolSize,
			server:          server,
			sourceName:      sourceName,
			connIndex:       i + 1,
			connectTimeout:  connectTimeout,
			responseTimeout: responseTimeout,
			getTLSConfig:    getTLSConfig,
			chDone:          chDone,
			chRequest:       chRequest,
		}
	}
	addProcessorExecutor(c)
	return c
}

func (c *Processor) Start() {
	var connDone = make(chan bool, len(c.moreConns)+1)
	c.startOnce.Do(func() {
		c.chProcDone = StartRequestProcessor(
			c.server, c.sourceName, connDone, c.connIndex,
			c.connectTimeout, c.responseTimeout, c.getTLSConfig, c.chDone, c.chRequest)
		for _, p := range c.moreConns {
			p.chProcDone = StartRequestProcessor(
				p.server, p.sourceName, connDone, p.connIndex,
				p.connectTimeout, p.responseTimeout, c.getTLSConfig, p.chDone, p.chRequest)
		}
		if len(c.moreConns) > 0 {
			glog.Infof("conn_count=%d", len(c.moreConns)+1)
		}
		if len(c.moreConns) == 0 {
			return
		}
		timer := time.NewTimer(c.connectTimeout)
		defer timer.Stop()
		select {
		case <-connDone:
		case <-timer.C:
		}
	})
}

func (c *Processor) Close() {
	if c.PoolSize > 1 {
		// Connection pool is persistent
		return
	}
	close(c.chDone)
	<-c.chProcDone
	decrementExecutor()
}

func (c *Processor) sendWithResponseChannel(chResponse chan IResponseContext, m *proto.OperationalMessage) error {
	var err error
	select {
	case c.chRequest <- NewRequestContext(m, chResponse):
	default:
		err = fmt.Errorf("Likely queue full, qlen=%d ps=%d", len(c.chRequest), c.PoolSize)
	}

	if glog.LOG_VERBOSE {
		opcode := m.GetOpCode()
		buf := logging.NewKVBufferForLog()
		if opcode != proto.OpCodeNop {
			buf.AddReqIdString(m.GetRequestIDString())
		}
		if err == nil {
			glog.Verbosef("proc <- %s %s", opcode.String(), buf.String())
		} else {
			glog.Verbosef("Failed: proc <- %s %s", opcode.String(), buf.String())
		}

	}
	return err
}

func (c *Processor) send(request *proto.OperationalMessage) (<-chan IResponseContext, error) {
	ch := make(chan IResponseContext)
	return ch, c.sendWithResponseChannel(ch, request)
}

func (c *Processor) ProcessRequest(request *proto.OperationalMessage) (resp *proto.OperationalMessage, err error) {
	timeStart := time.Now()

	ch, err := c.send(request)
	if err == nil {
		if r, ok := <-ch; ok {
			resp = r.GetResponse()
			err = r.GetError()
		} else {
			resp = nil
			err = fmt.Errorf("response channel closed by request processor")
		}
	}

	if err != nil && !specialError(err) {
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

		if err == nil {
			status := resp.GetOpStatus()
			b := logging.NewKVBuffer()
			b.AddOpRequestResponseInfo(request, resp)
			cal.AtomicTransaction(txnType, request.GetOpCode().String(), logging.CalStatus(status).CalStatus(), rht, b.Bytes())
		} else {
			tail := fmt.Sprintf("raddr=%s&res_timeout=%dms&ns=%s&%s", c.server.Addr,
				c.responseTimeout.Nanoseconds()/int64(1e6), request.GetNamespace(), err.Error())
			cal.AtomicTransaction(txnType, request.GetOpCode().String(),
				cal.StatusError, rht, []byte(tail)) ///TODO to change: data to cal
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
