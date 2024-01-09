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
	"fmt"
	"io"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/errors"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/logging/otel"
	"juno/pkg/proto"
	"juno/pkg/util"
)

type (
	IConnEventHandler interface {
		OnConnectSuccess(conn Conn, connector *OutboundConnector, timeTaken time.Duration)
		OnConnectError(timeTaken time.Duration, connStr string, err error)
	}

	//
	// OutboundProcessor manages a pool of one or more underlying connections
	// to a downstream server; It also bounces incoming requests when all
	// connections are down.
	//
	OutboundProcessor struct {
		numConns     int32           // size of connector pool
		numActive    int32           // automic counter of active connectors
		connInfo     ServiceEndpoint // ip:port for tcp connect to downstream server, e.g. ss
		connectors   []*OutboundConnector
		connCh       chan *OutboundConnector
		reqCh        chan IRequestContext
		monitorCh    chan int // channel for signal connector communication error
		doneCh       chan struct{}
		shutdown     bool
		enableBounce bool
		config       *OutboundConfig
		wg           sync.WaitGroup

		// when connection recycle is enabled, we'll set aside one standby connector,
		// rotate out the connector needs to be recycled every connectRecycleT/numConns time
		standbyId  int
		connEvHdlr IConnEventHandler
	}
)

func (p *OutboundProcessor) Init(endpoint ServiceEndpoint, config *OutboundConfig, enableBounce bool) {
	numConns := config.NumConnsPerTarget
	p.numConns = numConns
	p.numActive = 0
	p.connInfo = endpoint
	p.connectors = make([]*OutboundConnector, numConns+1)
	p.connCh = make(chan *OutboundConnector, numConns+1)
	p.monitorCh = make(chan int, numConns+1)
	p.reqCh = make(chan IRequestContext, config.ReqChanBufSize)
	p.doneCh = make(chan struct{})
	p.shutdown = false
	p.enableBounce = enableBounce
	p.config = config
	p.connEvHdlr = p
	p.standbyId = -1

	rand.Seed(time.Now().UnixNano())
}

func (p *OutboundProcessor) SetConnEventHandler(hdlr IConnEventHandler) {
	p.connEvHdlr = hdlr
}

func (p *OutboundProcessor) Start() {
	p.wg.Add(1)
	go p.Run()
}

func NewOutbProcessor(endpoint ServiceEndpoint, config *OutboundConfig, enableBounce bool) (p *OutboundProcessor) {
	p = &OutboundProcessor{}
	p.Init(endpoint, config, enableBounce)

	p.Start()
	return p
}

func NewOutboundProcessor(connInfo string, config *OutboundConfig, enableBounce bool) *OutboundProcessor {
	return NewOutbProcessor(ServiceEndpoint{Addr: connInfo}, config, enableBounce)
}

func (p *OutboundProcessor) GetRequestCh() chan IRequestContext {
	return p.reqCh
}

//
// Non-blocking send
//
func (p *OutboundProcessor) sendRequest(req IRequestContext) (err *errors.Error) {
	// send request
	select {
	case p.reqCh <- req:
	default:
		return errors.ErrBusy
	}
	return nil
}

func (p *OutboundProcessor) SendRequest(req IRequestContext) (err *errors.Error) {
	// bounce check
	if p.enableBounce && atomic.LoadInt32(&p.numActive) <= 0 {
		return errors.ErrNoConnection
	}

	return p.sendRequest(req)
}

func (p *OutboundProcessor) SendRequestLowPriority(req IRequestContext) (err *errors.Error) {
	// always bounce for snapshot -- to yield priority for real time traffic
	if atomic.LoadInt32(&p.numActive) <= 0 {
		return errors.ErrNoConnection
	}

	// reject when half of the channel is used.
	if len(p.reqCh) >= p.config.ReqChanBufSize/2 {
		return errors.ErrBusy
	}

	return p.sendRequest(req)
}

func (p *OutboundProcessor) GetRandFrequency(freq time.Duration) time.Duration {
	if int(freq/4) == 0 {
		return freq
	}
	return time.Duration(int64(int(freq*3/4) + rand.Intn(int(freq/4))))
}

func (p *OutboundProcessor) Run() {
	defer p.wg.Done()

	var bounceCh chan IRequestContext = nil
	if p.enableBounce {
		bounceCh = p.reqCh
	}

	p.wg.Add(int(p.numConns))
	for i := 0; i < int(p.numConns); i++ {
		go p.connect(p.connCh, i, nil)
	}

	// if connection recycle is enabled, make the last connector as the standby connector first
	// otherwise, the last connector is not used.
	if p.config.EnableConnRecycle {
		p.standbyId = int(p.numConns)
	}

	// recycle frequency randomnized a bit
	var recycleFrequency time.Duration
	var recycleTimer *util.TimerWrapper = nil
	var reccycleCh <-chan time.Time = nil

	if p.config.EnableConnRecycle {
		recycleFrequency = time.Duration(int64(p.config.ConnectRecycleT.Duration) / int64(p.numConns))
		recycleTimer = util.NewTimerWrapper(p.GetRandFrequency(recycleFrequency))
		recycleTimer.Reset(p.GetRandFrequency(recycleFrequency))
		defer recycleTimer.Stop()
		reccycleCh = recycleTimer.GetTimeoutCh()
	}

	for {
		select {
		case conn := <-p.connCh:
			glog.Debugf("connector %s started", conn.displayName)

			p.connectors[conn.GetId()] = conn
			atomic.AddInt32(&p.numActive, 1)
			conn.Start()

			if p.enableBounce && p.numActive > 0 {
				bounceCh = nil
			}

			conn.SetState(SERVING)
			if conn.GetId() == p.standbyId {
				// extra connection is up, recycle the next one
				p.standbyId = (p.standbyId + 1) % (int(p.numConns) + 1)
				connector := p.connectors[p.standbyId]
				if connector != nil && connector.IsActive() {
					glog.Debugf("recycle connector %d", p.standbyId)
					p.connectors[p.standbyId].Recycle()
				}
			}

		case <-p.doneCh:
			return

		case id, ok := <-p.monitorCh:
			if !ok || id >= int(p.numConns+1) {
				return
			}

			glog.Debugf("%s connector %d down", p.connInfo.GetConnString(), id)

			connector := p.connectors[id]
			if connector == nil {
				// the connection for this slot is ongoing
				continue
			}

			atomic.AddInt32(&p.numActive, -1)

			if p.enableBounce && p.numActive <= 0 {
				bounceCh = p.reqCh
			}

			if p.config.EnableConnRecycle && id == p.standbyId {
				// waiting for it's turn to restart
				connector.SetState(WAITING)
				continue
			}

			p.connectors[id] = nil

			if !p.shutdown {
				p.wg.Add(1)
				go p.connect(p.connCh, id, connector)
			}

		// bounceCh is reqCh only if all connections are down
		case req, ok := <-bounceCh:
			if ok && req != nil {
				ReplyError(req, proto.StatusNoConn)
			}

		case <-reccycleCh:
			// in order to recycle a connection, the standby connector need to kick in and connected first.
			connector := p.connectors[p.standbyId]

			if !p.shutdown && (connector == nil || connector.AllowRestart()) {
				p.wg.Add(1)
				//glog.Infof("start standby connection: %d", p.standbyId)
				go p.connect(p.connCh, p.standbyId, connector)
			}
			recycleTimer.Reset(p.GetRandFrequency(recycleFrequency))
		}
	}
}

func (p *OutboundProcessor) Shutdown() {

	glog.Debugf("Shutdown: OutboundProcessor=%p, gid=%d", p, util.GetGID())

	p.shutdown = true
	for i := 0; i < int(p.numConns); i++ {
		if p.connectors[i] != nil {
			p.connectors[i].Shutdown()
		}
	}
	close(p.monitorCh)
	close(p.doneCh)
}

func (p *OutboundProcessor) WaitShutdown() {
	p.wg.Wait()
	close(p.connCh)
	close(p.reqCh)
}

func (p *OutboundProcessor) GetNumConnections() int {

	numConns := atomic.LoadInt32(&p.numActive)
	return int(numConns)
}

func (p *OutboundProcessor) GetIsConnected() int {
	numConns := atomic.LoadInt32(&p.numActive)
	if numConns == 0 {
		return 0
	} else {
		return 1
	}
}

func (p *OutboundProcessor) OnConnectSuccess(conn Conn, connector *OutboundConnector, timeTaken time.Duration) {
	if cal.IsEnabled() {
		var data []byte
		if p.connInfo.SSLEnabled {
			b := logging.NewKVBuffer()
			b.Add([]byte("ssl"), conn.GetStateString())
			data = b.Bytes()
		}

		cal.AtomicTransaction(cal.TxnTypeConnect, p.connInfo.GetConnString(), cal.StatusSuccess, timeTaken, data)
	}
	otel.RecordOutboundConnection(p.connInfo.GetConnString(), otel.StatusSuccess, timeTaken.Microseconds())
	otel.RecordCount(otel.TLSStatus, []otel.Tags{{otel.Endpoint, p.connInfo.GetConnString()}, {otel.TLS_version, conn.GetTLSVersion()},
		{otel.Cipher, conn.GetCipherName()}, {otel.Ssl_r, conn.DidResume()}})
}

func (p *OutboundProcessor) OnConnectError(timeTaken time.Duration, connStr string, err error) {
	if cal.IsEnabled() {
		cal.AtomicTransaction(cal.TxnTypeConnect, connStr, cal.StatusError, timeTaken, []byte(err.Error()))
	}
	otel.RecordOutboundConnection(connStr, otel.StatusError, timeTaken.Microseconds())
}

func (p *OutboundProcessor) connect(connCh chan *OutboundConnector, id int, connector *OutboundConnector) {
	defer p.wg.Done()

	// first make sure the existing one is cleaned up properly
	if connector != nil {
		connector.Shutdown()
		connector.SetState(CONNECTING)
	}

	interval := p.config.ReconnectIntervalBase
	timer := util.NewTimerWrapper(time.Duration(interval) * time.Millisecond)
	timer.Reset(time.Duration(interval) * time.Millisecond)
	defer timer.Stop()

	for {
		if p.shutdown {
			return
		}

		select {
		case <-p.doneCh:
			return

		case now := <-timer.GetTimeoutCh():
			conn, err := ConnectTo(&p.connInfo, p.config.ConnectTimeout.Duration)
			timeTaken := time.Since(now)
			if err == nil {
				// TODO reuse!!!!
				connector = NewOutboundConnector(id, conn.GetNetConn(), p.reqCh, p.monitorCh, p.config)
				glog.Debugf("connector connected: id %d, laddr: %v", id, conn.GetNetConn().LocalAddr())
				if p.connEvHdlr != nil {
					p.connEvHdlr.OnConnectSuccess(conn, connector, timeTaken)
				}

				if !connector.Handshake() {
					glog.Debugf("handshake failed")
					connector.Close()
					if interval < p.config.ReconnectIntervalMax {
						interval = 2 * interval
					}
					timer.Reset(time.Duration(interval) * time.Millisecond)
					continue
				}

				// byPassingLTM if enabled
				pingIP := connector.GetPingIP()
				if len(pingIP) > 0 {
					origIP, origPort := p.GetIPPort()
					if origIP != pingIP {
						newConnInfo := p.connInfo
						newConnInfo.Addr = pingIP + ":" + origPort
						conn2, err := ConnectTo(&newConnInfo, p.config.ConnectTimeout.Duration)
						if err == nil {
							if p.connEvHdlr != nil {
								p.connEvHdlr.OnConnectSuccess(conn2, connector, timeTaken)
							}
							connector.SetNewConn(conn2.GetNetConn())
							connector.Handshake()
							glog.Debugf("byPassingLTM, connected to: %s", newConnInfo.Addr)
						} else {
							if p.connEvHdlr != nil {
								p.connEvHdlr.OnConnectError(timeTaken, newConnInfo.GetConnString(), err)
							}
							glog.Debugf("byPassingLTM, connect to %s failed, revert to LTM", newConnInfo.Addr)
						}
					} else {
						glog.Debugf("pingIP same as original IP, ignor")
					}
				}

				interval = p.config.ReconnectIntervalBase // reset
				if !p.shutdown {
					connCh <- connector
				}
				return
			} else {
				if p.connEvHdlr != nil {
					p.connEvHdlr.OnConnectError(timeTaken, p.connInfo.GetConnString(), err)
				}
				if interval < p.config.ReconnectIntervalMax {
					interval = 2 * interval
				}
				timer.Reset(time.Duration(interval) * time.Millisecond)
			}
		}
	}
}

func (p *OutboundProcessor) GetIPPort() (ip string, port string) {
	res := strings.Split(p.connInfo.Addr, ":")
	if len(res) > 1 {
		ip = res[0]
		port = res[1]
	}
	return
}

func (p *OutboundProcessor) GetConnInfo() string {
	return p.connInfo.Addr ///TODO
}

func (p *OutboundProcessor) GetRequestSendingQueueSize() int {
	return len(p.reqCh)
}

func (p *OutboundProcessor) WriteStats(w io.Writer, indent int) {
	indentStr := strings.Repeat(" ", indent)
	for i := range p.connectors {
		fmt.Fprintf(w, "%sConnector %d %s:\n", indentStr, i, p.connInfo.GetConnString())
		p.connectors[i].WriteStats(w, indent+2)
	}
}
