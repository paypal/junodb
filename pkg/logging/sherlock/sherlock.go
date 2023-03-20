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

// -*- tab-width: 2 -*-

// Package sherlock is a Sherlock metric feeder based on Python Infra client
// For GoLang, API is to Send metrics, which will enqueue to a channel.
// It will fail instantly if the channel is full.
// You need to get a reservation from the PP Sherlock team to use it.

package sherlock

import (
	"errors"
	"fmt"
	"net"
	"runtime"
	"sync"
	"time"

	"juno/third_party/forked/golang/glog"

	proto "github.com/golang/protobuf/proto"

	frontier "juno/pkg/logging/sherlock/sherlockproto"
)

// PrintMsgs true to see the network layer to TSDB
var PrintMsgs = false

type readMsg struct {
	msg []byte
	err error
}

// these things go into a channel which sleeps till expiry, checks if id is still in a hash, then calls cb from the hash
type timeout struct {
	id     uint32
	expiry time.Time
}

// TSDB/frontier implementation for go
// Stolen from https://github.paypal.com/Python/PythonInfrastructure/blob/master/infra/contrib/frontier.py

type frontierMessage struct {
	dim        map[string]string
	data       []FrontierData
	when       time.Time
	resolution uint32
	cb         frontierCb
}

// FrontierClient is the main interface; create one with NewFrontierClient; .Send()
type FrontierClient struct {
	colo         string
	host         string
	port         uint32
	initHost     string
	initPort     uint32
	tenent       string
	env          string
	appSvc       string
	profile      string
	session      []byte
	metrics      chan frontierMessage
	writeMsgs    chan proto.Message
	readMsgs     chan readMsg
	timeOuts     chan timeout
	doConnect    chan bool
	gotConnect   chan bool
	clientMutex  *sync.RWMutex
	sessionMutex *sync.RWMutex
	cbMutex      *sync.RWMutex
	sessionChan  chan bool
	finished     chan bool
	finishedTO   chan bool
	id           uint32
	wsConn       wsConn
	clientID     uint32
	pendingCbs   map[uint32]frontierCb
}

//var SherlockClient *FrontierClient

// NewFrontierClient provides a Sherlock metric publisher  client with specified endpoints/reservation
func NewFrontierClient(host string,
	port uint32,
	tenent string,
	env string,
	appSvc string,
	profile string) (*FrontierClient, error) {
	ws := newWs(host, int(port))
	colo, _err := getEnvFromSyshieraYaml()
	if _err != nil {
		colo = "qa"
	}
	clientIDMutex.Lock()
	clientID = clientID + 1
	myFrontierClient := &FrontierClient{colo,
		host,
		port,
		host,
		port,
		tenent,
		env,
		appSvc,
		profile,
		[]byte{},
		make(chan frontierMessage, 20000),
		make(chan proto.Message, 20000),
		make(chan readMsg, 20000),
		make(chan timeout, 50000),
		make(chan bool, 10),
		make(chan bool, 10),
		&sync.RWMutex{},
		&sync.RWMutex{},
		&sync.RWMutex{},
		make(chan bool, 1),
		make(chan bool, 2),
		make(chan bool, 2),
		1,
		ws,
		clientID,
		make(map[uint32]frontierCb)}

	clientIDMutex.Unlock()
	go myFrontierClient.mainLoopWrite()
	go myFrontierClient.killPendingCb()

	return myFrontierClient, nil
}

func initFCWithConfig(conf *Config) {
	if conf != nil {
		ShrLockConfig = conf
		ShrLockConfig.Default()
		ShrLockConfig.Validate()
		if !ShrLockConfig.Enabled {
			return
		}
		var err error
		SherlockClient, err = NewFrontierClientNormalEndpoints(conf.SherlockSvc,
			conf.SherlockProfile)
		if err != nil {
			glog.Errorln("failed to make Sherlock client", err)
			//TODO : cal event throw ERROR
		}
	}
}

func IsEnabled() bool {
	if ShrLockConfig != nil {
		return ShrLockConfig.Enabled
	}
	return false
}

func GetDimName() string {
	if ShrLockConfig != nil {
		if len(ShrLockConfig.ClientType) == 0 || ShrLockConfig.ClientType == "sherlock" {
			return "pool"
		} else {
			return "application"
		}
	}
	return "pool"
}

type frontierMethod func(m *FrontierClient) error

func (m *FrontierClient) lock() {
	m.clientMutex.Lock()
}

func (m *FrontierClient) unlock() {
	m.clientMutex.Unlock()
}

func (m *FrontierClient) rlock() {
	m.clientMutex.RLock()
}

func (m *FrontierClient) runlock() {
	m.clientMutex.RUnlock()
}

func (m *FrontierClient) connectWs() error {
	var err error
	err = m.wsConn.connect(m.host, int(m.port))
	return err
}

func (m *FrontierClient) connect() {
	// gets the mutux - calls doRedirect
	for {
		m.clearSession()
		m.lock()
		m.host = m.initHost
		m.port = m.initPort
		err := m.connectWs()
		if err != nil {
			m.unlock()
			glog.Errorln("Got an error connecting", err)
			time.Sleep(time.Second * 2)
			continue
		}

		gs := m.generateSessionRequest()

		err = m.wsConn.writeMsg(m.clientID, gs)
		m.unlock()
		if err != nil {
			glog.Errorln("Got an error connecting ", err)
			m.close()
			time.Sleep(time.Second * 2)
			continue
		}
		//err = m.readOneMsg()
		//if err != nil {
		//	glog.Errorf("Got an error from sherlock server ", err)
		//}
		err = m.waitForSession()
		if err == nil {
			return
		} else {
			glog.Errorln("Got an error wait for session ", err)
			m.close()
		}
	}
}

func (m *FrontierClient) startConnect() {
	select {
	case m.doConnect <- true:
		// good
	default:
		// bad but ok
	}
}

// Restart will close the underlying connection to Frontier and re-connect
func (m *FrontierClient) Restart() {
	m.close()
	m.startConnect()
}

// close will close the underlying connection to Frontier so you can force a new connection
func (m *FrontierClient) close() {
	m.clearSession()
	m.lock()
	m.wsConn.close()
	m.unlock()
}

func (m *FrontierClient) killPendingCb() {
	for {
		select {
		case <-m.finished:
			return
		default:
		}
		t := <-m.timeOuts
		time.Sleep(t.expiry.Sub(time.Now()))
		f := m.getAndClearCb(t.id)
		if f != nil {
			e := fmt.Errorf("timeout for id %d", t.id)
			f(e)
		}
	}
}

func (m *FrontierClient) setCb(cb frontierCb, id uint32) {
	m.cbMutex.Lock()
	m.pendingCbs[id] = cb
	m.cbMutex.Unlock()
}

func (m *FrontierClient) getAndClearCb(id uint32) frontierCb {
	m.cbMutex.Lock()
	cb, ok := m.pendingCbs[id]
	if ok {
		delete(m.pendingCbs, id)
	} else {
		cb = nil
	}
	m.cbMutex.Unlock()
	return cb
}

func (m *FrontierClient) readOneMsg(readMsg []byte) error {
	msg := &frontier.ServerMessageProto{}
	err := proto.Unmarshal(readMsg, msg)
	if err != nil {
		return err
	}
	glog.Debugf("Received: %s %d %s.\n",
		time.Now().Format(time.RFC850),
		m.clientID, msg)
	err = m.handle(msg)
	return err
}

func (m *FrontierClient) mainLoopRead() {
	// one per connection, long lived.
	var err error
	var bytes []byte
	for {
		m.rlock()
		if m.wsConn.ws == nil {
			m.runlock()
			time.Sleep(time.Second * 1)
			continue
		} else {
			m.wsConn.ws.SetReadDeadline(time.Now().Add(5 * time.Second))
			ws := m.wsConn
			m.runlock()
			bytes, err = ws.readMsg()
		}
		e, ok := err.(net.Error)
		if ok && e.Timeout() {
			runtime.Gosched()
			continue // ignore read timeouts
		}
		r := readMsg{msg: bytes, err: err}
		m.readMsgs <- r // block? that's ok don't read till can handle
		if err != nil {
			glog.Error("error for read ", err)
			// no close - will be handled in sync with write loop
			time.Sleep(2 * time.Second) // takes some time to reconnect
		}

	}

}

func (m *FrontierClient) mainLoopWrite() {
	go m.mainLoopRead() // also never exits
	connectionPending := false
	m.doConnect <- true
	for {
		// instead of explicit state, different states in the connection are different
		// sections of this method
		var err error
		select {
		case <-m.gotConnect:
			connectionPending = false
		case <-m.doConnect:
			if connectionPending == false {
				go m.connect()
				connectionPending = true
			}
		case <-m.finished:
			glog.Info("Done - exiting")
			return
		case metric := <-m.metrics:
			if m.checkSession() {
				msg := m.metricToMsg(metric)
				m.lockSession()
				err = m.wsConn.writeMsg(m.clientID, msg)
				m.unlockSession()
				if err != nil {
					glog.Warningln("dropping metric - ", err, metric.dim, metric.data)
					m.Restart() // reconnect
				}
			} else { // no session yet and drop the message
				glog.Debugln("dropping metric - buffer full ", metric.dim, metric.data)
			}
		case readMsg := <-m.readMsgs:
			var err error
			if readMsg.err == nil {
				err = m.readOneMsg(readMsg.msg)
				if err != nil {
					if !connectionPending {
						glog.Error("Error with parse ", err)
						m.Restart()
					}
				}
			} else {
				if !connectionPending {
					glog.Error("Error with read ", readMsg.err, len(m.doConnect))
					m.Restart()
				}
			}
		case <-time.After(time.Second * 300):
			glog.Info("No writes for 5 minutes")
			continue
		}
	}

}

// SendWithCb  will enqueue dim, data at time when; never blocks; err means msg dropped
// f is called when message is acked/nacked from Frontier.
func (m *FrontierClient) SendWithCb(dim map[string]string,
	data []FrontierData,
	when time.Time,
	resolution uint32,
	f frontierCb) error {
	select {
	case m.metrics <- frontierMessage{dim, data, when, resolution, f}:
	default:
		err := errors.New("frontier msg buffer full while calling API")
		glog.Debug(err, dim, data)
		return err
	}
	return nil
}

// Send will enqueue dim, data at time when; never blocks; err means msg dropped
func (m *FrontierClient) SendMetric(dim map[string]string,
	data []FrontierData,
	when time.Time) error {
	return m.SendWithCb(dim, data, when, ShrLockConfig.Resolution, nil)
}

// Send will enqueue dim, data at time when; never blocks; err means msg dropped
func (m *FrontierClient) Send(dim map[string]string,
	data []FrontierData,
	when time.Time,
	resolution uint32) error {
	return m.SendWithCb(dim, data, when, resolution, nil)
}

// SendBlocking will enqueue dim, data at time when; blocks on the channel
func (m *FrontierClient) SendBlocking(dim map[string]string,
	data []FrontierData,
	when time.Time,
	resolution uint32) {
	m.metrics <- frontierMessage{dim, data, when, resolution, nil}
}

// SendBlockingWithCB will enqueue dim, data at time when; blocks on the channel
// f is called when message is acked/nacked from Frontier.
func (m *FrontierClient) SendBlockingWithCB(dim map[string]string,
	data []FrontierData,
	when time.Time,
	resolution uint32,
	f frontierCb) {
	m.metrics <- frontierMessage{dim, data, when, resolution, f}
}

// GetChannelLenCap will return the channel len and capacity
// Do not count on this as a way to make SendBlocking not block
func (m *FrontierClient) GetChannelLenCap() (int, int) {
	m.rlock()
	defer m.runlock()
	return len(m.metrics), cap(m.metrics)
}

// Stop will stop all the goroutines associated with this
func (m *FrontierClient) Stop() {
	m.finished <- true
	m.finishedTO <- true
}
