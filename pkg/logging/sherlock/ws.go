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

package sherlock

import (
	"fmt"
	"juno/third_party/forked/golang/glog"
	"net"
	"sync"
	"time"

	proto "github.com/golang/protobuf/proto"
	"golang.org/x/net/websocket"
)

type wsConn struct {
	host    string
	port    int
	ws      *websocket.Conn
	wsMutex *sync.RWMutex
}

func newWs(host string, port int) wsConn {
	return wsConn{host, port, nil, &sync.RWMutex{}}
}

func (m *wsConn) connect(host string, port int) error {
	m.wlock()
	defer m.wunlock()
	origin := "http://localhost/"
	url := fmt.Sprintf("ws://%s:%d/api/v1", host, port)

	cfg, er := websocket.NewConfig(url, origin)
	if er != nil {
		glog.Infoln("Error while creating websocket config ", er)
		return er
	}

	cfg.Dialer = &net.Dialer{Timeout: ShrLockConfig.ConnectTimeout, Deadline: time.Now().Add(2 * time.Second)}
	ws, err := websocket.DialConfig(cfg)
	if err != nil {
		glog.Infof("Error while connecting to sherlock %s", err)
		m.ws = nil
	} else {
		glog.Infof("Connected to %s", url)
		m.ws = ws
	}
	return err
}

func (m wsConn) wlock() {
	m.wsMutex.RLock()
}

func (m wsConn) wunlock() {
	m.wsMutex.RUnlock()
}

func (m *wsConn) writeMsg(id uint32, env proto.Message) error {
	var bytes []byte
	if m.ws == nil {
		return fmt.Errorf("Writing with no ws")
	}
	bytes, err := proto.Marshal(env)
	if err != nil {
		return err
	}

	m.wlock()
	m.ws.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err = websocket.Message.Send(m.ws, bytes)
	m.wunlock()
	if err == nil && PrintMsgs {
		fmt.Println("Sent:",
			time.Now().Format(time.RFC850),
			id,
			env)
	}
	return err
}

func (m wsConn) readMsg() (buf []byte, err error) {
	m.wlock()
	if m.ws != nil {
		m.ws.SetReadDeadline(time.Now().Add(5 * time.Second))
		err = websocket.Message.Receive(m.ws, &buf)
	} else {
		err = fmt.Errorf("No ws on read")
	}
	m.wunlock()
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (m *wsConn) close() error {
	var r error
	m.wlock()
	if m.ws != nil {
		r = m.ws.Close()
		m.ws = nil
	}
	m.wunlock()
	return r
}
