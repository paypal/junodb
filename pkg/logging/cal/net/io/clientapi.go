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
	"juno/pkg/logging/cal/net/protocol"
	"net"
	"sync"
	"sync/atomic"
)

// Client is a CAL client. It is used to configure and organize
// the connection to the CAL daemon. Host, port, and service name
// must be set prior to doing any logging and may not be changed
// afterwards.
type Client interface {
	CalHost() string
	SetCalHost(string)

	CalPort() int
	SetCalPort(int)

	LocalHost() string

	ServiceName() string
	SetServiceName(string)

	SetSendBufferSize(size int)

	// Send encodes and asynchronously (best effort) transmits m to
	// the CAL server.  Typically this method is not called directly.
	// Use a Logger instead.
	Send(m *protocol.CalMessage)

	Connect() error

	Shutdown()

	Flush()

	GetCalDropCount() uint64
}

type Connector struct {
	calHost string // cal daemon hostname
	calPort int    // cal daemon port
	writer  *bufio.Writer
	conn    net.Conn
}

type client struct {
	Client
	msgDrpCnt  uint64
	connector  *Connector
	clientInfo *protocol.ClientInfo
	initOnce   sync.Once
	closeOnce  sync.Once
	sendCh     chan *protocol.CalMessage // data to send
	wg         *sync.WaitGroup
	closeCh    chan struct{}
	threadId   int
	pid        int
}

func (c *client) CalHost() string { return c.connector.calHost }

func (c *client) CalPort() int { return c.connector.calPort }

func (c *client) SetCalHost(h string) { c.connector.calHost = h }

func (c *client) SetCalPort(p int) { c.connector.calPort = p }

func (c *client) ServiceName() string { return c.clientInfo.Service }

func (c *client) SetServiceName(s string) { c.clientInfo.Service = s }

func (c *client) SetSendBufferSize(size int) { c.sendCh = make(chan *protocol.CalMessage, size) }

func (c *client) LocalHost() string { return c.clientInfo.Hostname }

func (c *client) GetCalDropCount() uint64 { return atomic.LoadUint64(&c.msgDrpCnt) }
