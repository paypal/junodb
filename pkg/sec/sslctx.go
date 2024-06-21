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

package sec

import (
	"fmt"
	"net"
	"time"
)

type (
	Conn interface {
		GetStateString() string
		GetTLSVersion() string
		GetCipherName() string
		DidResume() string
		IsServer() bool
		Handshake() error
		GetNetConn() net.Conn
		IsTLS() bool
	}
	tlsContextI interface {
		newServerConn(conn net.Conn) (Conn, error)
		dial(target string, timeout time.Duration) (conn Conn, err error)
		//	loadSessions(sessions [][]byte)
	}
)

func NewServerConn(conn net.Conn) (sConn Conn, err error) {
	var ctx tlsContextI
	if ctx, err = getServerTlsContext(); err == nil {
		sConn, err = ctx.newServerConn(conn)
	}
	return
}

func Dial(target string, timeout time.Duration) (conn Conn, err error) {
	var ctx tlsContextI
	if ctx, err = getClientTlsContext(); err == nil {
		conn, err = ctx.dial(target, timeout)
	}
	return
}

func getServerTlsContext() (ctx tlsContextI, err error) {
	gRwCtxMtx.RLock()
	ctx = gSvrTlsCtx
	gRwCtxMtx.RUnlock()
	if ctx == nil {
		err = fmt.Errorf("nil server TLS context")
	}
	return
}

func getClientTlsContext() (ctx tlsContextI, err error) {
	gRwCtxMtx.RLock()
	ctx = gCliTlsCtx
	gRwCtxMtx.RUnlock()
	if ctx == nil {
		err = fmt.Errorf("nil client TLS context")
	}
	return
}
