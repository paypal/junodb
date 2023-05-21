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
	"io"
	"net"
	"os"
	"syscall"
	"time"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/paypal/junodb/pkg/logging"
	"github.com/paypal/junodb/pkg/logging/cal"
	"github.com/paypal/junodb/pkg/sec"
)

type SslListener struct {
	Listener
}

func (l *SslListener) AcceptAndServe() error {
	conn, err := l.netListener.Accept()

	if err != nil {
		return err
	}

	go func() {
		handshakeTimeout := l.ioConfig.HandshakeTimeout.Duration
		if handshakeTimeout == 0 {
			handshakeTimeout = 1 * time.Second
		}
		startTime := time.Now()

		conn.SetReadDeadline(startTime.Add(handshakeTimeout))
		if sslConn, err := sec.NewServerConn(conn); err == nil {

			if err = sslConn.Handshake(); err == nil {
				if cal.IsEnabled() {
					raddr := conn.RemoteAddr().String()
					if rhost, _, e := net.SplitHostPort(raddr); e == nil {
						b := logging.NewKVBuffer()
						b.Add([]byte("raddr"), raddr).
							Add([]byte("laddr"), conn.LocalAddr().String()).
							Add([]byte("ssl"), sslConn.GetStateString()).
							Add([]byte("et"), time.Since(startTime).String())
						cal.Event(cal.TxnTypeAccept, rhost, cal.StatusSuccess, b.Bytes())
					}
				}
				l.startNewConnector(sslConn.GetNetConn())
			} else {
				logAsWarning := true

				if opErr, ok := err.(*net.OpError); ok {
					if sErr, ok := opErr.Err.(*os.SyscallError); ok {
						if sErr.Err == syscall.ECONNRESET {
							logAsWarning = false
						}
					}
				} else if err == io.EOF {
					logAsWarning = false
				}

				if cal.IsEnabled() {
					raddr := conn.RemoteAddr().String()
					st := cal.StatusSuccess
					if logAsWarning {
						st = cal.StatusWarning
					}
					if rhost, _, e := net.SplitHostPort(raddr); e == nil {
						b := logging.NewKVBuffer()
						b.Add([]byte("raddr"), raddr).
							Add([]byte("laddr"), conn.LocalAddr().String()).
							Add([]byte("et"), time.Since(startTime).String()).
							Add([]byte("err"), "\"handshake error: "+err.Error()+"\"")
						cal.Event(cal.TxnTypeAccept, rhost, st, b.Bytes())
					}
				}
				if logAsWarning {
					glog.Warning("handshaking error: ", err)
				} else {
					glog.Debug("handshaking error: ", err)
				}
			}
		}
	}()

	return nil
}

func (l *SslListener) GetType() ListenerType {
	return ListenerTypeTCPwSSL
}

func (l *SslListener) Refresh() {

	var err error
	l.netListener, err = net.Listen(l.config.Network, l.config.Addr)
	if err != nil {
		glog.Error(err)
	}
}
