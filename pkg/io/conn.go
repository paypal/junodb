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
	"net"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/logging/otel"
	"juno/pkg/sec"
)

type Conn interface {
	GetStateString() string
	GetNetConn() net.Conn
	IsTLS() bool
}

type Connection struct {
	conn net.Conn
}

const (
	// See pkg/sec/tls/handshak_client.go for this string
	downGraded = "tls: downgrade attempt detected, possibly due to a MitM attack or a broken middlebox"
)

func (c *Connection) GetStateString() string {
	return ""
}

func (c *Connection) GetNetConn() net.Conn {
	return c.conn
}

func (c *Connection) IsTLS() bool {
	return false
}

func Connect(endpoint *ServiceEndpoint, connectTimeout time.Duration) (conn net.Conn, err error) {
	timeStart := time.Now()

	if endpoint.SSLEnabled {
		var sslconn sec.Conn

		if sslconn, err = sec.Dial(endpoint.Addr, connectTimeout); err == nil {
			conn = sslconn.GetNetConn()
			if glog.LOG_DEBUG {
				glog.DebugDepth(1, fmt.Sprintf("connected to %s ssl=%s", endpoint.GetConnString(), sslconn.GetStateString()))
			}
		} else {
			glog.ErrorDepth(1, fmt.Sprintf("fail to connect %s error: %s", endpoint.GetConnString(), err))
			// if err.Error() == downGraded {
			// 	sec.ResetSessionIdForClient()
			// }
		}

		if cal.IsEnabled() {
			status := cal.StatusSuccess
			b := logging.NewKVBuffer()
			if err != nil {
				status = cal.StatusError
				b.Add([]byte("err"), err.Error())
			} else {
				b.Add([]byte("ssl"), sslconn.GetStateString())
			}

			if !otel.IsEnabled() || status != cal.StatusSuccess {
				cal.AtomicTransaction(cal.TxnTypeConnect, endpoint.Addr, status, time.Since(timeStart), b.Bytes())
			}
		}
		if otel.IsEnabled() {
			status := otel.StatusSuccess
			if err != nil {
				status = otel.StatusError
			}
			otel.RecordOutboundConnection(endpoint.Addr, status, time.Since(timeStart).Milliseconds())
		}
	} else {
		if conn, err = net.DialTimeout("tcp", endpoint.Addr, connectTimeout); err == nil {
			if glog.LOG_DEBUG {
				glog.DebugDepth(1, fmt.Sprintf("connected to %s", endpoint.GetConnString()))
			}
		} else {
			glog.ErrorDepth(1, fmt.Sprintf("fail to connect %s error: %s", endpoint.GetConnString(), err.Error()))
		}
		if cal.IsEnabled() {
			status := cal.StatusSuccess
			var data []byte
			if err != nil {
				status = cal.StatusError
				data = []byte(err.Error())
			}
			cal.AtomicTransaction(cal.TxnTypeConnect, endpoint.GetConnString(), status, time.Since(timeStart), data)
		}
		if otel.IsEnabled() {
			status := otel.StatusSuccess
			if err != nil {
				status = otel.StatusError
			}
			otel.RecordOutboundConnection(endpoint.GetConnString(), status, time.Since(timeStart).Milliseconds())
		}
	}

	return
}

func ConnectTo(endpoint *ServiceEndpoint, connectTimeout time.Duration) (conn Conn, err error) {
	if endpoint.SSLEnabled {
		var sslconn sec.Conn

		if sslconn, err = sec.Dial(endpoint.Addr, connectTimeout); err == nil {
			conn = sslconn
			if glog.LOG_DEBUG {
				glog.DebugDepth(1, fmt.Sprintf("connected to %s ssl=%s", endpoint.GetConnString(), sslconn.GetStateString()))
			}
		} else {
			glog.ErrorDepth(1, fmt.Sprintf("fail to connect %s error: %s", endpoint.GetConnString(), err))
			// if err.Error() == downGraded {
			// 	sec.ResetSessionIdForClient()
			// }
		}
	} else {
		var connection Connection
		if connection.conn, err = net.DialTimeout("tcp", endpoint.Addr, connectTimeout); err == nil {
			conn = &connection
			if glog.LOG_DEBUG {
				glog.DebugDepth(1, fmt.Sprintf("connected to %s", endpoint.GetConnString()))
			}
		} else {
			glog.ErrorDepth(1, fmt.Sprintf("fail to connect %s error: %s", endpoint.GetConnString(), err.Error()))
		}
	}

	return
}
