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
	"errors"
	"fmt"
	"net"
	"time"

	"juno/pkg/logging"
	"juno/pkg/logging/cal"
)

func TLSInitialized() bool {
	return false
}

func Dial(addr string, timeout time.Duration, getTLSConfig func() *tls.Config) (conn net.Conn, err error) {
	var tlsConn *tls.Conn

	if getTLSConfig == nil {
		return nil, errors.New("Unable to get TLS config")
	}
	timeStart := time.Now()
	tlsCfg := getTLSConfig()
	if tlsCfg == nil {
		err = errors.New("Unable to get TLS config")
	} else {
		dialer := &net.Dialer{Timeout: timeout}
		tlsConn, err = tls.DialWithDialer(dialer, "tcp", addr, tlsCfg)
		conn = tlsConn
		if tlsConn == nil && err == nil {
			err = errors.New("Connect failed.")
		}
	}

	if !cal.IsEnabled() {
		return conn, err
	}

	// Cal logging
	status := cal.StatusSuccess
	b := logging.NewKVBuffer()
	if err != nil {
		status = cal.StatusError
		b.Add([]byte("err"), err.Error())
	} else {
		b.Add([]byte("ssl"), getConnectionState(tlsConn))
	}

	cal.AtomicTransaction(cal.TxnTypeConnect, addr, status,
		time.Since(timeStart), b.Bytes())

	return conn, err
}

func getConnectionState(c *tls.Conn) string {
	if c == nil {
		return ""
	}

	st := c.ConnectionState()
	rid := 0
	if st.DidResume {
		rid = 1
	}
	msg := fmt.Sprintf("GoTLS:%s:%s:ssl_r=%d", getVersionName(st.Version),
		tls.CipherSuiteName(st.CipherSuite), rid)

	return msg
}

func getVersionName(ver uint16) string {
	switch ver {
	case tls.VersionSSL30:
		return "SSLv3"
	case tls.VersionTLS10:
		return "TLSv1"
	case tls.VersionTLS11:
		return "TLSv1.1"
	case tls.VersionTLS12:
		return "TLSv1.2"
	case tls.VersionTLS13:
		return "TLSv1.3"
	default:
		return ""
	}
}
