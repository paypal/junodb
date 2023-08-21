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
	"crypto/x509"
	"os"
	"sync"
	"testing"
	"time"

	"juno/third_party/forked/golang/glog"
)

var (
	tlsConfig *tls.Config
	mutex     sync.RWMutex
)

func GetTLSConfig() *tls.Config {
	mutex.RLock()
	defer mutex.RUnlock()
	return tlsConfig
}

func loadCertificate() {
	caCert, err := os.ReadFile("./server.crt")
	if err != nil {
		glog.Exitf("%s", err.Error())
	}
	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}
	rootCAs.AppendCertsFromPEM(caCert)

	cert, err := tls.LoadX509KeyPair("./server.crt", "./server.pem")
	if err != nil {
		glog.Exitf("%s", err.Error())
	}

	mutex.Lock()
	defer mutex.Unlock()
	tlsConfig = &tls.Config{
		RootCAs:                rootCAs,
		Certificates:           []tls.Certificate{cert},
		InsecureSkipVerify:     true,
		SessionTicketsDisabled: false,
		ClientSessionCache:     tls.NewLRUClientSessionCache(0),
	}
}

func TestMain(m *testing.M) {

	glog.Infof("Start testing")

	loadCertificate()

	code := m.Run()
	glog.Finalize()
	time.Sleep(1 * time.Second)
	os.Exit(code)
}
