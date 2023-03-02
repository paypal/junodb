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
//  Package utility provides the utility interfaces for mux package
//  
package io

import (
	"sync"
	"time"

	"juno/third_party/forked/golang/glog"
)

type InboundConnManager struct {
	mtx         sync.Mutex
	activeConns map[*Connector]struct{}
	wg          sync.WaitGroup
}

func (m *InboundConnManager) TrackConn(c *Connector, add bool) {
	m.mtx.Lock()
	if m.activeConns == nil {
		m.activeConns = make(map[*Connector]struct{})
	}

	if add {
		m.activeConns[c] = struct{}{}
		m.wg.Add(1)
		if glog.LOG_VERBOSE {
			glog.Verbosef("add active conns: %d", len(m.activeConns))
		}
	} else {
		delete(m.activeConns, c)
		m.wg.Done()
		if glog.LOG_VERBOSE {
			glog.Verbosef("remove active conns: %d", len(m.activeConns))
		}
	}
	m.mtx.Unlock()
}

func (m *InboundConnManager) Shutdown() {
	m.mtx.Lock()
	for connector := range m.activeConns {
		connector.Stop()
	}
	m.mtx.Unlock()
}

func (m *InboundConnManager) WaitForShutdownToComplete(timeout time.Duration) {
	done := make(chan bool)
	go func() {
		m.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(timeout):
	}
}

// thread safe?
func (m *InboundConnManager) GetNumActiveConnections() uint32 {
	return uint32(len(m.activeConns))
}
