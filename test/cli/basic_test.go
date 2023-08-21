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
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"juno/pkg/client"
	"juno/third_party/forked/golang/glog"
)

var (
	server  *Cmd
	server2 *Cmd
)

func TestBasic(t *testing.T) {
	glog.Info("")
	glog.Info("=== TestBasic")

	server, _ = NewCmdWithConfig(serverAddr, 5)

	if server == nil {
		t.Errorf("Failed to init")
		return
	}
	sendRequests(server, 5, t)
}

func TestTls(t *testing.T) {
	glog.Info("")
	glog.Info("=== TestTls")

	server, _ = NewCmdWithConfig(serverTls, 5)
	if server == nil {
		t.Errorf("Failed to init")
		return
	}
	sendRequests(server, 5, t)
}

func sendRequests(server *Cmd, count int, t *testing.T) {
	for i := 0; i < count; i++ {
		if server.deleteKey(i) != nil {
			t.Errorf("Delete failed")
		}
		_, err := server.createKey(i)
		if err != nil {
			t.Errorf("Create failed")
		}
		_, err = server.updateKey(i)
		if err != nil {
			t.Errorf("Update failed")
		}
		if server.setKey(i) != nil {
			t.Errorf("Set failed")
		}
		if server.getKey(i) != nil {
			t.Errorf("Get failed")
		}
		if server.deleteKey(i) != nil {
			t.Errorf("Delete failed")
		}
	}
}

func TestConcurrent(t *testing.T) {
	glog.Info("")
	glog.Info("=== TestConcurrent")

	a := 20
	b := 1000
	total := a * b

	server, _ = NewCmdWithConfig(serverAddr, 5)
	if server == nil {
		t.Errorf("Failed to init")
		return
	}

	var wg sync.WaitGroup
	wg.Add(a)
	st := time.Now()

	failCount := int64(0)
	timeoutCount := int64(0)
	for i := 0; i < a; i++ {
		go func(k int) {
			for j := 0; j < b; j++ {
				if (j % 300) == 0 {
					glog.Infof("loop k=%d j=%d", k, j)
				}
				err := server.setKey(k*b + j)
				if err != nil {
					atomic.AddInt64(&failCount, 1)
					if errors.Is(err, client.ErrResponseTimeout) {
						atomic.AddInt64(&timeoutCount, 1)
					}
				}
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(st).Seconds()

	var wg2 sync.WaitGroup
	wg2.Add(a)
	for i := 0; i < a; i++ {
		go func(k int) {
			for j := 0; j < b; j++ {
				server.deleteKey(k*b + j)
			}
			wg2.Done()
		}(i)
	}
	wg2.Wait()

	glog.Infof("elapsed=%.1fs fails=%d timeouts=%d total=%d", elapsed,
		failCount, timeoutCount, total)
	if failCount > int64(total/10) {
		t.Errorf("Concurrent test failed")
	}
}
