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
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

func send(f *FrontierClient,
	doneCh chan bool,
	dims map[string]string,
	data []FrontierData) error {
	return f.SendWithCb(dims,
		data,
		time.Now(),
		60,
		func(e error) {
			if e != nil {
				fmt.Println("CB, dims, data, e", dims, data, e)
				send(f, doneCh, dims, data)
			}
			doneCh <- false // test will fail via timeout
		})
}

var txn int
var txnMutex = &sync.RWMutex{}

func sendSomeStuff(f *FrontierClient,
	doneCh chan bool) {

	hn, err := os.Hostname()
	if err != nil {
		hn = "unknown"
	}
	dims := map[string]string{
		"src_host":     hn,
		"service_type": "ES"}
	data := make([]FrontierData, 3)
	d := FrontierData{"CPU", Gauge, rand.Float64() * 100}
	data[0] = d
	d = FrontierData{"temp", Gauge, rand.Float64() * 100}
	data[1] = d
	txnMutex.Lock()
	d = FrontierData{"TXN", Counter, float64(txn)}
	data[2] = d
	txn = txn + int(math.Floor(rand.Float64()*1000000))
	txnMutex.Unlock()
	err = send(f, doneCh, dims, data)
}

func doTestFrontier(t *testing.T, done2Chan chan bool) {
	f, err := NewFrontierClient("sherlock-frontier-vip.qa.paypal.com",
		80,
		"pp", "qa",
		"PyInfra",
		"PythonInfraDev")
	if err != nil {
		t.Log("Can't connect to frontier", err)
		t.Fail()
		return
	}
	var dones = []chan bool{}
	for i := 0; i <= 100; i++ {
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
		doneChan := make(chan bool, 1)
		go sendSomeStuff(f, doneChan)
		rn := rand.Intn(100)
		if rn > 90 {
			f.lock()
			if f.wsConn.ws != nil {
				f.wsConn.close()
			}
			f.unlock()
		}
		rn = rand.Intn(100)
		if rn > 90 {
			f.lockSession()
			if len(f.session) > 0 {
				f.session[0] = 48
			}
			f.unlockSession()
		}
		dones = append(dones, doneChan)
	}
	for i := 0; i <= 100; i++ {
		r := <-dones[i]
		if r {
			fmt.Println("Status in done is", r)
			done2Chan <- true
			return
		}
	}
	fmt.Println("Finished sending 100")
	done2Chan <- false
}

func TestFrontierLots(t *testing.T) {
	setupFCConfig()
	PrintMsgs = false
	var dones = []chan bool{}
	for i := 0; i < 20; i++ {
		done2Chan := make(chan bool, 1)
		dones = append(dones, done2Chan)
		go doTestFrontier(t, done2Chan)
	}
	for i := 0; i < 20; i++ {
		r := <-dones[i]
		if r {
			fmt.Println("Status in done2 is", r)
			t.Fail()
		}
	}
}

func TestGetChannel(t *testing.T) {
	f, err := NewFrontierClient("sherlock-frontier-vip.qa.paypal.com",
		80,
		"pp", "qa",
		"PyInfra",
		"PythonInfraDev")
	if err != nil {
		t.Log("Can't connect to frontier", err)
		t.Fail()
		return
	}
	a, b := f.GetChannelLenCap()
	fmt.Println("Channel stats is", a, b)
}

func setupFCConfig() {
	ShrLockConfig = &Config{
		SherlockSvc:     "PyInfra",
		SherlockProfile: "PythonInfraDev",
		Enabled:         true,
		Resolution:      60,

		ClientType: "sherlock",
	}
}

func TestFCInterface(t *testing.T) {
	setupFCConfig()
	InitWithConfig(ShrLockConfig)

	dims := map[string]string{
		"src_host":     "testing",
		"service_type": "ES"}
	data := make([]FrontierData, 1)
	d := FrontierData{"CPU", Gauge, rand.Float64() * 100}
	data[0] = d

	time.Sleep(5 * time.Second)
	err := SherlockClient.SendMetric(dims, data, time.Now())
	if err != nil {
		glog.Errorln(err)
	}
	time.Sleep(5 * time.Second)
}
