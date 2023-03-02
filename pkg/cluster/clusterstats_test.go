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
package cluster

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

func sim_timeout(zoneid uint32, nodeid uint32, to_freq uint32,
	chQuit chan bool, cs *ClusterStats) {

	ticker := time.NewTicker(time.Duration(to_freq) * time.Millisecond)

	for {
		select {
		case <-chQuit:
			return

		case <-ticker.C:

			if !cs.IsMarkeddown(zoneid, nodeid) {
				ps := &ProcStat{
					zoneid:   zoneid,
					nodeid:   nodeid,
					timeout:  true,
					procTime: 100,
				}
				cs.SendNodeProcState(ps)
			}

		default:
		}
	}
}

func sim_response_time(zoneid uint32, nodeid uint32, send_freq uint32, proctime uint32,
	chQuit chan bool, cs *ClusterStats) {

	ticker := time.NewTicker(time.Duration(send_freq) * time.Millisecond)

	for {
		select {
		case <-chQuit:
			return

		case <-ticker.C:

			rn := uint32(rand.Intn(5))

			if !cs.IsMarkeddown(zoneid, nodeid) {
				ps := &ProcStat{
					zoneid:   zoneid,
					nodeid:   nodeid,
					timeout:  false,
					procTime: int32(proctime + rn),
				}
				cs.SendNodeProcState(ps)
			}

		default:
		}
	}
}

func TestTimeoutStats(t *testing.T) {
	var NumZones uint32 = 5
	var NumNodesPerZone uint32 = 10

	conf := &StatsConfig{
		TimeoutStatsEnabled:    true,
		RespTimeStatsEnabled:   true,
		MarkdownThreashold:     10,
		MarkdownExpirationBase: 20,
		EMARespTimeWindowSize:  39,
		TimeoutWindowSize:      5,
		TimeoutWindowUint:      10,
	}

	st := NewClusterStats(NumZones, NumNodesPerZone, conf)
	st.Run()

	chQuit := make(chan bool)
	// node 1-5, time out every 4 seconds
	// => 12.5 timeout every 50s window => markdown
	go sim_timeout(1, 5, 2000, chQuit, st)

	// node 2-4, timeout every 10 seconds
	// => 5 timeout every 50s window => no markdown
	go sim_timeout(2, 4, 3000, chQuit, st)

	for i := 0; i < 10; i++ {
		time.Sleep(10 * time.Second)
		st.PrintMarkDown()
	}

	close(chQuit)
	time.Sleep(1 * time.Second)
	st.Quit()
	fmt.Printf("done\n")
}

func TestVerySlowStats(t *testing.T) {
	var NumZones uint32 = 5
	var NumNodesPerZone uint32 = 10

	conf := &StatsConfig{
		TimeoutStatsEnabled:    true,
		RespTimeStatsEnabled:   true,
		MarkdownThreashold:     10,
		MarkdownExpirationBase: 20,
		EMARespTimeWindowSize:  39,
		TimeoutWindowSize:      5,
		TimeoutWindowUint:      10,
	}

	st := NewClusterStats(NumZones, NumNodesPerZone, conf)
	st.Run()

	chQuit := make(chan bool)
	// node 1-5, time out every 4 seconds
	// => 12.5 timeout every 50s window => markdown
	go sim_timeout(2, 5, 100, chQuit, st)

	for i := 0; i < 60; i++ {
		time.Sleep(5 * time.Second)
		//st.PrintMarkDown()
	}

	close(chQuit)
	time.Sleep(1 * time.Second)
	st.Quit()
	fmt.Printf("done\n")
}

func TestEMAStats(t *testing.T) {
	var NumZones uint32 = 5
	var NumNodesPerZone uint32 = 4

	conf := &StatsConfig{
		TimeoutStatsEnabled:    true,
		RespTimeStatsEnabled:   true,
		MarkdownThreashold:     10,
		MarkdownExpirationBase: 20,
		EMARespTimeWindowSize:  39,
		TimeoutWindowSize:      5,
		TimeoutWindowUint:      5,
	}

	st := NewClusterStats(NumZones, NumNodesPerZone, conf)
	st.Run()

	chQuit := make(chan bool)

	go sim_response_time(0, 2, 100, 200, chQuit, st)
	go sim_response_time(0, 2, 100, 20, chQuit, st)
	go sim_response_time(1, 0, 100, 50, chQuit, st)
	go sim_response_time(1, 1, 100, 150, chQuit, st)
	go sim_response_time(3, 3, 100, 550, chQuit, st)
	go sim_response_time(2, 3, 100, 150, chQuit, st)

	// node 2-4, timeout every 10 seconds
	// => 5 timeout every 50s window => no markdown
	go sim_timeout(2, 4, 3000, chQuit, st)

	for i := 0; i < 20; i++ {
		time.Sleep(2 * time.Second)
		st.PrintStats()
	}

	time.Sleep(3 * time.Second)
	st.PrintStats()
	st.Quit()
}
