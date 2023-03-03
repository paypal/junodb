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
package qry

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"

	"juno/pkg/cluster"
)

var (
	infoQueryFuncMap      = map[string]func(w http.ResponseWriter, v url.Values){}
	kQryCmdSsConnected    = "ss_connected"
	kQryCmdSsNotConnected = "ss_not_connected"
	kQryCmdSsGetPid       = "get_pid"
	kQryCmdBadShardHosts  = "ss_bad_shard_hosts"
)

func InfoQuery(w http.ResponseWriter, values url.Values) {
	keys, ok := values["info"]
	if ok && len(keys) != 0 {
		cmd := keys[0]
		f, ok := infoQueryFuncMap[cmd]
		if ok {
			f(w, values)
		}
	}
}

func getPid(w http.ResponseWriter, values url.Values) {
	fmt.Fprintf(w, "%v", os.Getpid())
}

func querySsConnected(w http.ResponseWriter, values url.Values) {
	connected := true
	if cluster.GetShardMgr() != nil {

		nodes, found := values["node"]
		if found {
			for _, n := range nodes {
				v := strings.Split(n, ",")
				if len(v) >= 2 {

					zone, err := strconv.Atoi(v[0])
					if err == nil {
						if zoneIndex, err := strconv.Atoi(v[1]); err == nil {
							if cluster.GetShardMgr().IsConnected(zone, zoneIndex) == false {
								connected = false
								break
							}
						}
					}
				}
			}
		} else {
			connStates := cluster.GetShardMgr().GetConnectivity()
		loop:
			for i := range connStates {
				for v := range connStates[i] {
					if connStates[i][v] == 0 {
						connected = false
						break loop
					}
				}
			}
		}
	}
	fmt.Fprintf(w, "%v", connected)
}

func querySsNotConnected(w http.ResponseWriter, values url.Values) {
	notconnected := true
	if cluster.GetShardMgr() != nil {

		nodes, found := values["node"]
		if found {
			for _, n := range nodes {
				v := strings.Split(n, ",")
				if len(v) >= 2 {

					zone, err := strconv.Atoi(v[0])
					if err == nil {
						if zoneIndex, err := strconv.Atoi(v[1]); err == nil {
							if cluster.GetShardMgr().IsConnected(zone, zoneIndex) == true {
								notconnected = false
								break
							}
						}
					}
				}
			}
		} else {
			connStates := cluster.GetShardMgr().GetConnectivity()
		loop:
			for i := range connStates {
				for v := range connStates[i] {
					if connStates[i][v] == 1 {
						notconnected = false
						break loop
					}
				}
			}
		}
	}
	fmt.Fprintf(w, "%v", notconnected)
}

func queryBadShardHosts(w http.ResponseWriter, values url.Values) {
	mgr := cluster.GetShardMgr()
	if mgr != nil {
		v, found := values["status"]
		c := uint32(0)
		if found {
			switch v[0] {
			case "warning":
				c = 2
			case "alert":
				c = 1
			case "fatal":
			default:
				c = 0
			}
		}
		hosts := mgr.GetBadShardHosts(c)
		fmt.Fprintf(w, "%v", hosts)
	} else {
		fmt.Fprintf(w, "%v", "Unable to get cluster manager")
	}
}

func init() {
	infoQueryFuncMap[kQryCmdSsGetPid] = getPid
	infoQueryFuncMap[kQryCmdSsConnected] = querySsConnected
	infoQueryFuncMap[kQryCmdSsNotConnected] = querySsNotConnected
	infoQueryFuncMap[kQryCmdBadShardHosts] = queryBadShardHosts
}
