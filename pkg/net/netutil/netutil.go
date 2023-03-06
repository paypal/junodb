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
  
package netutil

import (
	"net"

	"juno/third_party/forked/golang/glog"
)

var (
	localIPMap       map[string]bool = make(map[string]bool)
	localIPv4Address net.IP
)

func init() {
	if addrs, err := net.InterfaceAddrs(); err == nil {

		for _, a := range addrs {
			if ipnet, ok := a.(*net.IPNet); ok {
				if localIPv4Address == nil {
					if !ipnet.IP.IsLoopback() {
						localIPv4Address = ipnet.IP.To4()
					}
				}
				localIPMap[ipnet.IP.String()] = true
			}
		}
	} else {
		glog.Warningln(err)
	}
	if localIPv4Address == nil {
		localIPv4Address = net.ParseIP("127.0.0.1").To4()
	}
}

func IsLocalAddress(addr string) bool {
	if net.ParseIP(addr) != nil {
		return IsLocalIPAddress(addr)
	}

	if ips, err := net.LookupIP(addr); err == nil {
		for _, ip := range ips {
			if IsLocalIPAddress(ip.String()) {
				return true
			}
		}
	}
	return false
}

func IsLocalIPAddress(ipAddr string) bool {
	if _, found := localIPMap[ipAddr]; found {
		return true
	}
	return false
}

func GetLocalIPv4Address() net.IP {
	return localIPv4Address
}
