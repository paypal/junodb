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
package util

import (
	"fmt"
	"hash/fnv"
	"juno/pkg/logging/cal/net/protocol"
	"net"
	"os"
	"path"
	"runtime"
	"strconv"
	"time"
)

const (
	defaultPort       int    = 1118
	defaultBufferSize int    = 100
	poolName          string = "defaultgopool"
)

// autoType generates a CAL type field by spitting out the filename+lineno
// of the caller. Maybe good enough for now, until we come up with a better idea?
// skip indicates how many layers there are between the caller and the log call.
// Setting explicitly is much cheaper than autodetecting.
func autoType(skip int) string {
	_, fname, lineno, ok := runtime.Caller(skip + 1) // +1 is for autoType itself
	if !ok {
		return ""
	}
	_, file := path.Split(fname)
	return protocol.CleanNamespace(fmt.Sprintf("%s-%d", file, lineno))
}

// generateCorrId generates a correlation id.
func generateCorrId(hostname string, pid int) string {
	now := time.Now()
	// Python uses microseconds; follow their example, even though nanoseconds seems better.
	micro := int64(now.Nanosecond() / 1000)
	raw := hostname + strconv.Itoa(pid) + strconv.FormatInt(now.Unix(), 10) + strconv.FormatInt(micro, 10)
	h := fnv.New64()
	h.Write([]byte(raw))
	sum := h.Sum64()
	return strconv.FormatUint(sum, 16) + strconv.FormatInt(micro, 16)
}

// hostname returns the best available hostname.
// It tries in order: proper hostname, non-degenerate
// ip address, "localhost".
func Hostname() string {
	if hostname, err := os.Hostname(); err == nil {
		return hostname
	}
	// Try to find a non-degenerate ip address
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, addr := range addrs {
			ipnet, ok := addr.(*net.IPNet)
			if !ok {
				continue
			}
			ip := ipnet.IP
			if ip.IsUnspecified() || ip.IsLoopback() || ip.IsMulticast() {
				continue
			}
			return ip.String()
		}
	}
	return "localhost"
}

func CalHostname(hostname string) string {
	if hostname == "" {
		return "127.0.0.1"
	}
	return hostname
}

func CalPort(port int) int {
	if port == 0 {
		return defaultPort
	}
	return port
}

func BufferChan(bSize int) chan *protocol.CalMessage {
	if bSize == 0 {
		return make(chan *protocol.CalMessage, defaultBufferSize)
	}
	return make(chan *protocol.CalMessage, bSize)
}

func Poolname(pName string) string {
	if pName == "" {
		return poolName
	}
	return pName
}
