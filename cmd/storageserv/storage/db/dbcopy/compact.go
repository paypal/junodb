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
package main

import (
	"bytes"
	"juno/third_party/forked/golang/glog"
)

type CompactionFilter struct {
}

var (
	matchCount    = uint64(0)
	namespaceList [][]byte
	keep          bool
	shardidFilter []byte
)

func InitCompact(ns []string, b bool) {
	namespaceList = make([][]byte, len(ns))
	for i := range ns {
		namespaceList[i] = []byte(ns[i])
	}

	keep = b
	if len(ns) == 0 {
		keep = true
	} else {
		glog.Infof("ns=%v", ns)
	}
}

func SetShardidFilter(val []byte) {
	shardidFilter = val
}

func GetMatchCount() uint64 {
	return matchCount
}

func matchNamespace(key []byte, forCompact bool) bool {

	const offNamespace = 4
	if len(namespaceList) == 0 || len(key) < offNamespace+2 {
		return false
	}
	// key[offNamespace:stop] stores namespace
	stop := offNamespace + uint8(key[offNamespace-1])
	if len(key) < int(stop) {
		return false
	}

	if shardidFilter != nil &&
		bytes.Compare(key[0:2], shardidFilter) != 0 {
		return false
	}

	for i := range namespaceList {
		if bytes.Compare(key[offNamespace:stop], namespaceList[i]) != 0 {
			continue
		}

		matchCount++
		if forCompact && matchCount%100000 == 0 {
			glog.Infof("ns_keys=%d", matchCount)
		}

		if keep {
			return false
		}
		return true
	}

	return false
}

func (m *CompactionFilter) Filter(level int, key, val []byte) (exp bool,
	newVal []byte) {

	if matchNamespace(key, true) {
		return true, nil // remove key
	}

	return false, nil
}

func (m *CompactionFilter) Name() string {
	return "CompactionFilter"
}
