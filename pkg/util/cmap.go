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

package util

import (
	"sync"

	"github.com/paypal/junodb/third_party/forked/golang/glog"
	"github.com/spaolacci/murmur3"
)

type MapPartition struct {
	sync.RWMutex
	data map[string]interface{}
}

type CMap struct {
	partitions     []*MapPartition
	paritionsCount uint32
}

func NewCMap(paritionsCount uint32) *CMap {
	m := new(CMap)
	m.paritionsCount = paritionsCount
	m.partitions = make([]*MapPartition, paritionsCount)
	for i := 0; i < int(paritionsCount); i++ {
		m.partitions[i] = &MapPartition{data: make(map[string]interface{})}
	}
	return m
}

func (m *CMap) getPartition(key string) *MapPartition {
	partitionNo := murmur3.Sum32([]byte(key)) % uint32(m.paritionsCount)
	return m.partitions[partitionNo]
}

func (m *CMap) Put(key []byte, value interface{}) {
	keyStr := string(key)
	glog.Verbosef("CMAP Put >> key:%X", key)
	partition := m.getPartition(keyStr)
	partition.Lock()
	partition.data[keyStr] = value
	partition.Unlock()
}

func (m *CMap) Get(key []byte) (interface{}, bool) {
	keyStr := string(key)
	partition := m.getPartition(keyStr)
	partition.RLock()
	val, present := partition.data[keyStr]
	partition.RUnlock()
	glog.Verbosef("CMAP Get >> key:%X", key)
	return val, present
}

func (m *CMap) Delete(key []byte) {
	keyStr := string(key)
	partition := m.getPartition(keyStr)
	partition.Lock()
	delete(partition.data, keyStr)
	partition.Unlock()
	glog.Verbosef("CMAP Delete >> key:%X", key)
}

func (m *CMap) PutIfAbsent(key []byte, value interface{}) (interface{}, bool) {
	keyStr := string(key)
	glog.Verbosef("CMAP PutIfAbsent >> key:%X", key)
	partition := m.getPartition(keyStr)
	partition.Lock() //can't use read lock and upgrade atomically
	curValue, present := partition.data[keyStr]
	if !present {
		partition.data[keyStr] = value
	}
	partition.Unlock()
	//fmt.Println("FMT - Value returned =>", curValue) //TODO: Test with IO write
	return curValue, !present
}

/* Testing and logging purpose only. Don't dump in production as it has overhead and file IO will take more time. */
func (m *CMap) PrintAll(bucketId uint32, dataType string) {
	for i := 0; i < int(m.paritionsCount); i++ {
		m.partitions[i].Lock()
		for key, value := range m.partitions[i].data {
			if glog.LOG_VERBOSE {
				if value != nil {
					glog.Verbosef("updata: %d %d, %X", bucketId, i, key)
				}
			}
		}
		m.partitions[i].Unlock()
	}
}
