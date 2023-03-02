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
// +build debug

package db

import (
	"fmt"
	"io"
	"sync"
	"unsafe"

	"juno/third_party/forked/tecbot/gorocksdb"
)

var (
	gorocksdbSliceTracker sync.Map
)

func onAllocValue(i interface{}) {
	t := unsafe.Pointer((i.(*gorocksdb.Slice)))
	if _, loaded := gorocksdbSliceTracker.LoadOrStore(t, true); loaded {
		panic("")
	}
}

func onFreeValue(i interface{}) {
	t := unsafe.Pointer((i.(*gorocksdb.Slice)))
	gorocksdbSliceTracker.Delete(t)
}

func WriteSliceTrackerStats(w io.Writer) {
	num := 0

	gorocksdbSliceTracker.Range(func(key, value interface{}) bool {
		num++
		return true
	})
	fmt.Fprintf(w, "number of gorocksdb.Slice not being freed yet: %d", num)
}
