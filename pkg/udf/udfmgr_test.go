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
  
package udf

import (
	"encoding/binary"
	"juno/third_party/forked/golang/glog"

	"fmt"
	"testing"
)

func TestBuiltinCounter(t *testing.T) {

	mgr, _ := NewUDFManager("")
	udf := mgr.GetUDF("sc")
	if udf != nil {
		key1 := "k1"
		value1 := make([]byte, 4)
		delta := make([]byte, 4)
		binary.BigEndian.PutUint32(value1, 5)
		binary.BigEndian.PutUint32(delta, 4)

		r, _ := udf.Call([]byte(key1), value1, delta)
		newcount := binary.BigEndian.Uint32(r)
		if newcount != 9 {
			t.Errorf("wrong count")
		}
	} else {
		t.Errorf("can't find the counter udf plugin")
	}
	glog.Flush()
}
func TestCounterPlugin(t *testing.T) {

	mgr, _ := NewUDFManager("./example_plugins/counter")
	udf := mgr.GetUDF("counter")
	if udf != nil {
		key1 := "k1"
		value1 := make([]byte, 4)
		delta := make([]byte, 4)
		binary.BigEndian.PutUint32(value1, 5)
		binary.BigEndian.PutUint32(delta, 1)

		r, _ := udf.Call([]byte(key1), value1, delta)
		newcount := binary.BigEndian.Uint32(r)
		if newcount != 6 {
			t.Errorf("wrong count")
		}
	} else {
		t.Errorf("can't find the counter udf plugin")
	}
	glog.Flush()
}

func TestBadCounterPlugin(t *testing.T) {
	mgr, _ := NewUDFManager("./example_plugins/bad_plugin")
	udf := mgr.GetUDF("bad_plugin")
	if udf != nil {
		t.Error("should not get here")
	} else {
		fmt.Printf("can't find the udf plugin \n")
	}
	glog.Flush()
}
