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
	"encoding/binary"
	"errors"
)

type CounterUDF struct{}

func (u *CounterUDF) Call(key []byte, value []byte, params []byte) (res []byte, err error) {
	if len(value) != 4 || len(params) != 4 {
		return nil, errors.New("Bad Param")
	}
	var counter uint32 = binary.BigEndian.Uint32(value)
	var delta uint32 = binary.BigEndian.Uint32(params)
	counter += delta
	res = make([]byte, 4)
	binary.BigEndian.PutUint32(res, counter)
	return res, nil
}

// not all interface functions are implemented
//func (u *CounterUDF) GetVersion() uint32 {
//	return 1
//}

//func (u *CounterUDF) GetName() string {
//	return "counter"
//}

func GetUDFInterface() (f interface{}, err error) {
	f = &CounterUDF{}
	return
}
