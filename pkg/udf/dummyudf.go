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
package udf

// dummy udfs for testing

// Hello udf
type HelloUDF struct{}

func (u *HelloUDF) Call(key []byte, value []byte, params []byte) (res []byte, err error) {
	res = make([]byte, len("hello world"))
	copy(res, "hello world")
	return res, nil
}

func (u *HelloUDF) GetVersion() uint32 {
	return 1
}

func (u *HelloUDF) GetName() string {
	return "hello"
}

// Echo udf
type EchoUDF struct{}

func (u *EchoUDF) Call(key []byte, value []byte, params []byte) (res []byte, err error) {
	res = make([]byte, len(value))
	copy(res, value)
	return res, nil
}

func (u *EchoUDF) GetVersion() uint32 {
	return 1
}

func (u *EchoUDF) GetName() string {
	return "echo"
}

// Register built-in UDFs
func registerDummyUDFs(um *UDFMap) {
	(*um)["hello"] = &HelloUDF{}
	(*um)["echo"] = &EchoUDF{}
}
