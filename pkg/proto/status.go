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

package proto

const (
	StatusOk          = 0
	StatusNoConn      = 1
	StatusCommErr     = 2
	StatusTimeout     = 3
	StatusBadRequest  = 4
	StatusBadResponse = 5
	StatusNoCapacity  = 6
	StatusSSBusy      = 7
	StatusRBCleanup   = 8
	StatusRBExpire    = 9
)

var statusText = map[int]string{
	StatusOk:          "OK",
	StatusNoConn:      "not connected",
	StatusCommErr:     "communication error",
	StatusTimeout:     "timed out",
	StatusBadRequest:  "bad request",
	StatusBadResponse: "bad response",
	StatusNoCapacity:  "no capacity",
	StatusSSBusy:      "SS busy",
	StatusRBCleanup:   "RB cleanup", // target connection closed
	StatusRBExpire:    "RB expire",  // item in the rb expired
}

func StatusText(code int) string {
	return statusText[code]
}
