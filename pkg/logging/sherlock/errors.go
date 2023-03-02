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
package sherlock

import "fmt"

// TimeoutError returned for Frontier timeout
// other errors are from websocket
type TimeoutError struct {
	id uint32
}

func newTimeoutError(id uint32) *TimeoutError {
	return &TimeoutError{id: id}
}

func (t *TimeoutError) Error() string {
	if t == nil {
		return "Timed out on Frontier send"
	}
	return fmt.Sprintf("Timed out on Frontier send %d", t.id)

}

// RejectedError returned for Frontier rejection
// not currently distinguishing fatal and retryable
type RejectedError struct {
	msg string
}

func newRejectedError(msg string) *RejectedError {
	return &RejectedError{msg: msg}
}

func (t *RejectedError) Error() string {
	if t == nil {
		return "Rejected on Frontier send"
	}
	return fmt.Sprintf("Rejected on Frontier send %s", t.msg)
}

type frontierCb func(e error)
