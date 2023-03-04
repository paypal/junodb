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
package errors

import (
	"fmt"
)

var (
	ErrNoConnection = &Error{what: "no connection", errno: KErrNoConnection}
	ErrBusy         = &Error{what: "busy", errno: KErrBusy}
)

type Error struct {
	what  string
	errno uint32
}

func NewError(what string, errno uint32) *Error {
	return &Error{what: what, errno: errno}
}

func (e *Error) Error() string {
	return fmt.Sprintf("error: %s (%d) ", e.what, e.errno)
}

func (e *Error) ErrNo() uint32 {
	return e.errno
}
