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

import (
	"bytes"
	"fmt"
	"io"

	uuid "github.com/satori/go.uuid"
)

type (
	IRequestId interface {
		Bytes() []byte
		String() string
		PrettyPrint(w io.Writer)
	}
	RequestId [16]byte
)

var NilRequestId = RequestId{}

func (rid RequestId) String() string {
	return RequestIdTextFromBytes(rid[:])
}

func (rid RequestId) Bytes() []byte {
	return rid[:]
}

func (rid *RequestId) SetFromBytes(b []byte) error {
	if len(b) != 16 { ///TODO
		return fmt.Errorf("not valid request id: %v", b)
	}
	copy((*rid)[:], b)
	return nil
}

func (rid *RequestId) SetFromString(str string) error {
	if prid, err := uuid.FromString(str); err == nil {
		*rid = RequestId(prid)
	} else {
		return err
	}
	return nil
}

func (rid *RequestId) SetNewRequestId() {
	uuid := uuid.NewV1()
	copy((*rid)[:], uuid.Bytes())
}

func (rid RequestId) IsNotNil() bool {
	if rid.Equal(NilRequestId) {
		return false
	}
	return true
}

func (rid RequestId) IsSet() bool {
	if rid.Equal(NilRequestId) {
		return false
	}
	return true
}
func (rid RequestId) PrettyPrint(w io.Writer) {
	var uid uuid.UUID
	copy(uid[:], rid[:])
	fmt.Fprintf(w, "  %d\n", uid.Version())
}

func (rid RequestId) Equal(id RequestId) bool {
	return bytes.Equal(rid[:], id[:])
}

func Equal(rid1 RequestId, rid2 RequestId) bool {
	return bytes.Equal(rid1[:], rid2[:])
}

func RequestIdTextFromBytes(b []byte) string {
	var id uuid.UUID
	err := id.UnmarshalBinary(b)
	if err != nil {
		return "Invalid request ID"
	}
	return id.String()
}
