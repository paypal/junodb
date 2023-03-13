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

/*
package client implements Juno client API.

possible returned error if client successfully received the response from Proxy

  Create
  * nil
  * ErrBadMsg
  * ErrBadParam
  * ErrInternal
  * ErrBusy
  * ErrNoStorage
  * ErrUniqueKeyViolation
  * ErrRecordLocked
  * ErrWriteFailure

  Get
  * nil
  * ErrBadMsg
  * ErrBadParam
  * ErrInternal
  * ErrBusy
  * ErrNoStorage
  * ErrNoKey
  * ErrTTLExtendFailure

  Update
  * nil
  * ErrBadMsg
  * ErrBadParam
  * ErrInternal
  * ErrBusy
  * ErrNoStorage
  * ErrRecordLocked
  * ErrConditionViolation
  * ErrWriteFailure

  Set
  * nil
  * ErrBadMsg
  * ErrBadParam
  * ErrInternal
  * ErrBusy
  * ErrNoStorage
  * ErrRecordLocked
  * ErrWriteFailure

  Destroy
  * nil
  * ErrBadMsg
  * ErrBadParam
  * ErrInternal
  * ErrBusy
  * ErrNoStorage
  * ErrRecordLocked
  * ErrWriteFailure

*/
package client

import (
	"io"
)

type IContext interface {
	GetVersion() uint32
	GetCreationTime() uint32
	GetTimeToLive() uint32
	PrettyPrint(w io.Writer)
}

///TODO check API input arguments

type IClient interface {
	Create(key []byte, value []byte, opts ...IOption) (IContext, error)
	Get(key []byte, opts ...IOption) ([]byte, IContext, error)
	Update(key []byte, value []byte, opts ...IOption) (IContext, error)
	Set(key []byte, value []byte, opts ...IOption) (IContext, error)
	Destroy(key []byte, opts ...IOption) (err error)
	UDFGet(key []byte, fname []byte, params []byte, opts ...IOption) ([]byte, IContext, error)
	UDFSet(key []byte, fname []byte, params []byte, opts ...IOption) (IContext, error)
}

//type IResult interface {
//	Get()
//	GetWithTimeout()
//	Poll()
//}
//type IValueResult interface {
//	IResult
//}
//
//type AsyncClient interface {
//	Create(key []byte, value []byte, opts ...IOption) IResult
//	Update(key []byte, value []byte, opts ...IOption) IResult
//	Set(key []byte, value []byte, opts ...IOption) IResult
//	Get(key []byte, value []byte, opts ...IOption) IResult
//	Destroy(key []byte) IResult
//}
