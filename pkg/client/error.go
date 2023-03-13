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

package client

import (
	"juno/internal/cli"
	"juno/pkg/proto"
)

var (
	ErrNoKey              error
	ErrUniqueKeyViolation error
	ErrBadParam           error
	ErrConditionViolation error

	ErrBadMsg           error
	ErrNoStorage        error
	ErrRecordLocked     error
	ErrTTLExtendFailure error
	ErrBusy             error

	ErrWriteFailure   error
	ErrInternal       error
	ErrOpNotSupported error
)

var errorMapping map[proto.OpStatus]error

func init() {
	ErrNoKey = &cli.Error{"no key"}
	ErrUniqueKeyViolation = &cli.Error{"unique key violation"}
	ErrBadParam = &cli.Error{"bad parameter"}
	ErrConditionViolation = &cli.Error{"condition violation"} //version too old
	ErrTTLExtendFailure = &cli.Error{"fail to extend TTL"}

	ErrBadMsg = &cli.RetryableError{"bad message"}
	ErrNoStorage = &cli.RetryableError{"no storage"}
	ErrRecordLocked = &cli.RetryableError{"record locked"}
	ErrBusy = &cli.RetryableError{"server busy"}

	ErrWriteFailure = &cli.Error{"write failure"}
	ErrInternal = &cli.Error{"internal error"}
	ErrOpNotSupported = &cli.Error{"Op not supported"}

	errorMapping = map[proto.OpStatus]error{
		proto.OpStatusNoError:            nil,
		proto.OpStatusInconsistent:       nil,
		proto.OpStatusBadMsg:             ErrBadMsg,
		proto.OpStatusNoKey:              ErrNoKey,
		proto.OpStatusDupKey:             ErrUniqueKeyViolation,
		proto.OpStatusNoStorageServer:    ErrNoStorage,
		proto.OpStatusBadParam:           ErrBadParam,
		proto.OpStatusRecordLocked:       ErrRecordLocked,
		proto.OpStatusVersionConflict:    ErrConditionViolation,
		proto.OpStatusSSReadTTLExtendErr: ErrTTLExtendFailure,
		proto.OpStatusCommitFailure:      ErrWriteFailure,
		proto.OpStatusBusy:               ErrBusy,
		proto.OpStatusNotSupported:       ErrOpNotSupported,
	}
}
