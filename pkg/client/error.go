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

// client is a package that handles various error situations in the Juno application.
package client

import (
	"github.com/paypal/junodb/internal/cli"
	"github.com/paypal/junodb/pkg/proto"
)

// Error variables for different scenarios in the application.
var (
	ErrConnect         error
	ErrResponseTimeout error

	ErrNoKey              error
	ErrUniqueKeyViolation error
	ErrBadParam           error
	ErrConditionViolation error

	ErrBadMsg           error // Error when a bad message is encountered.
	ErrNoStorage        error // Error when no storage is available.
	ErrRecordLocked     error // Error when a record is locked.
	ErrTTLExtendFailure error // Error when TTL extension fails.
	ErrBusy             error // Error when the server is busy.

	ErrWriteFailure   error // Error when a write operation fails.
	ErrInternal       error // Error when an internal problem occurs.
	ErrOpNotSupported error // Error when the operation is not supported.
)

// errorMapping is a map between different operation status and their corresponding errors.
var errorMapping map[proto.OpStatus]error

// init function initializes the error variables and the errorMapping map.
func init() {
	ErrResponseTimeout = cli.ErrResponseTimeout
	ErrConnect = cli.ErrConnect

	ErrNoKey = &cli.Error{"no key"}
	ErrUniqueKeyViolation = &cli.Error{"unique key violation"}
	ErrBadParam = &cli.Error{"bad parameter"}
	ErrConditionViolation = &cli.Error{"condition violation"} //version too old
	ErrTTLExtendFailure = &cli.Error{"fail to extend TTL"}

	ErrBadMsg = &cli.RetryableError{"bad message"}         // Error when an inappropriate message is received.
	ErrNoStorage = &cli.RetryableError{"no storage"}       // Error when there is no storage available.
	ErrRecordLocked = &cli.RetryableError{"record locked"} // Error when a record is locked.
	ErrBusy = &cli.RetryableError{"server busy"}           // Error when the server is busy.

	ErrWriteFailure = &cli.Error{"write failure"}      // Error when a write operation fails.
	ErrInternal = &cli.Error{"internal error"}         // Error when an internal error occurs.
	ErrOpNotSupported = &cli.Error{"Op not supported"} // Error when the operation is not supported.

	// Mapping between the operation status and the corresponding errors.
	errorMapping = map[proto.OpStatus]error{
		proto.OpStatusNoError:            nil,                   // Status when there is no error.
		proto.OpStatusInconsistent:       nil,                   // Status when there is an inconsistency.
		proto.OpStatusBadMsg:             ErrBadMsg,             // Status when a bad message is received.
		proto.OpStatusNoKey:              ErrNoKey,              // Status when the key is not present.
		proto.OpStatusDupKey:             ErrUniqueKeyViolation, // Status when unique key constraint is violated.
		proto.OpStatusNoStorageServer:    ErrNoStorage,          // Status when there is no storage server available.
		proto.OpStatusBadParam:           ErrBadParam,           // Status when a bad parameter is passed.
		proto.OpStatusRecordLocked:       ErrRecordLocked,       // Status when a record is locked.
		proto.OpStatusVersionConflict:    ErrConditionViolation, // Status when there is a version conflict.
		proto.OpStatusSSReadTTLExtendErr: ErrTTLExtendFailure,   // Status when TTL extension fails.
		proto.OpStatusCommitFailure:      ErrWriteFailure,       // Status when a commit operation fails.
		proto.OpStatusBusy:               ErrBusy,               // Status when the server is busy.
		proto.OpStatusNotSupported:       ErrOpNotSupported,     // Status when the operation is not supported.
	}
}
