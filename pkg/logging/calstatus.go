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
package logging

import (
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
)

type Status int

const (
	kStatusSuccess Status = Status(iota)
	kStatusFatal
	kStatusError
	kStatusWarning
	kNumStatus
)

var (
	calStatus []string = []string{
		cal.StatusSuccess,
		cal.StatusFatal,
		cal.StatusError,
		cal.StatusWarning,
	}

	opStatusToCalStatusMap map[proto.OpStatus]Status = map[proto.OpStatus]Status{
		proto.OpStatusNoError:          kStatusSuccess,
		proto.OpStatusNoKey:            kStatusSuccess,
		proto.OpStatusDupKey:           kStatusSuccess,
		proto.OpStatusRecordLocked:     kStatusSuccess,
		proto.OpStatusInserting:        kStatusSuccess,
		proto.OpStatusAlreadyFulfilled: kStatusSuccess,
		proto.OpStatusVersionConflict:  kStatusSuccess,
		proto.OpStatusInconsistent:     kStatusSuccess,

		proto.OpStatusBadMsg:             kStatusWarning,
		proto.OpStatusBadParam:           kStatusWarning,
		proto.OpStatusNoUncommitted:      kStatusWarning,
		proto.OpStatusSSReadTTLExtendErr: kStatusWarning,

		proto.OpStatusServiceDenied:   kStatusError,
		proto.OpStatusNoStorageServer: kStatusFatal,
		proto.OpStatusSSError:         kStatusError,
		proto.OpStatusSSOutofResource: kStatusError,
		proto.OpStatusReqProcTimeout:  kStatusError,
		proto.OpStatusCommitFailure:   kStatusError,
	}
)

func CalStatus(st proto.OpStatus) Status {
	if calstatus, ok := opStatusToCalStatusMap[st]; ok {
		return calstatus
	}
	return kStatusSuccess
}

func (s Status) CalStatus() string {
	if s < kNumStatus {
		return calStatus[int(s)]
	}
	return cal.StatusSuccess
}

func (s Status) NotSuccess() bool {
	return (s != kStatusSuccess)
}
