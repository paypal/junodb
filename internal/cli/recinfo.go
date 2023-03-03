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
package cli

import (
	"fmt"
	"io"
	"juno/pkg/proto"
)

type RecordInfo struct {
	version      uint32
	creationTime uint32
	timeToLive   uint32
	originatorId proto.RequestId
}

func (r *RecordInfo) GetVersion() uint32 {
	return r.version
}

func (r *RecordInfo) GetCreationTime() uint32 {
	return r.creationTime
}

func (r *RecordInfo) GetTimeToLive() uint32 {
	return r.timeToLive
}

func (r *RecordInfo) SetRequestWithUpdateCond(request *proto.OperationalMessage) {
	if r.creationTime != 0 {
		request.SetCreationTime(r.creationTime)
	}
	if r.version != 0 {
		request.SetVersion(r.version)
	}
	if r.originatorId.IsSet() {
		request.SetOriginatorRequestID(r.originatorId)
	}
}

func (r *RecordInfo) String() string {
	return fmt.Sprintf("ver=%d,ct=%d,ttl=%d", r.version, r.creationTime, r.timeToLive)
}

func (r *RecordInfo) SetFromOpMsg(m *proto.OperationalMessage) {
	r.version = m.GetVersion()
	r.creationTime = m.GetCreationTime()
	r.timeToLive = m.GetTimeToLive()
	if m.IsOriginatorSet() {
		r.originatorId = m.GetOriginatorRequestID()
	} else {
		r.originatorId = proto.NilRequestId
	}
}

func (r *RecordInfo) IsSameOriginator(ctx *RecordInfo) bool {
	if ctx != nil {
		return r.originatorId.Equal(ctx.originatorId)
	}
	return false
}

func (r *RecordInfo) PrettyPrint(w io.Writer) {
	fmt.Fprintf(w,
		`RecordInfo {
  version     : %d
  creationTime: %d
  timeToLive  : %d
  originatorID: %s
}
`, r.version, r.creationTime, r.timeToLive, proto.RequestIdTextFromBytes(r.originatorId.Bytes()))
}
