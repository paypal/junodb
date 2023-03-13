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

package storage

import (
	"testing"
	"time"

	"juno/pkg/proto"
)

func BenchmarkSet(b *testing.B) {

	ct := uint32(time.Now().Unix())

	for i := 0; i < b.N; i++ {
		req := newDefaultSetRequest()
		resp, _ := processRequest(req)
		ttl := uint32(7200)
		commit := &proto.OperationalMessage{}
		commit.SetAsRequest()
		commit.SetOpCode(proto.OpCodeCommit)
		commit.SetKey(req.GetKey())
		commit.SetNamespace(req.GetNamespace())
		commit.SetRequestID(req.GetRequestID())
		commit.SetTimeToLive(ttl)
		if resp.GetCreationTime() == 0 {
			commit.SetCreationTime(ct)
		} else {
			commit.SetCreationTime(resp.GetCreationTime())
		}
		//commit.SetCreationTime(resp.GetCreationTime())
		commit.SetOriginatorRequestID(resp.GetOriginatorRequestID())
		commit.SetVersion(resp.GetVersion() + 1)
		resp, _ = processRequest(commit)
	}

}
