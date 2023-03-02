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
package storage

import (
	"os"
	"testing"
	"time"

	"juno/pkg/proto"
)

var (
	kSpecCreate_PrepReq specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kOptional,  //	kMsgValue
		kMust,      //	kMsgTTLOrExpTime
		kShouldNot, //	kMsgVersion
		kShould,    //	kMsgCreationTime
		kShouldNot, //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kShouldNot, //	kMsgOriginatorRID
		kOptional,  //	kMsgCorrelationID
	}
	kSpecCreate_PrepResp_NoErr specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kShouldNot, //	kMsgTTLOrExpTime
		kShouldNot, //	kMsgVersion
		kShouldNot, //	kMsgCreationTime
		kShouldNot, //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kShouldNot, //	kMsgOriginatorRID
		kShouldNot, //	kMsgCorrelationID
	}
	kSpecCreate_Commit_Req specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kMust,      //	kMsgTTLOrExpTime
		kMust,      //	kMsgVersion                  AND MUST BE 1
		kMust,      //	kMsgCreationTime
		kShould,    //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kShouldNot, //	kMsgOriginatorRID
		kOptional,  //	kMsgCorrelationID
	}
	kSpecCreate_Commit_Resp_NoErr specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kMust,      //	kMsgTTLOrExpTime
		kMust,      //	kMsgVersion                  AND MUST BE 1
		kMust,      //	kMsgCreationTime
		kMust,      //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kMust,      //	kMsgOriginatorRID
		kShouldNot, //	kMsgCorrelationID
	}
)

func TestCreate_PrepReq_Validate(t *testing.T) {
	req := &proto.OperationalMessage{}
	req.SetAsRequest()
	req.SetOpCode(proto.OpCodePrepareCreate)
	if validate(req) {
		t.FailNow()
	}
	resp, _ := processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetNewRequestID()
	if validate(req) {
		t.FailNow()
	}
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetNamespace(testNamespace)
	if validate(req) {
		t.FailNow()
	}
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetKey(testKey)
	if validate(req) {
		req.PrettyPrint(os.Stdout)
		t.FailNow()
	}
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)
	//	resp, _ = processRequest(req)
	//	if ok, err := hasExpectedStatus(resp, proto.OpStatusBadParam); !ok {
	//		t.Error(err)
	//	}
}

func TestCreate_PrepResp_NoErr(t *testing.T) {
	var err error
	creationTime := uint32(time.Now().Unix())
	ttl := uint32(1800)

	req := &proto.OperationalMessage{}
	req.SetAsRequest()
	payload := &proto.Payload{}
	payload.SetWithClearValue(testValue)
	req.SetRequest(proto.OpCodePrepareCreate, testKey, testNamespace, payload, ttl)
	req.SetNewRequestID()
	req.SetCreationTime(creationTime)
	validateRequest(t, req, kSpecCreate_PrepReq)
	resp, _ := processRequest(req)
	if resp.GetOpCode() != req.GetOpCode() {
		t.Error("wrong opcode")
	}
	expectStatus(t, resp, proto.OpStatusNoError)

	validateResponse(t, req, resp, kSpecCreate_PrepResp_NoErr)

	commit := &proto.OperationalMessage{}
	commit.SetAsRequest()
	commit.SetOpCode(proto.OpCodeCommit)
	commit.SetKey(req.GetKey())
	commit.SetNamespace(req.GetNamespace())
	commit.SetRequestID(req.GetRequestID())
	commit.SetTimeToLive(ttl)
	commit.SetCreationTime(creationTime)
	commit.SetVersion(1)
	validateRequest(t, commit, kSpecCreate_Commit_Req)
	resp, err = processRequest(commit)
	if err != nil {
		t.Error(err)
	}
	validateResponse(t, commit, resp, kSpecCreate_Commit_Resp_NoErr)
}
