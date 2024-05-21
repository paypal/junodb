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

	"github.com/paypal/junodb/pkg/proto"
)

var (
	kSpecSet_PrepReq specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kOptional,  //	kMsgValue
		kOptional,  //	kMsgTTLOrExpTime
		kMustNot,   //	kMsgVersion
		kShouldNot, //	kMsgCreationTime
		kShouldNot, //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kShouldNot, //	kMsgOriginatorRID
		kOptional,  //	kMsgCorrelationID
	}
	kSpecSet_PrepResp_NoErr specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kOptional,  //	kMsgValue
		kMust,      //	kMsgTTLOrExpTime
		kMust,      //	kMsgVersion
		kMust,      //	kMsgCreationTime
		kMust,      //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kMust,      //	kMsgOriginatorRID
		kShouldNot, //	kMsgCorrelationID
	}
	kSpecSet_PrepResp_Inserting specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kOptional,  //	kMsgTTLOrExpTime
		kOptional,  //	kMsgVersion
		kOptional,  //	kMsgCreationTime
		kOptional,  //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kOptional,  //	kMsgOriginatorRID
		kShouldNot, //	kMsgCorrelationID
	}
	kSpecSet_Commit_Req specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kMust,      //	kMsgTTLOrExpTime
		kMust,      //	kMsgVersion
		kMust,      //	kMsgCreationTime
		kShould,    //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kMust,      //	kMsgOriginatorRID
		kShouldNot, //	kMsgCorrelationID
	}
	kSpecSet_Commit_Resp_NoErr specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kMust,      //	kMsgTTLOrExpTime
		kMust,      //	kMsgVersion
		kMust,      //	kMsgCreationTime
		kMust,      //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kMust,      //	kMsgOriginatorRID
		kShouldNot, //	kMsgCorrelationID
	}
)

func newDefaultSetRequest() *proto.OperationalMessage {
	req := &proto.OperationalMessage{}
	req.SetAsRequest()
	req.SetOpCode(proto.OpCodePrepareSet)
	req.SetKey(testKey)
	req.SetNamespace(testNamespace)
	req.SetNewRequestID()
	payload := &proto.Payload{}
	payload.SetWithClearValue(testValue)
	req.SetPayload(payload)
	return req
}

func TestSet_PrepReq_Validate(t *testing.T) {
	deleteRecord()
	req := &proto.OperationalMessage{}
	req.SetOpCode(proto.OpCodePrepareSet)
	resp, _ := processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetNewRequestID()
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetNamespace(testNamespace)
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetKey(testKey)
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusInserting)
	validateResponse(t, req, resp, kSpecSet_PrepResp_Inserting)
	processRequest(newAbortRequest(req))
}

func TestSet_PrepReq_Rep_Validate(t *testing.T) {
	req := &proto.OperationalMessage{}
	req.SetOpCode(proto.OpCodePrepareSet)
	req.SetAsReplication()
	resp, _ := processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetNewRequestID()
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetNamespace(testNamespace)
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetKey(testKey)
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetTimeToLive(1800)
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetCreationTime(uint32(time.Now().Unix()))
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusBadParam)

	req.SetVersion(1)
	//TODO: to enable originator request id later
	/*
		resp, _ = processRequest(req)
		expectStatus(resp, proto.OpStatusBadParam, t) //still missing the Originator request Id
	*/

	req.SetOriginatorRequestID(req.GetRequestID())
	resp, _ = processRequest(req)
	expectStatus(t, resp, proto.OpStatusInserting)

	validateResponse(t, req, resp, kSpecSet_PrepResp_Inserting)

	processRequest(newAbortRequest(req))
}

func TestSet_to_existing_record(t *testing.T) {
	version := uint32(10)

	rec := newDefaultRecord()
	rec.Version = version
	storeRecord(rec)

	req := newDefaultSetRequest()
	validateRequest(t, req, kSpecSet_PrepReq)
	resp, _ := processRequest(req)
	expectStatus(t, resp, proto.OpStatusNoError)
	validateResponse(t, req, resp, kSpecUpdate_PrepResp_Inserting)
	if resp.GetVersion() != version {
		t.Errorf("wrong returned version. expected: %d, returned : %d", version, resp.GetVersion())
	}
	ttl := uint32(7200)
	commit := &proto.OperationalMessage{}
	commit.SetAsRequest()
	commit.SetOpCode(proto.OpCodeCommit)
	commit.SetKey(req.GetKey())
	commit.SetNamespace(req.GetNamespace())
	commit.SetRequestID(req.GetRequestID())
	commit.SetTimeToLive(ttl)
	commit.SetCreationTime(resp.GetCreationTime())
	commit.SetOriginatorRequestID(resp.GetOriginatorRequestID())
	commit.SetVersion(resp.GetVersion() + 1)
	validateRequest(t, commit, kSpecSet_Commit_Req)
	resp, _ = processRequest(commit)
	validateResponse(t, commit, resp, kSpecSet_Commit_Resp_NoErr)
}

func TestSet_to_markedDelete_record(t *testing.T) {
	version := uint32(10)

	rec := newDefaultRecord()
	rec.MarkDelete()
	rec.Version = version
	storeRecord(rec)

	req := newDefaultSetRequest()
	validateRequest(t, req, kSpecSet_PrepReq)
	resp, _ := processRequest(req)
	expectStatus(t, resp, proto.OpStatusInserting)
	validateResponse(t, req, resp, kSpecSet_PrepResp_Inserting)
	if resp.GetVersion() != version {
		t.Errorf("wrong returned version. expected: %d, returned : %d", version, resp.GetVersion())
	}
	processRequest(newAbortRequest(req))
}
