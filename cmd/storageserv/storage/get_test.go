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

///TODO:
///     Get when the key being locked by two phase writes

import (
	"testing"

	"juno/pkg/proto"
	"juno/test/testutil"
)

var (
	kSpecGet_Req specsT = specsT{
		kMust,      //	kMsgNamespace
		kMust,      //	kMsgKey
		kShouldNot, //	kMsgValue
		kOptional,  //	kMsgTTLOrExpTime
		kShouldNot, //	kMsgVersion
		kShouldNot, //	kMsgCreationTime
		kShouldNot, //	kMsgLastMdificationTime
		kShouldNot, //	kMsgSrcInfo
		kMust,      //	kMsgRequestID
		kShouldNot, //	kMsgOriginatorRID
		kOptional,  //	kMsgCorrelationID
	}
	kSpecGet_Req_Rep specsT = specsT{
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
		kOptional,  //	kMsgCorrelationID
	}
	kSpecGet_Resp_NoErr specsT = specsT{
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
	kSpecGet_Resp_NoKey specsT = specsT{
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
)

func newDefaultGetRequest() *proto.OperationalMessage {
	req := &proto.OperationalMessage{}
	req.SetAsRequest()
	req.SetOpCode(proto.OpCodeRead)
	req.SetKey(testKey)
	req.SetNamespace(testNamespace)
	req.SetNewRequestID()
	return req
}

func TestGet(t *testing.T) {
	deleteRecord()
	req := newDefaultGetRequest()
	validateRequest(t, req, kSpecGet_Req)
	resp, err := processRequest(req)
	if err != nil {
		t.Error(err)
	}

	expectStatus(t, resp, proto.OpStatusNoKey)
	validateResponse(t, req, resp, kSpecGet_Resp_NoKey)

	version := uint32(2)

	rec := newDefaultRecord()
	rec.Version = version
	storeRecord(rec)

	req = newDefaultGetRequest()
	resp, err = processRequest(req)
	if err != nil {
		t.Error(err)
	}
	expectStatus(t, resp, proto.OpStatusNoError)
	validateResponse(t, req, resp, kSpecGet_Resp_NoErr)
	if resp.GetVersion() != version {
		t.Errorf("respose version: %d. expect: %d", resp.GetVersion(), version)
	}

	ttl := resp.GetTimeToLive() * 2
	req.SetTimeToLive(ttl) //For replication get request, only TTL matters
	req.SetVersion(version)
	req.SetCreationTime(rec.CreationTime)
	req.SetOriginatorRequestID(rec.OriginatorRequestId)
	req.SetLastModificationTime(rec.LastModificationTime)
	validateRequest(t, req, kSpecGet_Req_Rep)
	resp, err = processRequest(req)
	if err != nil {
		t.Error(err)
	}
	expectStatus(t, resp, proto.OpStatusNoError)
	validateResponse(t, req, resp, kSpecGet_Resp_NoErr)
	if testutil.ApproxEqual(ttl, resp.GetTimeToLive(), 2) == false {
		t.Errorf("expected: %d actual: %d", ttl, resp.GetTimeToLive())
	}
}

func TestGet_Rep(t *testing.T) {
	deleteRecord()
	rec := newDefaultRecord()
	ttl := uint32(1800)
	version := uint32(9)

	req := newDefaultGetRequest()
	req.SetAsReplication()
	req.SetCreationTime(rec.CreationTime)
	req.SetTimeToLive(ttl)
	req.SetOriginatorRequestID(rec.OriginatorRequestId)
	req.SetLastModificationTime(rec.LastModificationTime)
	req.SetVersion(version)
	validateRequest(t, req, kSpecGet_Req_Rep)
	resp, err := processRequest(req)
	if err != nil {
		t.Error(err)
	}
	expectStatus(t, resp, proto.OpStatusNoKey)

	validateResponse(t, req, resp, kSpecGet_Resp_NoKey)

	storeRecord(rec)

	req = newDefaultGetRequest()
	req.SetAsReplication()
	req.SetCreationTime(rec.CreationTime)
	req.SetTimeToLive(ttl * 2)
	req.SetOriginatorRequestID(rec.OriginatorRequestId)
	req.SetLastModificationTime(rec.LastModificationTime)
	req.SetVersion(version)
	validateRequest(t, req, kSpecGet_Req_Rep)
	resp, err = processRequest(req)
	if err != nil {
		t.Error(err)
	}
	expectStatus(t, resp, proto.OpStatusNoError)

	validateResponse(t, req, resp, kSpecGet_Resp_NoErr)

}
