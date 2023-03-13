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
	"context"
	//	"fmt"
	"bytes"
	goio "io"
	"testing"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/storage/db"
	"juno/pkg/io"
	"juno/pkg/proto"
	"juno/pkg/shard"
)

const (
	testDefaultTTL = uint32(1800)
)

var (
	testKey       = []byte("key")
	testNamespace = []byte("namespace")
	testValue     = []byte("value")
)

func getTestRecordID() db.RecordID {
	var buf bytes.Buffer
	id := db.NewRecordIDWithBuffer(&buf, shard.ID(0), 0, testNamespace, testKey)
	return id
}

const (
	kMsgNamespace = iota
	kMsgKey
	kMsgValue
	kMsgTTLOrExpTime
	kMsgVersion
	kMsgCreationTime
	kMsgLastMdificationTime
	kMsgSrcInfo
	kMsgRequestID
	kMsgOriginatorRID
	kMsgCorrelationID
	kNumMsgContents
)

var kMsgFieldName []string = []string{
	"Namespace",
	"Key",
	"Value",
	"TTLa/oExpTime",
	"Version",
	"CreationTime",
	"LastModificationTime",
	"SrcInfo",
	"RequestID",
	"OriginatorRID",
	"CorrelationID",
	"Not supported",
}

const (
	kUnspecified = iota
	kMust
	kMustNot
	kShould
	kShouldNot
	kOptional
)

type specsT [kNumMsgContents]int

type (
	testInboundReqCtxT struct {
		chResponse chan<- io.IResponseContext
		respCtx    io.IResponseContext
	}
)

func (r *testInboundReqCtxT) GetMessage() *proto.RawMessage {
	return nil
}
func (r *testInboundReqCtxT) GetCtx() context.Context {
	return nil
}
func (r *testInboundReqCtxT) Cancel() {}
func (r *testInboundReqCtxT) Read(re goio.Reader) (n int, err error) {
	return
}
func (r *testInboundReqCtxT) Reply(resp io.IResponseContext) {
	glog.InfoDepth(10, "testInboundReqCtx.Reply")
	r.respCtx = resp
	r.chResponse <- resp
}

func (r *testInboundReqCtxT) WriteWithOpaque(opaque uint32, w goio.Writer) (n int, err error) {
	return
}
func (r *testInboundReqCtxT) OnComplete()                                              {}
func (r *testInboundReqCtxT) OnCleanup()                                               {}
func (r *testInboundReqCtxT) OnExpiration()                                            {}
func (r *testInboundReqCtxT) SetId(id uint32)                                          {}
func (r *testInboundReqCtxT) GetId() uint32                                            { return 0 }
func (r *testInboundReqCtxT) SetInUse(flag bool)                                       {}
func (r *testInboundReqCtxT) IsInUse() bool                                            { return false }
func (r *testInboundReqCtxT) Cleanup()                                                 {}
func (r *testInboundReqCtxT) GetReceiveTime() time.Time                                { return time.Now() }
func (r *testInboundReqCtxT) SetTimeout(parent context.Context, timeout time.Duration) {}
func (r *testInboundReqCtxT) Deadline() (deadline time.Time)                           { return time.Time{} }

func dbStoreValidate(req *proto.OperationalMessage) bool {
	//validate() modifies the expiration time now, so make a copy here
	///TODO xuli revisit storage.validate()
	msg := *req
	return validate(&msg)
}

func processRequest(req *proto.OperationalMessage) (resp *proto.OperationalMessage, err error) {
	//treq := *req

	chResponse := make(chan io.IResponseContext)

	reqCtx := &reqProcCtxT{}
	reqCtx.init()
	reqCtx.request = *req
	reqCtx.reqctx = &testInboundReqCtxT{chResponse: chResponse}
	//reqCtx.request.Encode(reqCtx.)
	//reqCtx.attach(&testInboundReqCtxT{chResponse: chResponse})
	//process(reqCtx) //.Process(&testInboundReqCtxT{chResponse: chResponse})
	var buf bytes.Buffer
	reqCtx.recordId = db.NewRecordIDWithBuffer(&buf, shard.ID(0), 0, req.GetNamespace(), req.GetKey())
	if err != nil {
		return
	}
	reqCtx.shardId = shard.ID(req.GetShardId())

	go func() {
		process(reqCtx)
		/*
			resp, err = ProcessRequest(reqCtx)
			if resp != nil || err != nil {
				close(chResponse)
			}
		*/
	}()

	glog.Info("waiting for response...")
	response := <-chResponse
	if response != nil {

		resp = &proto.OperationalMessage{}

		resp.Decode(response.GetMessage())
		//resp.PrettyPrint(os.Stdout)
	}
	return
}

func expectStatus(t *testing.T, resp *proto.OperationalMessage, expectedStatus proto.OpStatus) {
	t.Helper()
	if resp != nil {
		if resp.IsResponse() == false {
			t.Errorf("not response message")
		}
		if resp.GetOpStatus() != expectedStatus {
			t.Errorf("%s expected, resp.st=%s", expectedStatus, resp.GetOpStatus())
		}
	} else {
		t.Fatal("op message is nil")
	}
}

func newDefaultRecord() *db.Record {
	now := uint32(time.Now().Unix())
	orid := proto.RequestId{}
	orid.SetNewRequestId()

	rid := proto.RequestId{}
	rid.SetNewRequestId()
	rec := &db.Record{
		RecordHeader: db.RecordHeader{
			Version:              2,
			CreationTime:         now - testDefaultTTL,
			LastModificationTime: uint64(time.Now().Add(-time.Duration(testDefaultTTL) * time.Second).UnixNano()),
			ExpirationTime:       now + testDefaultTTL,
			OriginatorRequestId:  orid,
			RequestId:            rid,
		},
	}
	rec.Payload.SetWithClearValue(testValue)
	return rec
}

func deleteRecord() {
	if err := db.GetDB().Delete(getTestRecordID()); err != nil {
		glog.Error(err)
	}
}
func storeRecord(rec *db.Record) error {
	var buf bytes.Buffer
	buf.Grow(rec.EncodingSize())
	err := rec.EncodeToBuffer(&buf)
	if err != nil {
		return err
	}
	return db.GetDB().Put(getTestRecordID(), buf.Bytes())
}

func getTestRecord() (*db.Record, error) {
	return db.GetDB().Get(getTestRecordID(), true)
}

func checkExistence(t *testing.T, msg *proto.OperationalMessage, msgContentID int, flag int) {
	t.Helper()
	var present bool
	switch msgContentID {
	case kMsgNamespace:
		present = len(msg.GetNamespace()) != 0
	case kMsgKey:
		present = len(msg.GetKey()) != 0
	case kMsgValue:
		pl := msg.GetPayload()
		if pl != nil {
			v, e := pl.GetClearValue()
			if e == nil && len(v) != 0 {
				present = true
			}
		}
	case kMsgTTLOrExpTime:
		present = msg.GetTimeToLive() != 0 || (msg.GetExpirationTime() > uint32(time.Now().Unix()))
	case kMsgVersion:
		present = msg.GetVersion() != 0
	case kMsgCreationTime:
		present = msg.GetCreationTime() != 0
	case kMsgLastMdificationTime:
		present = msg.GetLastModificationTime() != 0
	case kMsgSrcInfo:
		present = len(msg.GetSrcIP()) != 0 || msg.GetSrcPort() != 0 || len(msg.GetAppName()) != 0
	case kMsgRequestID:
		present = msg.IsRequestIDSet() //len(msg.GetRequestID()) != 0
	case kMsgOriginatorRID:
		present = msg.IsOriginatorSet() //len(msg.GetOriginatorRequestID()) != 0
	case kMsgCorrelationID:
		present = len(msg.GetCorrelationID()) != 0
	default:
		t.Error("not supported")
	}
	name := kMsgFieldName[msgContentID]
	switch flag {
	case kMust:
		if !present {
			t.Errorf("missing %s, which is a MUST", name)
		}
	case kMustNot:
		if present {
			t.Errorf("having %s, which is a MUST NOT", name)
		}
	case kShould:
		if !present {
			t.Logf("not having %s, which is a SHOULD", name)
		}
	case kShouldNot:
		if present {
			t.Logf("having %s, which is a SHOULD NOT", name)
		}
	}
}

func validateRequest(t *testing.T, req *proto.OperationalMessage, specs specsT) {
	t.Helper()

	if !req.IsRequest() {
		t.Error("not a request message")
	}
	checkExistence(t, req, kMsgNamespace, specs[kMsgNamespace])
	checkExistence(t, req, kMsgRequestID, specs[kMsgRequestID])
	checkExistence(t, req, kMsgOriginatorRID, specs[kMsgOriginatorRID])
	checkExistence(t, req, kMsgCreationTime, specs[kMsgCreationTime])
	checkExistence(t, req, kMsgTTLOrExpTime, specs[kMsgTTLOrExpTime])
	checkExistence(t, req, kMsgLastMdificationTime, specs[kMsgLastMdificationTime])
	checkExistence(t, req, kMsgSrcInfo, specs[kMsgSrcInfo])
	checkExistence(t, req, kMsgValue, specs[kMsgValue])
	checkExistence(t, req, kMsgVersion, specs[kMsgVersion])
	checkExistence(t, req, kMsgCorrelationID, specs[kMsgCorrelationID])
	checkExistence(t, req, kMsgKey, specs[kMsgKey])
}

func validateResponse(t *testing.T, req *proto.OperationalMessage, resp *proto.OperationalMessage, specs specsT) {
	t.Helper()

	opcode := req.GetOpCode()

	if !resp.IsResponse() {
		t.Error("not a response message")
	}
	if resp.GetOpCode() != opcode {
		t.Errorf("wrong opcode. %s - %s ", req.GetOpCode(), resp.GetOpCode())
	}

	if !req.GetRequestID().Equal(resp.GetRequestID()) {
		t.Error("wrong request id in response")
	}

	checkExistence(t, resp, kMsgNamespace, specs[kMsgNamespace])
	checkExistence(t, resp, kMsgRequestID, specs[kMsgRequestID])
	checkExistence(t, resp, kMsgOriginatorRID, specs[kMsgOriginatorRID])
	checkExistence(t, resp, kMsgCreationTime, specs[kMsgCreationTime])
	checkExistence(t, resp, kMsgTTLOrExpTime, specs[kMsgTTLOrExpTime])
	checkExistence(t, resp, kMsgLastMdificationTime, specs[kMsgLastMdificationTime])
	checkExistence(t, resp, kMsgSrcInfo, specs[kMsgSrcInfo])
	checkExistence(t, resp, kMsgValue, specs[kMsgValue])
	checkExistence(t, resp, kMsgVersion, specs[kMsgVersion])
	checkExistence(t, resp, kMsgCorrelationID, specs[kMsgCorrelationID])
	checkExistence(t, resp, kMsgKey, specs[kMsgKey])
}

func newAbortRequest(req *proto.OperationalMessage) (abortReq *proto.OperationalMessage) {
	abortReq = &proto.OperationalMessage{}
	abortReq.SetAsRequest()
	abortReq.SetOpCode(proto.OpCodeAbort)
	abortReq.SetKey(req.GetKey())
	abortReq.SetNamespace(req.GetNamespace())
	abortReq.SetRequestID(req.GetRequestID())
	return
}

func newDefaultCommitFromPrepReq(prepReq *proto.OperationalMessage) *proto.OperationalMessage {
	commitReq := &proto.OperationalMessage{}
	commitReq.SetAsRequest()
	commitReq.SetOpCode(proto.OpCodeCommit)
	commitReq.SetKey(prepReq.GetKey())
	commitReq.SetNamespace(prepReq.GetNamespace())
	commitReq.SetRequestID(prepReq.GetRequestID())
	return commitReq
}

func abort(req *proto.OperationalMessage) {
	processRequest(newAbortRequest(req))
}
