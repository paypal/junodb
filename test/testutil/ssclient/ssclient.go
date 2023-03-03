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
package ssclient

import (
	"fmt"
	"io"
	"net"

	"juno/third_party/forked/golang/glog"

	"juno/internal/cli"
	"juno/pkg/client"
	"juno/pkg/proto"
	"juno/pkg/util"
)

type Record struct {
	CreationTime         uint32
	TimeToLive           uint32
	Version              uint32
	LastModificationTime uint32
	Payload              proto.Payload
	OriginatorRequestId  proto.RequestId
	RequestId            proto.RequestId
}

func (r *Record) PrettyPrint(w io.Writer) {
	if r.CreationTime != 0 {
		fmt.Fprintf(w, "Creation Time : %d\n", r.CreationTime)
	}
	if r.TimeToLive != 0 {
		fmt.Fprintf(w, "TimeToLive    : %d\n", r.TimeToLive)
	}
	if r.Version != 0 {
		fmt.Fprintf(w, "Version       : %d\n", r.Version)
	}

	if r.RequestId.IsSet() {
		fmt.Fprintf(w, "RequestID     : %s\n", r.RequestId.String())
	}
	if r.OriginatorRequestId.IsSet() {
		fmt.Fprintf(w, "Originator    : %s\n", r.OriginatorRequestId.String())
	}
	r.Payload.PrettyPrint(w)
}

type SSClient struct {
	Target    string
	Namespace string
	NumShards uint32
}

var (
	ErrNoKey              = cli.NewErrorWithString("no key")
	ErrUniqueKeyViolation = cli.NewErrorWithString("unique key violation")
	ErrBadParam           = cli.NewErrorWithString("bad parameter")
	ErrConditionViolation = cli.NewErrorWithString("condition violation") //version too old

	ErrBadMsg        = cli.NewErrorWithString("bad message")
	ErrRecordLocked  = cli.NewErrorWithString("record locked")
	ErrServiceDenied = cli.NewErrorWithString("service denied")
	ErrCommitFailure = cli.NewErrorWithString("commit failure")
	ErrMarkedDelete  = cli.NewErrorWithString("record is marked as deleted")
	ErrInternal      = cli.NewErrorWithString("internal error")
)

var errorMapping map[proto.OpStatus]error = map[proto.OpStatus]error{
	proto.OpStatusNoError:         nil,
	proto.OpStatusBadMsg:          client.ErrBadMsg,
	proto.OpStatusServiceDenied:   ErrServiceDenied,
	proto.OpStatusNoKey:           client.ErrNoKey,
	proto.OpStatusDupKey:          ErrUniqueKeyViolation,
	proto.OpStatusBadParam:        ErrBadParam,
	proto.OpStatusRecordLocked:    ErrRecordLocked,
	proto.OpStatusVersionConflict: ErrConditionViolation,
	proto.OpStatusCommitFailure:   ErrCommitFailure,
	proto.OpStatusKeyMarkedDelete: ErrMarkedDelete,
}

///TODO to use client/Processor or io/OutboundProcessor

func NewSSClient(server string, ns string) *SSClient {
	return NewSSClientWithNumShards(server, ns, 1024)
}

func NewSSClientWithNumShards(server string, ns string, numShards uint32) *SSClient {
	client := &SSClient{
		Target:    server,
		Namespace: ns,
		NumShards: numShards,
	}
	return client
}

func (c *SSClient) process(request *proto.OperationalMessage) (response *proto.OperationalMessage, err error) {
	conn, err := net.Dial("tcp", c.Target)
	if err != nil {
		glog.Info("cannot connect: ", err)
		return
	}
	defer conn.Close()

	var raw proto.RawMessage

	if err = request.Encode(&raw); err != nil {
		glog.Info("Error: ", err.Error())
		return
	}
	_, err = raw.Write(conn)
	if err != nil {
		glog.Info("Write Error: ", err)
		return
	}

	var rawResponse proto.RawMessage
	_, err = rawResponse.Read(conn)
	if err != nil {
		glog.Info(err)
		return
	}
	response = &proto.OperationalMessage{}
	err = response.Decode(&rawResponse)

	return
}

func (c *SSClient) Get(key []byte) (value []byte, recInfo *cli.RecordInfo, err error) {
	request := proto.OperationalMessage{}
	request.SetRequest(proto.OpCodeRead, key, []byte(c.Namespace), nil, 0)

	request.SetNewRequestID()
	request.SetShardId(util.GetPartitionId(key, c.NumShards))

	var response *proto.OperationalMessage
	response, err = c.process(&request)

	if err == nil {
		recInfo = &cli.RecordInfo{}
		recInfo.SetFromOpMsg(response)
		status := response.GetOpStatus()
		if status == proto.OpStatusNoError {
			payload := response.GetPayload()
			if payload.GetPayloadType() == proto.PayloadTypeEncryptedByProxy {
				err = payload.Decrypt()
				if err != nil {
					return
				}
			}
			value = payload.GetData()
		} else {
			glog.Debug(response.GetOpStatusText())
			err = errorMapping[response.GetOpStatus()]
		}
	}
	if err != nil {
		glog.Debug(err)
		return
	}
	return
}

func (c *SSClient) Read(key []byte) (rec *Record, err error) {
	request := proto.OperationalMessage{}
	request.SetRequest(proto.OpCodeRead, key, []byte(c.Namespace), nil, 0)

	request.SetNewRequestID()
	request.SetShardId(util.GetPartitionId(key, c.NumShards))

	var response *proto.OperationalMessage
	response, err = c.process(&request)

	if err == nil {
		rec = &Record{
			CreationTime:        response.GetCreationTime(),
			TimeToLive:          response.GetTimeToLive(),
			Version:             response.GetVersion(),
			OriginatorRequestId: response.GetOriginatorRequestID(),
			RequestId:           response.GetRequestID(),
		}
		rec.Payload.Set(response.GetPayload())
		err = errorMapping[response.GetOpStatus()]
	}
	if err != nil {
		glog.Debug(err)
		return
	}
	return
}

func (c *SSClient) Delete(key []byte) (err error) {
	request := proto.OperationalMessage{}
	request.SetRequest(proto.OpCodeDelete, key, []byte(c.Namespace), nil, 0)

	request.SetNewRequestID()
	request.SetShardId(util.GetPartitionId(key, c.NumShards))

	var response *proto.OperationalMessage
	response, err = c.process(&request)

	if err != nil {
		glog.Debug(err)
		return
	}
	err = errorMapping[response.GetOpStatus()]
	return
}

func (c *SSClient) Store(key []byte, rec *Record) (err error) {
	request := proto.OperationalMessage{}
	opcode := proto.OpCodeRepair

	request.SetRequest(opcode, key, []byte(c.Namespace), &rec.Payload, rec.TimeToLive)
	request.SetVersion(rec.Version)
	/*
		if rec.MarkedDelete {
			var f uint8
			f |= 0x2
			request.SetFlags(f)
		}
	*/
	if rec.RequestId.IsSet() {
		request.SetRequestID(rec.RequestId)
	} else {
		request.SetNewRequestID()
	}
	if rec.OriginatorRequestId.IsSet() {
		request.SetOriginatorRequestID(rec.OriginatorRequestId)
	} else {
		err = fmt.Errorf("wrong originator request Id")
		return
	}
	if rec.CreationTime != 0 {
		request.SetCreationTime(rec.CreationTime)
	}
	request.SetShardId(util.GetPartitionId(key, c.NumShards))

	var response *proto.OperationalMessage
	response, err = c.process(&request)

	if err != nil {
		glog.Debug(err)
		return
	}
	err = errorMapping[response.GetOpStatus()]
	return
}

func (c *SSClient) MarkDelete(key []byte, rec *Record) (err error) {
	request := proto.OperationalMessage{}

	request.SetRequest(proto.OpCodeMarkDelete, key, []byte(c.Namespace), &rec.Payload, rec.TimeToLive)
	request.SetVersion(rec.Version)
	if rec.RequestId.IsSet() {
		request.SetRequestID(rec.RequestId)
	} else {
		request.SetNewRequestID()
	}
	if rec.OriginatorRequestId.IsSet() {
		request.SetOriginatorRequestID(rec.OriginatorRequestId)
	} else {
		err = fmt.Errorf("wrong originator request Id")
		return
	}
	if rec.CreationTime != 0 {
		request.SetCreationTime(rec.CreationTime)
	}
	request.SetShardId(util.GetPartitionId(key, c.NumShards))

	var response *proto.OperationalMessage
	response, err = c.process(&request)

	if err != nil {
		glog.Debug(err)
		return
	}
	var ok bool
	if err, ok = errorMapping[response.GetOpStatus()]; !ok {
		err = ErrInternal
	}
	return
}
