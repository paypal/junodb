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

//go:build ignore
// +build ignore

package main

import (
	"math/rand"
	"time"

	"github.com/paypal/junodb/pkg/io"
	_ "github.com/paypal/junodb/pkg/logging"
	"github.com/paypal/junodb/pkg/proto"

	"github.com/paypal/junodb/third_party/forked/golang/glog"
)

//type IRequestHandler interface {
//	Process(reqCtx *InboundRequestContext) error
//	Start()
//	Shutdown()
//}

type RequestHandler struct {
	meanDelay   int
	stdDevDelay int
	valueSize   int
	stdDevSize  int
	r_delay     *rand.Rand
	r_size      *rand.Rand
	value       []byte
}

func NewRequestHandler(meanDelay int, stdDevDelay int, valueSize int, stdDevSize int) *RequestHandler {

	rh := &RequestHandler{
		meanDelay:   meanDelay,
		stdDevDelay: stdDevDelay,
		valueSize:   valueSize,
		stdDevSize:  stdDevSize,
		r_delay:     rand.New(rand.NewSource(1698661970)),
		r_size:      rand.New(rand.NewSource(10)),
		value:       make([]byte, valueSize+10*stdDevSize, valueSize+10*stdDevSize),
	}
	cap := valueSize + 10*stdDevSize
	for k := 0; k < cap; k++ {
		c := rand.Intn(122)
		for {
			if (c < 48) || ((c > 57) && (c < 65)) || ((c > 90) && (c < 97)) {
				c = rand.Intn(122)
			} else {
				break
			}
		}
		rh.value[k] = byte(c)
	}

	return rh
}

func (rh *RequestHandler) Process(reqCtx io.IRequestContext) error {

	delay := 0
	if rh.meanDelay > 0 {
		delay = int(rh.r_delay.NormFloat64()*float64(rh.stdDevDelay) + float64(rh.meanDelay))
	}
	vsize := int(rh.r_size.NormFloat64()*float64(stdDevSize) + float64(valueSize))

	if delay > 0 {
		time.Sleep(time.Microsecond * time.Duration(delay))
	}

	var msg proto.OperationalMessage
	if err := msg.Decode(reqCtx.GetMessage()); err != nil {
		/*
			glog.Error("Failed to decode inbound request: ", err)
			p.replyErrorToClient(proto.OpStatusBadMsg)
			p.OnComplete()
		*/
		// reply the response with error status code
		return err
	}

	wmsg := reqCtx.GetMessage()
	op, err := proto.GetOpCode(wmsg)
	if err != nil {
		glog.Error("Cannot get Opcode: ", err)
		return err
	}

	msg.SetAsResponse()
	switch op {
	case proto.OpCodeGet, proto.OpCodeRead:
		msg.SetOpStatus(proto.OpStatusNoError)

		// TODO: copy the key to the begining of byte slice
		// note the value maybe be shared by multiple go routine, so make a copy
		if vsize > cap(value) {
			msg.SetValue(value[:cap(value)])
		} else {
			msg.SetValue(value[:vsize])
		}

		msg.SetOpStatus(proto.OpStatusNoError)
		msg.SetCreationTime(uint32(time.Now().Unix()))
		msg.SetTimeToLive(1000)

	case proto.OpCodeCommit:

		msg.SetOpStatus(proto.OpStatusNoError)
		msg.SetValue(nil)
		msg.SetCreationTime(uint32(time.Now().Unix()))
		msg.SetTimeToLive(1000)
		msg.SetVersion(1)

	case proto.OpCodeCreate, proto.OpCodePrepareCreate,
		proto.OpCodeUpdate, proto.OpCodePrepareUpdate,
		proto.OpCodeSet, proto.OpCodePrepareSet,
		proto.OpCodeDelete, proto.OpCodeDestroy,
		proto.OpCodePrepareDelete,
		proto.OpCodeAbort:

		msg.SetOpStatus(proto.OpStatusNoError)
		msg.SetValue(nil)

	default:
		glog.V(2).Infof("Error not handled operation")
		msg.SetOpStatus(proto.OpStatusBadParam)
		msg.SetValue(nil)
	}

	resp, err := io.NewInboundResponseContext(&msg)
	reqCtx.Reply(resp)

	// ???
	reqCtx.OnComplete()
	return nil
}

func (rh *RequestHandler) Init() {
}

func (rh *RequestHandler) Finish() {
}
