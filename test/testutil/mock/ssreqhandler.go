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

package mock

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"juno/third_party/forked/golang/glog"

	//	"juno/cmd/proxy/handler"
	"juno/pkg/io"
	"juno/pkg/proto"
	"juno/pkg/service"
	"juno/pkg/util"
)

var _ io.IRequestHandler = (*RequestHandler)(nil)

type RequestHandler struct {
	g_mockinfo     map[string]MockInfo
	mtx_g_mockinfo sync.RWMutex
	meanDelay      int
	stdDevDelay    int
	valueSize      int
	stdDevSize     int
	r_delay        *rand.Rand
	r_size         *rand.Rand
	value          []byte
}

func NewRequestHandler(conf *SSConfig) *RequestHandler {

	rh := &RequestHandler{
		meanDelay:   conf.MeanDelay,
		stdDevDelay: conf.StdDevDelay,
		valueSize:   conf.ValueSize,
		stdDevSize:  conf.StdDevSize,
		r_delay:     rand.New(rand.NewSource(1698661970)),
		r_size:      rand.New(rand.NewSource(10)),
		value:       make([]byte, conf.ValueSize+10*conf.StdDevSize, conf.ValueSize+10*conf.StdDevSize),
		g_mockinfo:  make(map[string]MockInfo),
	}
	cap := conf.ValueSize + 10*conf.StdDevSize
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

func (rh *RequestHandler) ReadAndProcessRequest(magic []byte, c *io.Connector) error {
	//	c.SetReadDeadline()
	ctx, err := io.DefaultInboundRequestContexCreator(magic, c)
	if err != nil {
		return err
	}
	//	ctx.SetTimeout(c.GetContext(), c.GetConfig().RequestTimeout.Duration)
	go rh.Process(ctx)
	return nil
}

func (rh *RequestHandler) Process(reqCtx io.IRequestContext) error {
	glog.Debug("MockSS::Processo()")
	delay := 0
	if rh.meanDelay > 0 {
		delay = int(rh.r_delay.NormFloat64()*float64(rh.stdDevDelay) + float64(rh.meanDelay))
	}
	vsize := int(rh.r_size.NormFloat64()*float64(rh.stdDevSize) + float64(rh.valueSize))
	if delay > 0 {
		time.Sleep(time.Microsecond * time.Duration(delay))
	}

	var msg proto.OperationalMessage
	if err := msg.Decode(reqCtx.GetMessage()); err != nil {
		return err
	}

	// get namespace first
	ns := string(msg.GetNamespace())
	glog.Debugf("ns: %s", ns)
	glog.Debugf("key: %s", util.ToPrintableAndHexString(msg.GetKey()))

	var ssmockinfo MockInfo
	if msg.GetOpCode() != proto.OpCodeMockSetParam {
		rh.mtx_g_mockinfo.RLock()
		ssmockinfo = rh.g_mockinfo[ns] // does it do a copy?
		rh.mtx_g_mockinfo.RUnlock()
	}

	// simulate timeout
	if msg.GetOpCode() != proto.OpCodeMockSetParam &&
		(msg.GetOpCode() == ssmockinfo.Opcode || ssmockinfo.Opcode == proto.OpCodeNop) &&
		ssmockinfo.Delay > 0 {
		time.Sleep(time.Microsecond * time.Duration(ssmockinfo.Delay))
		glog.Infof("simulate delay: %d Microsecond", ssmockinfo.Delay)
	}

	// simuate no response
	if (msg.GetOpCode() != proto.OpCodeMockSetParam &&
		msg.GetOpCode() == ssmockinfo.Opcode || ssmockinfo.Opcode == proto.OpCodeNop) &&
		ssmockinfo.NoResponse {
		glog.Infof("simulate drop response")
		return nil
	}

	msg.SetAsResponse()
	switch msg.GetOpCode() {
	case proto.OpCodeGet, proto.OpCodeRead:
		msg.SetOpStatus(proto.OpStatusNoError)

		var value []byte
		if (msg.GetOpCode() == ssmockinfo.Opcode || ssmockinfo.Opcode == proto.OpCodeNop) && ssmockinfo.Value != nil {
			value = ssmockinfo.Value
		} else {

			// TODO: copy the key to the begining of byte slice
			// note the value maybe be shared by multiple go routine, so make a copy
			if vsize > cap(rh.value) {
				value = rh.value[:cap(rh.value)]
			} else {
				value = rh.value[:vsize]
			}
		}
		var payload proto.Payload
		payload.SetWithClearValue(value)
		msg.SetPayload(&payload)

		msg.SetOpStatus(proto.OpStatusNoError)
		msg.SetCreationTime(ssmockinfo.CreationTime)
		msg.SetTimeToLive(1000)

	case proto.OpCodeCommit:
		msg.SetOpStatus(proto.OpStatusNoError)
		msg.ClearPayload()
		msg.SetCreationTime(uint32(time.Now().Unix()))
		glog.Debug("msg.GetTimeToLive under OpCodeCommit is", msg.GetTimeToLive())
		msg.SetTimeToLive(msg.GetTimeToLive())
		msg.SetVersion(msg.GetVersion())

	case proto.OpCodeCreate, proto.OpCodePrepareCreate,
		proto.OpCodeUpdate, proto.OpCodePrepareUpdate,
		proto.OpCodeSet, proto.OpCodePrepareSet,
		proto.OpCodePrepareDelete, proto.OpCodeDestroy,
		proto.OpCodeDelete,
		proto.OpCodeAbort,
		proto.OpCodeRepair,
		proto.OpCodeMarkDelete:

		msg.SetOpStatus(proto.OpStatusNoError)
		msg.SetCreationTime(ssmockinfo.CreationTime)
		msg.ClearPayload()

	case proto.OpCodeMockSetParam:

		value, e := msg.GetPayload().GetClearValue()
		if e != nil {
			return e
		}
		var mockinfo MockInfo
		mockinfo.Decode(bytes.NewBuffer(value))
		glog.Info(ssmockinfo)
		glog.Infof("MockSetParam namespace: %s", mockinfo.Namespace)

		rh.mtx_g_mockinfo.Lock()
		rh.g_mockinfo[mockinfo.Namespace] = mockinfo
		rh.mtx_g_mockinfo.Unlock()

		msg.SetOpStatus(proto.OpStatusNoError)
		msg.ClearPayload()

	case proto.OpCodeMockReSet:
		glog.Infof("OpCodeMockReSet")
		rh.mtx_g_mockinfo.Lock()
		for k := range rh.g_mockinfo {
			delete(rh.g_mockinfo, k)
		}
		rh.mtx_g_mockinfo.Unlock()

	default:
		glog.Infof("Error not handled operation: %s", msg.GetOpCodeText())
		msg.SetOpStatus(proto.OpStatusBadParam)
		msg.ClearPayload()
	}

	if msg.GetOpCode() != proto.OpCodeMockSetParam {
		if msg.GetOpCode() == ssmockinfo.Opcode || ssmockinfo.Opcode == proto.OpCodeNop {
			glog.Infof("%s %s", msg.GetOpCodeText(), proto.OpStatus(ssmockinfo.Status).String())
			if ssmockinfo.Status != uint8(proto.OpStatusNoError) {
				msg.SetOpStatus(proto.OpStatus(ssmockinfo.Status))
			}
			if ssmockinfo.Version != 0 {
				msg.SetVersion(ssmockinfo.Version)
			}
		}
	}
	resp, err := io.NewInboundResponseContext(&msg)
	reqCtx.Reply(resp)

	// ???
	reqCtx.OnComplete()
	return err
}

func NewMockStorageService(conf SSConfig, addrs ...string) *service.Service {
	glog.Info("Creating InProcess Mock SS Service")
	conf.SetListeners(addrs)
	s, _ := service.NewService(conf.Config, NewRequestHandler(&conf))
	return s
}

func (rh *RequestHandler) Init() {
}

func (rh *RequestHandler) Finish() {
}

func (rh *RequestHandler) GetReqCtxCreator() io.InboundRequestContextCreator {
	return io.ExtendedRequestContexCreator
}

func (rh *RequestHandler) OnKeepAlive(connector *io.Connector, reqCtx io.IRequestContext) (err error) {
	connector.OnKeepAlive()
	return
}
