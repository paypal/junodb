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
package client

import (
	"crypto/tls"
	"errors"
	"fmt"
	"runtime"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/internal/cli"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
)

type clientImplT struct {
	config    Config
	appName   string
	namespace string
	processor *cli.Processor
}

func newProcessorWithConfig(conf *Config, getTLSConfig func() *tls.Config) *cli.Processor {
	if conf == nil {
		return nil
	}
	c := cli.NewProcessor(
		conf.Server,
		conf.Appname,
		conf.ConnPoolSize,
		conf.ConnectTimeout.Duration,
		conf.ResponseTimeout.Duration,
		getTLSConfig)
	return c
}

func NewWithTLS(conf Config, getTLSConfig func() *tls.Config) (IClient, error) {
	if conf.Server.SSLEnabled && getTLSConfig == nil {
		return nil, errors.New("getTLSConfig is nil.")
	}
	if err := conf.validate(true); err != nil {
		return nil, err
	}
	glog.Infof("client cfg=%v withTLS=%v", conf, getTLSConfig != nil)
	if conf.ConnPoolSize < 2 {
		conf.ConnPoolSize = 2
	}
	cli.SetDefaultRecycleTimeout(time.Duration(30 * time.Second))
	client := &clientImplT{
		config:    conf,
		processor: newProcessorWithConfig(&conf, getTLSConfig),
		appName:   conf.Appname,
		namespace: conf.Namespace,
	}
	if conf.Cal.Enabled {
		cal.InitWithConfig(&conf.Cal)
	}
	client.processor.Start()
	return client, nil
}

func New(conf Config) (IClient, error) {
	if err := conf.validate(false); err != nil {
		return nil, err
	}
	glog.Debugf("client cfg=%v", conf)
	if conf.ConnPoolSize <= 1 {
		conf.ConnPoolSize = 1
	}
	client := &clientImplT{
		config:    conf,
		processor: newProcessorWithConfig(&conf, nil),
		appName:   conf.Appname,
		namespace: conf.Namespace,
	}
	if conf.Cal.Enabled {
		cal.InitWithConfig(&conf.Cal)
	}
	client.processor.Start()
	if conf.ConnPoolSize == 1 {
		runtime.SetFinalizer(client.processor, func(p *cli.Processor) {
			p.Close()
		})
	}
	return client, nil
}

func (c *clientImplT) Close() {
	if c.processor != nil {
		c.processor.Close()
		c.processor = nil
	}
}

func (c *clientImplT) getOptions(opts ...IOption) *optionData {
	data := &optionData{}
	for _, op := range opts {
		op(data)
	}
	return data
}

func newContext(resp *proto.OperationalMessage) IContext {
	recInfo := &cli.RecordInfo{}
	recInfo.SetFromOpMsg(resp)
	return recInfo
}

func (c *clientImplT) logError(op string, err error) {
	if err == nil || err == ErrNoKey ||
		err == ErrConditionViolation ||
		err == ErrUniqueKeyViolation {
		return
	}

	addr_type := "tcp"
	if c.config.Server.SSLEnabled {
		addr_type = "ssl"
	}
	msg := fmt.Sprintf("[ERROR] op=%s %s_addr=%s response_timeout=%dms ns=%s. %s",
		op, addr_type, c.config.Server.Addr,
		c.config.ResponseTimeout.Nanoseconds()/int64(1e6), c.config.Namespace, err.Error())
	glog.Error(msg)
	if err == ErrBusy || err == ErrRecordLocked {
		time.Sleep(20 * time.Millisecond)
	}
}

func (c *clientImplT) Create(key []byte, value []byte, opts ...IOption) (context IContext, err error) {
	var resp *proto.OperationalMessage
	options := newOptionData(opts...)
	recInfo := &cli.RecordInfo{}
	context = recInfo
	request := c.NewRequest(proto.OpCodeCreate, key, value, options.ttl)
	if len(options.correlationId) > 0 {
		request.SetCorrelationID([]byte(options.correlationId))
	}
	resp, err = c.processor.ProcessRequest(request)
	if err == nil {
		err = checkResponse(request, resp, recInfo)
	}
	if err != nil {
		c.logError("Create", err)
	}
	return
}

func (c *clientImplT) Get(key []byte, opts ...IOption) (value []byte, context IContext, err error) {
	var resp *proto.OperationalMessage
	options := newOptionData(opts...)
	recInfo := &cli.RecordInfo{}
	context = recInfo
	request := c.NewRequest(proto.OpCodeGet, key, nil, options.ttl)
	if len(options.correlationId) > 0 {
		request.SetCorrelationID([]byte(options.correlationId))
	}
	resp, err = c.processor.ProcessRequest(request)
	if err == nil {
		err = checkResponse(request, resp, recInfo)
	}
	if err != nil {
		c.logError("Get", err)
		return
	}

	payload := resp.GetPayload()
	sz := payload.GetLength()
	if sz != 0 {
		value, err = payload.GetClearValue()
	}
	return
}

func (c *clientImplT) Update(key []byte, value []byte, opts ...IOption) (context IContext, err error) {
	var resp *proto.OperationalMessage
	options := newOptionData(opts...)
	recInfo := &cli.RecordInfo{}
	context = recInfo
	request := c.NewRequest(proto.OpCodeUpdate, key, value, options.ttl)
	if len(options.correlationId) > 0 {
		request.SetCorrelationID([]byte(options.correlationId))
	}
	if inCtx := options.context; inCtx != nil {
		if r, ok := inCtx.(*cli.RecordInfo); ok {
			r.SetRequestWithUpdateCond(request)
		}
	}
	resp, err = c.processor.ProcessRequest(request)
	if err == nil {
		err = checkResponse(request, resp, recInfo)
	}
	if err != nil {
		c.logError("Update", err)
	}
	return
}

func (c *clientImplT) Set(key []byte, value []byte, opts ...IOption) (context IContext, err error) {
	var resp *proto.OperationalMessage
	options := newOptionData(opts...)
	recInfo := &cli.RecordInfo{}
	context = recInfo
	request := c.NewRequest(proto.OpCodeSet, key, value, options.ttl)
	if len(options.correlationId) > 0 {
		request.SetCorrelationID([]byte(options.correlationId))
	}
	resp, err = c.processor.ProcessRequest(request)
	if err == nil {
		err = checkResponse(request, resp, recInfo)
	}
	if err != nil {
		c.logError("Set", err)
	}
	return
}

func (c *clientImplT) Destroy(key []byte, opts ...IOption) (err error) {

	var resp *proto.OperationalMessage
	options := newOptionData(opts...)
	request := c.NewRequest(proto.OpCodeDestroy, key, nil, 0)
	if len(options.correlationId) > 0 {
		request.SetCorrelationID([]byte(options.correlationId))
	}
	resp, err = c.processor.ProcessRequest(request)
	if err == nil {
		err = checkResponse(request, resp, nil)
	}
	if err != nil {
		c.logError("Destroy", err)
	}
	return
}

func (c *clientImplT) UDFGet(key []byte, fname []byte, params []byte, opts ...IOption) (value []byte, context IContext, err error) {
	var resp *proto.OperationalMessage
	options := newOptionData(opts...)
	recInfo := &cli.RecordInfo{}
	context = recInfo
	request := c.NewUDFRequest(proto.OpCodeUDFGet, key, fname, params, options.ttl)
	if len(options.correlationId) > 0 {
		request.SetCorrelationID([]byte(options.correlationId))
	}

	if resp, err = c.processor.ProcessRequest(request); err == nil {
		if err = checkResponse(request, resp, recInfo); err == nil {
			payload := resp.GetPayload()
			sz := payload.GetLength()
			if sz != 0 {
				value, err = payload.GetClearValue()
			}
		} else {
			glog.Debug(err)
		}
	}
	return
}

func (c *clientImplT) UDFSet(key []byte, fname []byte, params []byte, opts ...IOption) (context IContext, err error) {
	var resp *proto.OperationalMessage
	options := newOptionData(opts...)
	recInfo := &cli.RecordInfo{}
	context = recInfo
	request := c.NewUDFRequest(proto.OpCodeUDFSet, key, fname, params, options.ttl)
	if len(options.correlationId) > 0 {
		request.SetCorrelationID([]byte(options.correlationId))
	}

	if resp, err = c.processor.ProcessRequest(request); err == nil {
		if err = checkResponse(request, resp, recInfo); err != nil {
			glog.Debug(err)
		}
	}
	return
}

///TODO temporary
func (c *clientImplT) Batch(requests []*proto.OperationalMessage) (responses []*proto.OperationalMessage, err error) {
	return c.processor.ProcessBatchRequests(requests)
}

func (c *clientImplT) NewRequest(op proto.OpCode, key []byte, value []byte, ttl uint32) (request *proto.OperationalMessage) {
	///TODO: validate op
	request = &proto.OperationalMessage{}
	var payload proto.Payload
	payload.SetWithClearValue(value)
	if ttl == 0 && op == proto.OpCodeCreate {
		ttl = uint32(c.config.DefaultTimeToLive)
	}
	request.SetRequest(op, key, []byte(c.namespace), &payload, ttl)
	request.SetNewRequestID()
	return
}

func (c *clientImplT) NewUDFRequest(op proto.OpCode, key []byte, fname []byte, params []byte, ttl uint32) (request *proto.OperationalMessage) {
	///TODO: validate op
	request = &proto.OperationalMessage{}
	var payload proto.Payload
	payload.SetWithClearValue(params)
	if ttl == 0 {
		ttl = uint32(c.config.DefaultTimeToLive)
	}
	request.SetRequest(op, key, []byte(c.namespace), &payload, ttl)
	request.SetNewRequestID()
	request.SetUDFName(fname)

	return
}

func checkResponse(request *proto.OperationalMessage, response *proto.OperationalMessage, recInfo *cli.RecordInfo) (err error) {
	opCode := request.GetOpCode()
	if opCode != response.GetOpCode() {
		err = fmt.Errorf("opcode mismatch: %s - %s", opCode.String(), response.GetOpCodeText())
		return
	}
	if recInfo != nil {
		recInfo.SetFromOpMsg(response)
	}
	status := response.GetOpStatus()
	if glog.LOG_DEBUG {
		b := logging.NewKVBufferForLog()
		b.AddOpRequestResponseInfo(request, response)

		glog.Debugf("%s %s", opCode.String(), b.String())
	}

	var ok bool
	if err, ok = errorMapping[status]; !ok {
		err = ErrInternal
	}
	return
}
