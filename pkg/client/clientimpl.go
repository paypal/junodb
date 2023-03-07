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
package client

import (
	"fmt"
	"runtime"

	"juno/third_party/forked/golang/glog"

	"juno/internal/cli"
	"juno/pkg/io"
	"juno/pkg/logging"
	"juno/pkg/proto"
)

type clientImplT struct {
	config    Config
	appName   string
	namespace string
	processor *cli.Processor
}

func newProcessorWithConfig(conf *Config) *cli.Processor {
	if conf == nil {
		return nil
	}
	c := cli.NewProcessor(
		conf.Server,
		conf.Appname,
		conf.ConnectTimeout.Duration,
		conf.RequestTimeout.Duration,
		conf.ConnRecycleTimeout.Duration)
	return c
}

func New(conf Config) (IClient, error) {
	if err := conf.validate(); err != nil {
		return nil, err
	}
	client := &clientImplT{
		config:    conf,
		processor: newProcessorWithConfig(&conf),
		appName:   conf.Appname,
		namespace: conf.Namespace,
	}
	client.processor.Start()
	runtime.SetFinalizer(client.processor, func(p *cli.Processor) {
		p.Close()
	})
	return client, nil
}

func NewClient(server string, ns string, app string) (IClient, error) {
	c := &clientImplT{
		config: Config{
			Server:            io.ServiceEndpoint{Addr: server, SSLEnabled: false},
			Namespace:         ns,
			Appname:           app,
			RetryCount:        defaultConfig.RetryCount,
			DefaultTimeToLive: defaultConfig.DefaultTimeToLive,
			ConnectTimeout:    defaultConfig.ConnectTimeout,
			ReadTimeout:       defaultConfig.ReadTimeout,
			WriteTimeout:      defaultConfig.WriteTimeout,
			RequestTimeout:    defaultConfig.RequestTimeout,
		},
		appName:   app,
		namespace: ns,
	}
	c.processor = newProcessorWithConfig(&c.config)
	if c.processor != nil {
		c.processor.Start()
	} else {
		errstr := "fail to create processor"
		glog.Error(errstr)
		return nil, fmt.Errorf(errstr)
	}
	runtime.SetFinalizer(c.processor, func(p *cli.Processor) {
		p.Close()
	})
	return c, nil
}

///TODO to revisit
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

func (c *clientImplT) Create(key []byte, value []byte, opts ...IOption) (context IContext, err error) {
	glog.Verbosef("Create ")
	var resp *proto.OperationalMessage
	options := newOptionData(opts...)
	recInfo := &cli.RecordInfo{}
	context = recInfo
	request := c.NewRequest(proto.OpCodeCreate, key, value, options.ttl)
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

func (c *clientImplT) Get(key []byte, opts ...IOption) (value []byte, context IContext, err error) {
	var resp *proto.OperationalMessage
	options := newOptionData(opts...)
	recInfo := &cli.RecordInfo{}
	context = recInfo
	request := c.NewRequest(proto.OpCodeGet, key, nil, options.ttl)
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
	if resp, err = c.processor.ProcessRequest(request); err == nil {
		if err = checkResponse(request, resp, recInfo); err != nil {
			glog.Debug(err)
		}
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
	if resp, err = c.processor.ProcessRequest(request); err == nil {
		if err = checkResponse(request, resp, recInfo); err != nil {
			glog.Debug(err)
		}
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
	if resp, err = c.processor.ProcessRequest(request); err == nil {
		if err = checkResponse(request, resp, nil); err != nil {
			glog.Debug(err)
		}
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
	request.SetRequest(op, key, []byte(c.namespace), &payload, ttl)
	request.SetNewRequestID()
	return
}

func (c *clientImplT) NewUDFRequest(op proto.OpCode, key []byte, fname []byte, params []byte, ttl uint32) (request *proto.OperationalMessage) {
	///TODO: validate op
	request = &proto.OperationalMessage{}
	var payload proto.Payload
	payload.SetWithClearValue(params)
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
