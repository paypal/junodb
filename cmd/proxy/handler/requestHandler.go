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
package handler

import (
	"os"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/config"
	"juno/cmd/proxy/proc"
	"juno/cmd/proxy/stats"
	"juno/pkg/io"
	"juno/pkg/net/netutil"
	"juno/pkg/proto"
	"juno/pkg/service"
)

var _ io.IRequestHandler = (*RequestHandler)(nil)

type RequestHandler struct {
	procPools []*proc.ReqProcessorPool
}

func NewRequestHandler() *RequestHandler {
	numProcType := proto.OpCodeLastProxyOp - proto.OpCodeNop
	rh := &RequestHandler{make([]*proc.ReqProcessorPool, numProcType)}
	for op := proto.OpCodeCreate; op < proto.OpCodeLastProxyOp; op++ {
		rh.procPools[op] = proc.NewRequestProcessorPool(
			int32(config.Conf.ReqProcessorPoolSize),
			int32(config.Conf.MaxNumReqProcessors),
			op)
	}

	return rh
}

func (rh *RequestHandler) GetReqCtxCreator() io.InboundRequestContextCreator {
	return io.ExtendedRequestContexCreator
}

func (rh *RequestHandler) GetProcessor(op proto.OpCode) proc.IRequestProcessor {
	procPool := rh.procPools[op]
	if procPool == nil {
		return nil
	}
	return procPool.GetProcessor()
}

func (rh *RequestHandler) PutProcessor(op proto.OpCode, p proc.IRequestProcessor) {
	procPool := rh.procPools[op]
	if procPool != nil {
		procPool.PutProcessor(p)
	}
}

func (rh *RequestHandler) decreaseProcPoolCount(op proto.OpCode) {
	procPool := rh.procPools[op]
	if procPool != nil {
		procPool.DecreaseCount()
	}
}

func (rh *RequestHandler) Process(reqCtx io.IRequestContext) error {
	wmsg := reqCtx.GetMessage()

	op, err := proto.GetOpCode(wmsg)
	if err != nil {
		glog.Error("Cannot get Opcode: ", err)
		return err
	}

	if op == proto.OpCodeNop {
		return rh.ProcessNoop(reqCtx)
	}

	if op >= proto.OpCodeLastProxyOp {
		glog.Error("wrong opcode: ", op)
		rh.ReplyStatus(op, reqCtx, proto.OpStatusNotSupported)
		return nil
	}

	processor := rh.GetProcessor(op)
	if processor == nil {
		glog.Error("Cannot get processor Opcode: ", op)
		return nil
	}

	if processor.Process(reqCtx) {
		rh.PutProcessor(op, processor)
	} else {
		/// not put back to the processor pool, as the channel(s) might still be referenced by OutboundProcessor.
		rh.decreaseProcPoolCount(op)
	}
	return nil
}

func (rh *RequestHandler) ReplyStatus(opCode proto.OpCode, reqCtx io.IRequestContext, st proto.OpStatus) (err error) {
	var opmsg proto.OperationalMessage
	wmsg := reqCtx.GetMessage()

	if err := opmsg.Decode(wmsg); err == nil {
		var rawResp proto.RawMessage
		opmsg.SetAsResponse()
		opmsg.SetOpStatus(st)

		opmsg.Encode(&rawResp)
		resp := io.NewInboundRespose(opCode, &rawResp)
		reqCtx.Reply(resp)
	} else {
		glog.Warningf("failed to decode message: %s", err.Error())
		reqCtx.Reply(io.NewInboundRespose(opCode, reqCtx.GetMessage()))
	}
	return nil
}

func (rh *RequestHandler) ProcessNoop(reqCtx io.IRequestContext) (err error) {
	var opmsg proto.OperationalMessage
	wmsg := reqCtx.GetMessage()

	// Using AppName from request is to avoid reverting a client side change for the time being.
	if err := opmsg.Decode(wmsg); err == nil {
		var rawResp proto.RawMessage
		opmsg.SetAsResponse()

		ip := netutil.GetLocalIPv4Address()
		if !ip.IsLoopback() && config.Conf.BypassLTMEnabled {
			opmsg.SetSource(ip, 0, opmsg.GetAppName())
		}

		opmsg.Encode(&rawResp)
		resp := io.NewInboundRespose(proto.OpCodeNop, &rawResp)
		reqCtx.Reply(resp)
	} else {
		glog.Warningf("failed to decode Nop message: %s", err.Error())
		reqCtx.Reply(io.NewInboundRespose(proto.OpCodeNop, reqCtx.GetMessage()))
	}

	return nil
}

func (rh *RequestHandler) OnKeepAlive(connector *io.Connector, reqCtx io.IRequestContext) (err error) {
	rh.ProcessNoop(reqCtx)
	connector.OnKeepAlive()
	return
}

func (rh *RequestHandler) Init() {
	proc.InitConfig()
}

func (rh *RequestHandler) Finish() {
}

func NewProxyService(conf *config.Config) *service.Service {
	s, _ := service.NewService(conf.Config, NewRequestHandler())

	stats.SetListeners(s.GetListeners())
	return s
}

func NewProxyServiceWithListenFd(conf *config.Config, limiter service.ILimiter, fds ...*os.File) *service.Service {
	s := service.NewWithLimiterAndListenFd(conf.Config, NewRequestHandler(), limiter, fds...)
	stats.SetListeners(s.GetListeners())
	return s
}
