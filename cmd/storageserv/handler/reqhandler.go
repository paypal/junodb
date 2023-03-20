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

package handler

import (
	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/storage"
	"juno/pkg/io"
	"juno/pkg/util"
)

var _ io.IRequestHandler = (*RequestHandler)(nil)

type RequestHandler struct {
	cnt                   util.AtomicCounter
	procPool              *storage.ReqProcCtxPool
	maxConcurrentRequests int32
}

func NewRequestHandler() *RequestHandler {
	rh := &RequestHandler{
		maxConcurrentRequests: int32(config.ServerConfig().MaxConcurrentRequests),
		procPool:              storage.NewReqProcCtxPool(int32(config.ServerConfig().ReqProcCtxPoolSize))}
	return rh
}

func (rh *RequestHandler) GetReqCtxCreator() io.InboundRequestContextCreator {
	glog.Verbosef("get ctx creator")
	return io.DefaultInboundRequestContexCreator
}

func (rh *RequestHandler) Process(reqCtx io.IRequestContext) error {

	glog.Verbosef("Process")
	if rh.cnt.Get() < rh.maxConcurrentRequests {
		rh.cnt.Add(1)
		proc := rh.procPool.Get()
		proc.Process(reqCtx)

		rh.cnt.Add(-1)
	}

	return nil
}

func (rh *RequestHandler) OnKeepAlive(connector *io.Connector, reqCtx io.IRequestContext) error {
	return nil
}

func (rh *RequestHandler) Init() {
}

func (rh *RequestHandler) Finish() {
}
