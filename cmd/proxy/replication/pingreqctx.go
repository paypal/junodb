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
  
package replication

import (
	"context"
	goio "io"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/io"
	"juno/pkg/proto"
	"juno/pkg/proto/mayfly"
	"juno/pkg/util"
)

type (
	keepAliveRequestContextT struct {
		util.QueItemBase
		message proto.RawMessage
	}
	mayflyKeepAliveRequestContextT struct {
		keepAliveRequestContextT
		mayflyPingMsg mayfly.Msg
	}
)

func (c *keepAliveRequestContextT) GetMessage() *proto.RawMessage {
	return &c.message
}

func (c *keepAliveRequestContextT) GetCtx() context.Context {
	return nil
}

func (c *keepAliveRequestContextT) Cancel() {
}

func (c *keepAliveRequestContextT) Read(r goio.Reader) (n int, err error) {
	n, err = c.message.Read(r)
	return
}

func (c *keepAliveRequestContextT) WriteWithOpaque(opaque uint32, w goio.Writer) (n int, err error) {
	var msg proto.RawMessage
	msg.ShallowCopy(&c.message)
	msg.SetOpaque(opaque)
	n, err = msg.Write(w)
	return
}

func (c *keepAliveRequestContextT) Reply(resp io.IResponseContext) {
	glog.Verbosef("KeepAlive Response")
}

func (c *keepAliveRequestContextT) OnComplete() {
    c.message.ReleaseBuffer()
}

func (c *keepAliveRequestContextT) OnCleanup() {
}

func (c *keepAliveRequestContextT) OnExpiration() {
}

func (c *keepAliveRequestContextT) GetReceiveTime() time.Time {
	return time.Time{}
}

func (c *keepAliveRequestContextT) SetTimeout(parent context.Context, timeout time.Duration) {
}

func (c *mayflyKeepAliveRequestContextT) WriteWithOpaque(opaque uint32, w goio.Writer) (n int, err error) {
	c.mayflyPingMsg.SetOpaque(opaque)
	var raw []byte
	if raw, err = c.mayflyPingMsg.Encode(); err == nil {
		n, err = w.Write(raw)
	}
	return
}
