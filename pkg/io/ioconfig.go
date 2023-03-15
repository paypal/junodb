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

package io

import (
	"time"

	"juno/pkg/util"
)

var (
	DefaultInboundConfig = InboundConfig{
		HandshakeTimeout:     util.Duration{500 * time.Millisecond},
		IdleTimeout:          util.Duration{120 * time.Second},
		ReadTimeout:          util.Duration{500 * time.Millisecond},
		WriteTimeout:         util.Duration{500 * time.Millisecond},
		RequestTimeout:       util.Duration{600 * time.Millisecond},
		MaxBufferedWriteSize: 64 * 1024,
		IOBufSize:            64 * 1024, // default 64k buf size
		RespChanSize:         10000,
	}

	DefaultOutboundConfig = OutboundConfig{
		ConnectTimeout:        util.Duration{1 * time.Second},
		ConnectRecycleT:       util.Duration{30 * time.Second},
		GracefulShutdownTime:  util.Duration{200 * time.Millisecond},
		EnableConnRecycle:     false,
		ReqChanBufSize:        8092,
		MaxPendingQueSize:     8092,
		PendingQueExtra:       50,
		MaxBufferedWriteSize:  64 * 1024, // default 64k
		ReconnectIntervalBase: 100,       // 100ms
		ReconnectIntervalMax:  20000,     // 20 seconds
		NumConnsPerTarget:     1,
		IOBufSize:             64 * 1024, // default 64k buf size
	}
)

type (
	InboundConfig struct {
		HandshakeTimeout     util.Duration //only for TLS connection
		IdleTimeout          util.Duration
		ReadTimeout          util.Duration
		WriteTimeout         util.Duration
		RequestTimeout       util.Duration
		MaxBufferedWriteSize int
		IOBufSize            int
		RespChanSize         int
	}

	OutboundConfig struct {
		ConnectTimeout        util.Duration
		ConnectRecycleT       util.Duration
		GracefulShutdownTime  util.Duration
		EnableConnRecycle     bool
		ReqChanBufSize        int
		MaxPendingQueSize     int
		PendingQueExtra       int
		MaxBufferedWriteSize  int
		ReconnectIntervalBase int
		ReconnectIntervalMax  int
		NumConnsPerTarget     int32
		IOBufSize             int
	}
	InboundConfigMap  map[string]InboundConfig
	OutboundConfigMap map[string]OutboundConfig
)

func (conf *InboundConfig) SetDefaultIfNotDefined() (set bool) {
	if conf.HandshakeTimeout.Duration == 0 {
		set = true
		conf.HandshakeTimeout = DefaultInboundConfig.HandshakeTimeout
	}
	if conf.IdleTimeout.Duration == 0 {
		set = true
		conf.IdleTimeout = DefaultInboundConfig.IdleTimeout
	}

	if conf.ReadTimeout.Duration == 0 {
		set = true
		conf.ReadTimeout = DefaultInboundConfig.ReadTimeout
	}

	if conf.WriteTimeout.Duration == 0 {
		set = true
		conf.WriteTimeout = DefaultInboundConfig.WriteTimeout
	}

	if conf.RequestTimeout.Duration == 0 {
		set = true
		conf.RequestTimeout = DefaultInboundConfig.RequestTimeout
	}

	if conf.IdleTimeout.Duration < conf.RequestTimeout.Duration {
		set = true
		conf.IdleTimeout.Duration = 2 * conf.RequestTimeout.Duration
	}
	if conf.MaxBufferedWriteSize == 0 {
		set = true
		conf.MaxBufferedWriteSize = DefaultInboundConfig.MaxBufferedWriteSize
	}
	if conf.IOBufSize == 0 {
		set = true
		conf.IOBufSize = DefaultInboundConfig.IOBufSize
	}
	if conf.RespChanSize == 0 {
		set = true
		conf.RespChanSize = DefaultInboundConfig.RespChanSize
	}
	return
}

func (conf *OutboundConfig) SetDefaultIfNotDefined() (set bool) {
	if conf.ConnectTimeout.Duration == 0 {
		set = true
		conf.ConnectTimeout.Duration = DefaultOutboundConfig.ConnectTimeout.Duration
	}
	if conf.ConnectRecycleT.Duration == 0 {
		set = true
		conf.ConnectRecycleT.Duration = DefaultOutboundConfig.ConnectRecycleT.Duration
	}
	if conf.GracefulShutdownTime.Duration == 0 {
		set = true
		conf.GracefulShutdownTime.Duration = DefaultOutboundConfig.GracefulShutdownTime.Duration
	}
	if conf.ReqChanBufSize == 0 {
		set = true
		conf.ReqChanBufSize = DefaultOutboundConfig.ReqChanBufSize
	}
	if conf.MaxPendingQueSize == 0 {
		set = true
		conf.MaxPendingQueSize = DefaultOutboundConfig.MaxPendingQueSize
	}
	if conf.PendingQueExtra == 0 {
		set = true
		conf.PendingQueExtra = DefaultOutboundConfig.PendingQueExtra
	}
	if conf.MaxBufferedWriteSize == 0 {
		set = true
		conf.MaxBufferedWriteSize = DefaultOutboundConfig.MaxPendingQueSize
	}
	if conf.ReconnectIntervalBase == 0 {
		set = true
		conf.ReconnectIntervalBase = DefaultOutboundConfig.ReconnectIntervalBase
	}
	if conf.ReconnectIntervalMax == 0 {
		set = true
		conf.ReconnectIntervalMax = DefaultOutboundConfig.ReconnectIntervalMax
	}
	if conf.NumConnsPerTarget == 0 {
		set = true
		conf.NumConnsPerTarget = DefaultOutboundConfig.NumConnsPerTarget
	}
	if conf.IOBufSize == 0 {
		set = true
		conf.IOBufSize = DefaultOutboundConfig.IOBufSize
	}
	return
}

func (m *InboundConfigMap) SetDefaultIfNotDefined() {
	for k, v := range *m {
		if v.SetDefaultIfNotDefined() {
			(*m)[k] = v
		}
	}
}

func (m *OutboundConfigMap) SetDefaultIfNotDefined() {
	for k, v := range *m {
		if v.SetDefaultIfNotDefined() {
			(*m)[k] = v
		}
	}
}
