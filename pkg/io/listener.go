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
	"context"
	"net"
	"os"
	"time"

	"juno/pkg/logging/cal"
	"juno/pkg/logging/otel"
	"juno/pkg/util"
	"juno/third_party/forked/golang/glog"
)

const (
	ListenerTypeTCP = ListenerType(iota)
	ListenerTypeTCPwSSL
)

type (
	InboundRequestContextCreator func(magic []byte, c *Connector) (ctx IRequestContext, err error)
	ListenerType                 byte

	IListener interface {
		GetName() string
		GetType() ListenerType
		AcceptAndServe() error
		Close() error
		Shutdown()
		WaitForShutdownToComplete(time.Duration)
		GetConnString() string
		GetNumActiveConnections() uint32
		Refresh()
	}

	Listener struct {
		config      ListenerConfig
		ioConfig    InboundConfig
		netListener net.Listener
		reqHandler  IRequestHandler
		connMgr     *InboundConnManager
		lsnrType    ListenerType
	}
)

var (
	supportedListenerNetworks map[string]bool = make(map[string]bool)
)

func init() {
	supportedListenerNetworks["tcp"] = true
}

func NewListener(cfg ListenerConfig, iocfg InboundConfig, reqHandler IRequestHandler) (lsnr IListener, err error) {
	ln := &Listener{
		config:     cfg,
		reqHandler: reqHandler,
		ioConfig:   iocfg,
		connMgr: &InboundConnManager{
			activeConns: make(map[*Connector]struct{}),
		},
	}
	ln.ioConfig.SetDefaultIfNotDefined()
	if len(ln.config.Network) == 0 {
		ln.config.Network = "tcp"
	}
	if ln.netListener, err = net.Listen(ln.config.Network, ln.config.Addr); err == nil {
		if cfg.SSLEnabled {
			sslLsnr := &SslListener{
				Listener: *ln,
			}
			sslLsnr.lsnrType = ListenerTypeTCPwSSL
			lsnr = sslLsnr
		} else {
			lsnr = ln
		}
	}

	return
}

func NewListenerWithFd(cfg ListenerConfig, iocfg InboundConfig, f *os.File, reqHandler IRequestHandler) (lsnr IListener, err error) {
	ln := &Listener{
		config:     cfg,
		reqHandler: reqHandler,
		ioConfig:   iocfg,
		connMgr: &InboundConnManager{
			activeConns: make(map[*Connector]struct{}),
		},
	}
	ln.ioConfig.SetDefaultIfNotDefined()
	if len(ln.config.Network) == 0 {
		ln.config.Network = "tcp"
	}
	if ln.netListener, err = net.FileListener(f); err == nil {
		if cfg.SSLEnabled {
			sslLsnr := &SslListener{
				Listener: *ln,
			}
			sslLsnr.lsnrType = ListenerTypeTCPwSSL
			lsnr = sslLsnr
		} else {
			lsnr = ln
		}
	}
	return
}

func (l *Listener) Close() error {
	return l.netListener.Close()
}

func (l *Listener) Shutdown() {
	l.netListener.Close()
	l.connMgr.Shutdown()
}

func (l *Listener) WaitForShutdownToComplete(timeout time.Duration) {
	l.connMgr.WaitForShutdownToComplete(timeout)
}

func (l *Listener) AcceptAndServe() error {
	conn, err := l.netListener.Accept()

	if err == nil {
		if cal.IsEnabled() {
			raddr := conn.RemoteAddr().String()
			if rhost, _, e := net.SplitHostPort(raddr); e == nil {
				cal.Event(cal.TxnTypeAccept, rhost, cal.StatusSuccess, []byte("raddr="+raddr+"&laddr="+conn.LocalAddr().String()))
			}
		}
		otel.RecordCount(otel.Accept, []otel.Tags{{otel.Status, otel.Success}})
		l.startNewConnector(conn)
	} else {
		otel.RecordCount(otel.Accept, []otel.Tags{{otel.Status, otel.Error}})
	}
	//log the error in caller if needed
	return err
}

func (l *Listener) startNewConnector(conn net.Conn) {
	ctx, cancel := context.WithCancel(context.Background())
	bufSize := l.ioConfig.IOBufSize
	if bufSize == 0 {
		bufSize = 64000
	}
	connector := &Connector{
		conn:       conn,
		reader:     util.NewBufioReader(conn, bufSize),
		ctx:        ctx,
		cancelCtx:  cancel,
		chResponse: make(chan IResponseContext, l.ioConfig.RespChanSize),
		chStop:     make(chan struct{}),
		connMgr:    l.connMgr,
		reqHandler: l.reqHandler,
		config:     l.ioConfig,
		pendingReq: 0,
		//		reqCounter:    server.GetReqCounter(),
		reqCtxCreator: l.reqHandler.GetReqCtxCreator(),
		lsnrType:      l.GetType(),
	}
	if connector.reqCtxCreator == nil {
		connector.reqCtxCreator = DefaultInboundRequestContexCreator
	}
	connector.Start()
}

func (l *Listener) GetType() ListenerType {
	return l.lsnrType
}

func (l *Listener) GetName() string {
	if len(l.config.Name) != 0 {
		return l.config.Name
	}
	return l.config.GetConnString()
}

func (l *Listener) GetConnString() string {
	return l.config.GetConnString()
}

func (l *Listener) GetNumActiveConnections() uint32 {
	if l.connMgr != nil {
		return l.connMgr.GetNumActiveConnections()
	}
	return 0
}

func (l *Listener) Refresh() {

	var err error
	l.netListener, err = net.Listen(l.config.Network, l.config.Addr)
	if err != nil {
		glog.Error(err)
	}
}
