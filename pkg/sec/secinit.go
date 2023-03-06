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
  
package sec

import (
	"fmt"
	"sync"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/initmgr"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
)

const (
	KFlagServerTlsEnabled  Flag = 0x1
	KFlagClientTlsEnabled  Flag = 0x2
	KFlagEncryptionEnabled Flag = 0x4
)

var (
	Initializer initmgr.IInitializer = initmgr.NewInitializer(Initialize, Finalize)
	ExitChan    chan bool            = make(chan bool)
)

var (
	gRwCtxMtx  sync.RWMutex
	gSvrTlsCtx tlsContextI
	gCliTlsCtx tlsContextI
)

var (
	gSecFlag    Flag
	gChShutdown chan bool = make(chan bool)
)

var (
	funcNewTlsContext func(isServer bool, certPEMBlk []byte, keyPEMBlk []byte, ks proto.IEncryptionKeyStore, done chan bool) (tlsContextI, error) = newGoTlsContext
)

type Flag uint8

/*
	Two arguments required
	arg 0: *Config
	arg 1: uint8 bitmask flag
	arg 2: isServerManager (optional)
*/
func Initialize(args ...interface{}) (err error) {
	var cfg *Config
	var flag Flag
	var isServerManager bool
	var ok bool

	if len(args) < 2 {
		err = fmt.Errorf("two arguments required")
		return
	}
	if cfg, ok = args[0].(*Config); !ok {
		err = fmt.Errorf("*Config argument expected")
		return
	}
	if flag, ok = args[1].(Flag); !ok {
		err = fmt.Errorf("uint8 bitmask argument expected")
		return
	}
	if len(args) > 2 {
		if isServerManager, ok = args[2].(bool); !ok {
			err = fmt.Errorf("bool argument expected")
			return
		}
	}

	err = initializeSec(cfg, flag, isServerManager)
	return
}

func Finalize() {
	close(gChShutdown)
}

func initializeSec(cfg *Config, flag Flag, isServerManager bool) (err error) {
	defer func() {
		if err != nil {
			//put the cal logging here for the time being. It only works if cal has been initialized
			cal.Event(logging.CalMsgTypeJunoSec, "InitError", cal.StatusError, []byte(err.Error()))
		}
	}()

	if err = InitSecConfig(cfg); err != nil { // ####
		return
	}
	gSecFlag = flag // ###
	funcNewTlsContext = newGoTlsContext

	var ks proto.IEncryptionKeyStore
	if flag != 0 {
		var err error
		ks, err = initLocalFileStore(cfg)
		if err == nil {
			proto.InitializeKeyStore(proto.PayloadTypeEncryptedByProxy, ks)
		} else {
			glog.Errorln(err)
			cal.Event(logging.CalMsgTypeJunoSec, logging.CalMsgNameGetEncrypKey, cal.StatusError, nil)
		}
	}

	if flag&(KFlagServerTlsEnabled|KFlagClientTlsEnabled) != 0 {
		localFileProtected := &localFileProtectedT{}
		var certPEMBlock, keyPEMBlock []byte
		if certPEMBlock, keyPEMBlock, err = localFileProtected.getCertAndKeyPemBlock(cfg); err != nil {
			glog.Errorln(err)
			return
		}
		if flag&KFlagServerTlsEnabled != 0 {
			if gSvrTlsCtx, err = funcNewTlsContext(true, certPEMBlock, keyPEMBlock, ks, ExitChan); err != nil {
				glog.Infoln(err)
				return
			}
		}
		if flag&KFlagClientTlsEnabled != 0 {
			if gCliTlsCtx, err = funcNewTlsContext(false, certPEMBlock, keyPEMBlock, ks, ExitChan); err != nil {
				glog.Infoln(err)
				return
			}
		}
	}
	return
}
