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
  
package proc

import (
	"juno/third_party/forked/golang/glog"

	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/proto"
)

var (
	LOG_ALERT   glog.Verbose
	LOG_WARN    glog.Verbose
	LOG_INFO    glog.Verbose
	LOG_DEBUG   glog.Verbose
	LOG_VERBOSE glog.Verbose
)

type LogLevel struct {
}

func (l *LogLevel) SetLevel() {
	LOG_ALERT = glog.V(1)
	LOG_WARN = glog.V(2)
	LOG_INFO = glog.V(3)
	LOG_DEBUG = glog.V(4)
	LOG_VERBOSE = glog.V(5)
}

var (
	level LogLevel
)

func init() {

	// TODO, get the current package name automatically
	glog.RegisterPackage("proc", &level)
}

const (
	kCalMsgTypeReqProc = "ReqProc"

	kReqTimeout     = "ReqTimeout"
	kReqCancelled   = "ReqCancelled"
	kSSReqTimeout   = "SSReqTimeout_"
	kDecrypt        = "Decrypt"
	kEncrypt        = "Encrypt"
	kInconsistent   = "Inconsistent"
	kRecVerOverflow = "RecVerOverflow"

	kBadParamInvalidKeyLen   = "BadParam_InvalidKeyLen"
	kBadParamInvalidNsLen    = "BadParam_invalidNsLen"
	kBadParamInvalidValueLen = "BadParam_InvalidValueLen"
	kBadParamInvalidTTL      = "BadParam_InvalidTTL"
)

var (
	logDataKeySS   []byte = []byte("ss")
	logDataKeyAddr []byte = []byte("addr")
)

func calNameReqTimeoutFor(op proto.OpCode) string {
	return kSSReqTimeout + op.String()
}

func writeBasicSSRequestInfo(b *logging.KeyValueBuffer, ssOp proto.OpCode, ssIndex int, ssAddr string, proc *ProcessorBase) {
	b.Add([]byte("op"), proc.logStrOpCode(ssOp))
	b.AddInt(logDataKeySS, ssIndex)
	b.Add(logDataKeyAddr, ssAddr)
	b.AddReqIdString(proc.requestID)
}

func calLogReqProcEvent(name string, data []byte) {
	cal.Event(kCalMsgTypeReqProc, name, cal.StatusSuccess, data)
}

func calLogReqProcError(name string, data []byte) {
	cal.Event(kCalMsgTypeReqProc, name, cal.StatusError, data)
}
