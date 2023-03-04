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
package proc

import (
	"juno/pkg/proto"
	"juno/pkg/udf"
	"juno/third_party/forked/golang/glog"
)

var _ IOnePhaseProcessor = (*UDFGetProcessor)(nil)

type UDFGetProcessor struct {
	GetProcessor
}

func NewUDFGetProcessor() *UDFGetProcessor {
	p := &UDFGetProcessor{
		GetProcessor: GetProcessor{
			OnePhaseProcessor: OnePhaseProcessor{
				ssRequestOpCode: proto.OpCodeRead,
			},
		},
	} //proto.OpCodeUDFGet
	p.self = p
	return p
}

func (p *UDFGetProcessor) Init() {
	p.GetProcessor.Init()
}

func (p *UDFGetProcessor) needApplyUDF() bool {
	return true
}
func (p *UDFGetProcessor) applyUDF(opmsg *proto.OperationalMessage) {
	mgr := udf.GetUDFManager()
	udfname := p.clientRequest.GetUDFName()

	if udf := mgr.GetUDF(string(udfname)); udf != nil {
		if res, err := udf.Call([]byte(""), opmsg.GetPayload().GetData(), p.clientRequest.GetPayload().GetData()); err == nil {
			opmsg.GetPayload().SetPayload(proto.PayloadTypeClear, res)
		} else {
			glog.Info("udf not exist")
		}
	}
}
