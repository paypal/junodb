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
	"juno/cmd/proxy/stats"
	"juno/pkg/proto"
	"juno/pkg/util"
)

type ReqProcessorPool struct {
	procPool *util.ChanPool
	maxCount int32
	curCount *util.AtomicCounter
}

func NewRequestProcessorPool(chansize int32, maxsize int32, op proto.OpCode) *ReqProcessorPool {

	procPool := util.NewChanPool(int(chansize), func() interface{} {
		var p IRequestProcessor

		switch op {
		case proto.OpCodeCreate:
			p = NewCreateProcessor()
		case proto.OpCodeGet:
			p = NewGetProcessor()
		case proto.OpCodeUpdate:
			p = NewUpdateProcessor()
		case proto.OpCodeSet:
			p = NewSetProcessor()
		case proto.OpCodeDestroy:
			p = newDestroyRequestProcessor()
		case proto.OpCodeUDFGet:
			p = NewUDFGetProcessor()
		case proto.OpCodeUDFSet:
			p = NewSetProcessor()
			//p = NewUDFSetProcessor()
		default:
			return nil
		}
		p.Init()
		return p
	})

	var counter *util.AtomicCounter
	switch op {
	case proto.OpCodeCreate:
		counter = stats.GetActiveCreateCounter()
	case proto.OpCodeGet:
		counter = stats.GetActiveGetCounter()
	case proto.OpCodeUpdate:
		counter = stats.GetActiveUpdateCounter()
	case proto.OpCodeSet:
		counter = stats.GetActiveSetCounter()
	case proto.OpCodeDestroy:
		counter = stats.GetActiveDestroyCounter()
	case proto.OpCodeUDFGet:
		counter = stats.GetActiveUDFGetCounter()
	case proto.OpCodeUDFSet:
		counter = stats.GetActiveUDFSetCounter()
	default:
	}

	return &ReqProcessorPool{procPool, maxsize, counter}
}

func (p *ReqProcessorPool) GetProcessor() IRequestProcessor {

	// reached absolute max, should reject or queue request
	if p.GetCount() >= p.maxCount {
		return nil
	}

	if p.curCount != nil {
		p.curCount.Add(1)
	}
	return p.procPool.Get().(IRequestProcessor)
}

func (p *ReqProcessorPool) PutProcessor(proc IRequestProcessor) {
	proc.Init()
	p.procPool.Put(proc)
	if p.curCount != nil {
		p.curCount.Add(-1)
	}
}

func (p *ReqProcessorPool) DecreaseCount() {
	if p.curCount != nil {
		p.curCount.Add(-1)
	}
}

func (p *ReqProcessorPool) GetCount() int32 {
	if p.curCount != nil {
		return p.curCount.Get()
	} else {
		return 0
	}
}
