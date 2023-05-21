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

package redist

import (
	"context"
	"io"
	"time"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	. "github.com/paypal/junodb/pkg/io"
	"github.com/paypal/junodb/pkg/proto"
	redistst "github.com/paypal/junodb/pkg/stats/redist"
	"github.com/paypal/junodb/pkg/util"
)

type RedistRequestContext struct {
	util.QueItemBase
	message      proto.RawMessage
	retry_cnt    uint16
	timeReceived time.Time
	reqCh        chan IRequestContext // channel for retry
	stats        *redistst.Stats
}

func NewRedistRequestContext(msg *proto.RawMessage,
	reqCh chan IRequestContext, stats *redistst.Stats) *RedistRequestContext {
	r := &RedistRequestContext{
		retry_cnt:    0,
		timeReceived: time.Now(),
		reqCh:        reqCh,
		stats:        stats,
	}
	r.SetQueTimeout(RedistConfig.RedistRespTimeout.Duration)
	r.message.DeepCopy(msg)
	return r
}

// To be implement
func (r *RedistRequestContext) SetTimeout(parent context.Context, timeout time.Duration) {
}

func (r *RedistRequestContext) GetMessage() *proto.RawMessage {
	return &r.message
}

func (r *RedistRequestContext) GetCtx() context.Context {
	return nil
}

func (r *RedistRequestContext) Cancel() {
}

func (r *RedistRequestContext) Read(reader io.Reader) (n int, err error) {
	// not implemented
	return 0, nil
}

func (r *RedistRequestContext) WriteWithOpaque(opaque uint32, w io.Writer) (n int, err error) {
	var msg proto.RawMessage
	msg.ShallowCopy(&r.message)
	msg.SetOpaque(opaque)
	n, err = msg.Write(w)
	return
}

func (r *RedistRequestContext) Reply(resp IResponseContext) {
	retry := false
	status := resp.GetStatus()
	opstatus, _ := proto.GetOpStatus(resp.GetMessage())

	glog.Verbosef("receiving replication response: status=%d opstatus=%d",
		status, opstatus)

	if status != proto.StatusOk {
		retry = true
	} else if opstatus == proto.OpStatusNoError || opstatus == proto.OpStatusVersionConflict {
		r.stats.IncreaseOkCnt()
	} else if opstatus == proto.OpStatusRecordLocked ||
		opstatus == proto.OpStatusNoStorageServer ||
		opstatus == proto.OpStatusReqProcTimeout ||
		opstatus == proto.OpStatusSSOutofResource {
		retry = true
	} else {
		glog.Infof("redist failed: status=%d opstatus=%d", status, opstatus)
		r.stats.IncreaseFailCnt()
	}

	if !retry {
		r.OnComplete()
		return
	}

	if r.retry_cnt >= RedistConfig.MaxRetry {
		glog.Infof("max Redistribution retry (%d) reached, drop the msg",
			r.retry_cnt)
		r.stats.IncreaseFailCnt()
		r.OnComplete()
		return
	}

	r.retry_cnt++

	select {
	case r.reqCh <- r:
	default:
		r.stats.IncreaseDropCnt()
		glog.Infof("Redistribution queue full, drop the req")
		r.OnComplete()
	}
}

func (r *RedistRequestContext) OnComplete() {
	r.message.ReleaseBuffer()
}

func (r *RedistRequestContext) OnCleanup() {
	glog.Infof("queue full, drop the req")
	r.stats.IncreaseDropCnt()
}

func (r *RedistRequestContext) OnExpiration() {
	glog.Infof("Request timed out, drop the req")
	r.stats.IncreaseDropCnt()
}

func (r *RedistRequestContext) GetReceiveTime() time.Time {
	return r.timeReceived
}
