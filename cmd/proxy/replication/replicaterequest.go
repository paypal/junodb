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
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/logging/otel"
	"juno/pkg/proto"
	"juno/pkg/proto/mayfly"
	"juno/pkg/util"
)

var (
	MAX_RETRY                int32         = 1
	MAX_RETRY_RECLOCK        int32         = 3
	REPLICATION_RESP_TIMEOUT time.Duration = 1000 * time.Millisecond

	_ repReqCtxCreatorI = (*repReqCreatorT)(nil)
	_ repReqCtxCreatorI = (*mayflyRepReqCreatorT)(nil)
)

type (
	repReqCreatorT struct {
		targetId string
	}

	RepRequestContext struct {
		util.QueItemBase
		targetId          string
		message           proto.RawMessage
		try_cnt           int32
		max_retry         int32
		timeReceived      time.Time
		recExpirationTime uint32                  //Unix Timestamp
		reqCh             chan io.IRequestContext // channel for retry
		calBuf            *logging.KeyValueBuffer
		this              io.IRequestContext
		dropCnt           *util.AtomicShareCounter
		errCnt            *util.AtomicShareCounter
	}

	mayflyRepRequestT struct {
		RepRequestContext
		mayflyMsg mayfly.Msg
	}
	mayflyRepReqCreatorT struct {
		targetId string
		ip       uint32
		port     uint16
	}
)

// xuli: revisit. may be better to set deadline when adding it to ringbuffer. Race condition may still exist.
// Need to consider how to make it consistant for outbound connection to SS and replication targets
func (r *repReqCreatorT) newRequestContext(recExpirationTime uint32, msg *proto.RawMessage, reqCh chan io.IRequestContext,
	dropCnt *util.AtomicShareCounter, errCnt *util.AtomicShareCounter) io.IRequestContext {
	ctx := &RepRequestContext{
		targetId:          r.targetId,
		try_cnt:           1,
		timeReceived:      time.Now(),
		recExpirationTime: recExpirationTime,
		reqCh:             reqCh,
		dropCnt:           dropCnt,
		errCnt:            errCnt,
	}
	ctx.this = ctx
	ctx.SetQueTimeout(REPLICATION_RESP_TIMEOUT)
	ctx.message.DeepCopy(msg)
	if cal.IsEnabled() {
		var request proto.OperationalMessage
		request.Decode(ctx.GetMessage())
		ctx.calBuf = logging.NewKVBuffer()
		ctx.calBuf.AddOpRequest(&request)
	}
	return ctx
}

func (r *repReqCreatorT) newKeepAliveRequestContext() io.IRequestContext {
	ctx := &keepAliveRequestContextT{}
	opmsg := &proto.OperationalMessage{}
	opmsg.SetOpCode(proto.OpCodeNop)
	opmsg.SetAsRequest()
	opmsg.SetAsReplication()
	opmsg.Encode(&ctx.message)
	return ctx
}

func (r *RepRequestContext) WriteWithOpaque(opaque uint32, w goio.Writer) (n int, err error) {
	var msg proto.RawMessage
	msg.ShallowCopy(&r.message)
	msg.SetOpaque(opaque)
	n, err = msg.Write(w)
	return
}

// To be implement
func (r *RepRequestContext) SetTimeout(parent context.Context, timeout time.Duration) {
}

func (r *RepRequestContext) GetMessage() *proto.RawMessage {
	return &r.message
}

func (r *RepRequestContext) GetCtx() context.Context {
	return nil
}

func (r *RepRequestContext) Cancel() {
}

func (r *RepRequestContext) Read(reader goio.Reader) (n int, err error) {
	// not implemented
	return 0, nil
}

func (r *RepRequestContext) complete(calstatus string, opStatus string, rht time.Duration, opCode string, target string) {

	if cal.IsEnabled() {
		var targetType string = logging.CalMsgTypeReplicate + target
		if !otel.IsEnabled() || calstatus != cal.StatusSuccess {
			cal.AtomicTransaction(targetType, opCode, calstatus, rht, r.calBuf.Bytes())
		}
	}

	otel.RecordReplication(opCode, opStatus, target, rht.Microseconds())

	r.this.OnComplete()
}

func (r *RepRequestContext) Reply(resp io.IResponseContext) {

	var retry int32 = 0
	status := resp.GetStatus()
	opstatus, _ := proto.GetOpStatus(resp.GetMessage())
	var statusText string
	var calStatusText string
	var rht time.Duration
	var opCodeText string
	var target string

	if cal.IsEnabled() {
		var request proto.OperationalMessage
		request.Decode(r.GetMessage())
		opCodeText = request.GetOpCodeText()

		if r.targetId == "" {
			target = ""
		} else {
			target = string("_") + r.targetId
		}
	}

	glog.Verbosef("receiving replication response: status=%d opstatus=%d",
		status, opstatus)

	if status != proto.StatusOk {
		retry = MAX_RETRY
		statusText = proto.StatusText(int(status))
		calStatusText = cal.StatusError
	} else {
		statusText = opstatus.String()
		calStatusText = logging.CalStatus(opstatus).CalStatus()
		if opstatus == proto.OpStatusRecordLocked {
			retry = MAX_RETRY_RECLOCK
		} else if opstatus == proto.OpStatusNoStorageServer ||
			opstatus == proto.OpStatusReqProcTimeout ||
			opstatus == proto.OpStatusBusy {
			retry = MAX_RETRY
		}
	}

	if cal.IsEnabled() {
		r.calBuf.AddDataTryNo(r.try_cnt)
		r.calBuf.AddStatus(statusText)
		rht = time.Since(r.timeReceived)
		r.calBuf.AddRequestHandleTime(int(rht / 1000))
	}

	resp.OnComplete()
	if retry == 0 {
		r.complete(calStatusText, opstatus.String(), rht, opCodeText, r.targetId)
		return
	}

	if r.max_retry == 0 {
		r.max_retry = retry
	}

	if r.try_cnt >= (r.max_retry + 1) {
		glog.Infof("max rep retry (%d) reached, drop req", r.try_cnt-1)
		if cal.IsEnabled() {
			var evType string = string("RR_Drop_MaxRetry") + target
			if !otel.IsEnabled() {
				cal.Event(evType, opCodeText, cal.StatusWarning, r.calBuf.Bytes())
			}
			r.calBuf.AddDropReason("MaxRetry")
		}
		otel.RecordCount(otel.RRDropMaxRetry, []otel.Tags{{"target", r.targetId}})
		r.errCnt.Add(1)
		r.complete(cal.StatusError, opstatus.String(), rht, opCodeText, r.targetId)
		return
	}

	now := time.Now()
	if r.recExpirationTime > uint32(now.Unix()) {

		r.try_cnt++

		select {
		case r.reqCh <- r.this:
		default:
			glog.Infof("replication queue full, drop the req, id=%d", r.this.GetId())
			if cal.IsEnabled() {
				var evType string = string("RR_Drop_QueueFull") + target
				if !otel.IsEnabled() {
					cal.Event(evType, opCodeText, cal.StatusWarning, r.calBuf.Bytes())
				}
				r.calBuf.AddDropReason("QueueFull")
			}
			otel.RecordCount(otel.RRDropQueueFull, []otel.Tags{{otel.Target, r.targetId}})
			r.dropCnt.Add(1)
			r.complete(cal.StatusError, opstatus.String(), rht, opCodeText, r.targetId)
		}
	} else {
		glog.Infof("req expired, id=%d", r.this.GetId())

		if cal.IsEnabled() {
			var evType string = "RR_Drop_RecExpired" + r.targetId
			if !otel.IsEnabled() {
				cal.Event(evType, opCodeText, cal.StatusSuccess, r.calBuf.Bytes())
			}
			r.calBuf.AddDropReason("RecExpired")
		}
		otel.RecordCount(otel.RRDropRecExpired, []otel.Tags{{otel.Target, r.targetId}})
		r.complete(cal.StatusSuccess, opstatus.String(), rht, opCodeText, r.targetId)
	}
}

func (r *RepRequestContext) OnComplete() {
	r.message.ReleaseBuffer()
}

func (r *RepRequestContext) OnCleanup() {
	glog.Verbosef("Juno RB cleanup: id=%d", r.GetId())
	resp := io.NewErrorOutboundResponse(proto.StatusRBCleanup)
	r.Reply(resp)
}

func (r *RepRequestContext) OnExpiration() {
	glog.Verbosef("Juno RB expire: id=%d", r.GetId())
	resp := io.NewErrorOutboundResponse(proto.StatusRBExpire)
	r.Reply(resp)
}

func (r *RepRequestContext) GetReceiveTime() time.Time {
	return r.timeReceived
}

func (c *mayflyRepReqCreatorT) newRequestContext(recExpirationTime uint32, msg *proto.RawMessage,
	reqCh chan io.IRequestContext, dropCnt *util.AtomicShareCounter, errCnt *util.AtomicShareCounter) io.IRequestContext {
	r := &mayflyRepRequestT{
		RepRequestContext: RepRequestContext{
			targetId:          c.targetId,
			try_cnt:           1,
			timeReceived:      time.Now(),
			recExpirationTime: recExpirationTime,
			reqCh:             reqCh,
			dropCnt:           dropCnt,
			errCnt:            errCnt,
		},
	}
	r.this = r
	r.message.DeepCopy(msg)
	r.SetQueTimeout(REPLICATION_RESP_TIMEOUT)

	var junoMsg proto.OperationalMessage
	junoMsg.Decode(msg)
	if cal.IsEnabled() {
		r.calBuf = logging.NewKVBuffer()
		r.calBuf.AddOpRequest(&junoMsg)
	}
	if err := mayfly.ToMayflyMsg(&r.mayflyMsg, &junoMsg); err == nil {
		r.mayflyMsg.SetRecipient(c.ip, c.port)
		r.mayflyMsg.ResetRequestId()
		r.mayflyMsg.SetAppName("junoproxy")
		if cal.IsEnabled() {
			b := logging.NewKVBuffer()
			b.Add([]byte("juno_rid"), junoMsg.GetRequestIDString())
			b.Add([]byte("mayfly_rid"), r.mayflyMsg.GetRequestIDString())
			cal.Event(logging.CalMsgTypeRidMapping, logging.CalMsgNameOutbound, cal.StatusSuccess, b.Bytes())
		}
	} else {
		glog.Error(err)
	}

	return r
}

func (r *mayflyRepReqCreatorT) newKeepAliveRequestContext() io.IRequestContext {
	ctx := &mayflyKeepAliveRequestContextT{}
	ctx.mayflyPingMsg.InitNOPRequest()
	ctx.mayflyPingMsg.SetRecipient(r.ip, r.port)
	return ctx
}

func (r *mayflyRepRequestT) WriteWithOpaque(opaque uint32, w goio.Writer) (n int, err error) {
	r.mayflyMsg.SetOpaque(opaque)

	var raw []byte
	if raw, err = r.mayflyMsg.Encode(); err == nil {
		n, err = w.Write(raw)
	}
	return
}
