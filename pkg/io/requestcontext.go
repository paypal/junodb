package io

import (
	"bytes"
	"context"
	"errors"
	"io"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/proto"
	"juno/pkg/proto/mayfly"
	"juno/pkg/util"
)

var oResponsePool = util.NewChanPool(10000, func() interface{} {
	return new(OutboundResponseContext)
})

type (
	IResponseContext interface {
		GetStatus() uint32
		GetMessage() *proto.RawMessage
		GetMsgSize() uint32
		OnComplete()
		Read(r io.Reader) (n int, err error)
		Write(w io.Writer) (n int, err error)
	}

	IRequestContext interface {
		util.QueItem
		GetMessage() *proto.RawMessage
		GetCtx() context.Context
		Cancel()
		Read(r io.Reader) (n int, err error)
		WriteWithOpaque(opaque uint32, w io.Writer) (n int, err error)
		Reply(resp IResponseContext)
		OnComplete()
		GetReceiveTime() time.Time
		SetTimeout(parent context.Context, duration time.Duration)
	}

	// Implement IRequestContext
	RequestContext struct {
		util.QueItemBase
		parentCtx    context.Context
		ctx          context.Context
		cancelCtx    context.CancelFunc
		message      proto.RawMessage
		chResponse   chan<- IResponseContext
		timeReceived time.Time
	}

	InboundRequestContext struct {
		RequestContext
		lsnrType ListenerType
	}

	OutboundRequestContext struct {
		RequestContext
	}

	// Implement IResponseContext
	ResponseContext struct {
		message proto.RawMessage
	}

	InboundResponseContext struct {
		ResponseContext
	}

	OutboundResponseContext struct {
		ResponseContext
		status uint32
	}
)

// To be implement
func (r *RequestContext) SetTimeout(parent context.Context, timeout time.Duration) {
	if parent == nil {
		r.parentCtx = context.Background()
	} else {
		r.parentCtx = parent
	}
	r.ctx, r.cancelCtx = context.WithTimeout(r.parentCtx, timeout)
}

func (r *RequestContext) WriteWithOpaque(opaque uint32, w io.Writer) (n int, err error) {
	var msg proto.RawMessage
	msg.ShallowCopy(&r.message)
	msg.SetOpaque(opaque)
	n, err = msg.Write(w)
	return
}

func (r *InboundRequestContext) GetListenerType() ListenerType {
	return r.lsnrType
}

func (r *InboundRequestContext) SetResponseChannel(ch chan<- IResponseContext) {
        r.chResponse = ch
}


func NewInboundResponseContext(opMsg *proto.OperationalMessage) (r *InboundResponseContext, err error) {
	r = &InboundResponseContext{}

	err = opMsg.Encode(&r.message)
	return
}

func NewInboundRespose(opCode proto.OpCode, m *proto.RawMessage) (r *InboundResponseContext) {
	r = &InboundResponseContext{
		ResponseContext{
			message: *m,
		},
	}

	m.GiveUpBufferOwnership()
	proto.SetOpCode(&r.message, opCode)
	return
}

func NewInboundRequestContext(c *Connector) (r *InboundRequestContext) {
	r = &InboundRequestContext{
		RequestContext: RequestContext{
			chResponse: c.chResponse,
		},
		lsnrType: c.lsnrType,
	}
	return
}

func NewOutboundRequestContext(msg *proto.RawMessage, opaque uint32,
	ctx context.Context, ch chan<- IResponseContext, to time.Duration) (r *OutboundRequestContext) {
	r = &OutboundRequestContext{
		RequestContext: RequestContext{
			ctx:        ctx,
			message:    *msg,
			chResponse: ch,
		},
	}
    r.SetQueTimeout(to)
	r.GetMessage().SetOpaque(opaque)
	return
}

func NewErrorOutboundResponse(status uint32) (r *OutboundResponseContext) {
	r = NewOutboundResponse()
	r.status = status
	return
}

func NewOutboundResponse() *OutboundResponseContext {
	return oResponsePool.Get().(*OutboundResponseContext)
}

func ReleaseOutboundResponse(resp IResponseContext) {
	if resp != nil {
		resp.OnComplete()
		if rctx, ok := resp.(*OutboundResponseContext); ok {
			rctx.status = 0
			oResponsePool.Put(rctx)
		}
	}
}

func (r *ResponseContext) GetStatus() (s uint32) {
	s = 0
	return
}

func (r *ResponseContext) GetMsgSize() uint32 {
	return r.message.GetMsgSize()
}

func (r *ResponseContext) GetMessage() *proto.RawMessage {
	return &r.message
}

func (r *ResponseContext) GetOpStatus() proto.OpStatus {
	st, _ := proto.GetOpStatus(&r.message)
	return st
}

func (r *ResponseContext) OnComplete() {
	r.message.ReleaseBuffer()
}

func (req *ResponseContext) Read(r io.Reader) (n int, err error) {
	return req.message.Read(r)
}

func (r *ResponseContext) Write(w io.Writer) (n int, err error) {
	return r.message.Write(w)
}

func (r *OutboundResponseContext) GetStatus() uint32 {
	return r.status
}

func (r *RequestContext) GetMessage() *proto.RawMessage {
	return &r.message
}

func (r *RequestContext) GetCtx() context.Context {
	return r.ctx
}

func (r *RequestContext) Cancel() {
	r.cancelCtx()
}

func (req *RequestContext) Read(r io.Reader) (n int, err error) {
	n, err = req.message.Read(r)
	req.timeReceived = time.Now()
	return
}

func (r *RequestContext) Reply(resp IResponseContext) {
	if r.parentCtx != nil {
		select {
		case <-r.parentCtx.Done():
			glog.Warningf("request context canceled. %s", r.parentCtx.Err().Error())
		case r.chResponse <- resp:
		}
	} else {
		glog.Debugf("parent context is nil")
		r.chResponse <- resp
	}
}

func (r *RequestContext) OnComplete() {
	r.message.ReleaseBuffer()
}

func (r *RequestContext) OnCleanup() {
}

func (r *RequestContext) OnExpiration() {
}

func (r *RequestContext) GetReceiveTime() time.Time {
	return r.timeReceived
}

func (r *OutboundRequestContext) Reply(resp IResponseContext) {

	if r.ctx != nil {
		select {
		case <-r.ctx.Done():
			resp.OnComplete()
			return
		default:
		}
	}

	resp.GetMessage().SetOpaque(r.GetMessage().GetOpaque())

	select {
	case r.chResponse <- resp:
	default:
		glog.Debugf("result channel busy, drop the response")
		resp.OnComplete()
	}
}

func (r *OutboundRequestContext) OnCleanup() {

	glog.Debugf("RB cleanup")
	resp := NewErrorOutboundResponse(proto.StatusRBCleanup)
	r.Reply(resp)
}

func (r *OutboundRequestContext) OnExpiration() {

	glog.Debugf("RB expire")
	resp := NewErrorOutboundResponse(proto.StatusRBExpire)
	r.Reply(resp)
}

func DefaultInboundRequestContexCreator(magic []byte, c *Connector) (ctx IRequestContext, err error) {
	if len(magic) < 4 {
		err = errors.New("no enough magic bytes")
		return
	}
	if bytes.Compare(magic[:2], proto.JunoMagic[:]) != 0 { ///TODO not checking protocol version for now
		err = errors.New("not juno message")
		return
	}
	ctx = NewInboundRequestContext(c)

	_, err = ctx.Read(c.reader)
	return
}

func ExtendedRequestContexCreator(magic []byte, c *Connector) (ctx IRequestContext, err error) {
	if len(magic) < 4 {
		err = errors.New("no enough magic bytes")
		return
	}
	if bytes.Compare(magic[:2], proto.JunoMagic[:]) == 0 {
		ctx = NewInboundRequestContext(c)
	} else if bytes.Compare(magic[:4], mayfly.MayflyMagic[:]) == 0 {
		ctx = newMayflyInboundRequestContext(c)
	} else {
		err = errors.New("protocol not supported")
		return
	}
	_, err = ctx.Read(c.reader)
	return
}
