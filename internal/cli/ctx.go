package cli

import (
	"juno/third_party/forked/golang/glog"

	"juno/pkg/proto"
)

//GetResponse() != nil and GetError() != nil are mutually exclusive
type IResponseContext interface {
	GetResponse() *proto.OperationalMessage
	GetError() error
	GetOpaque() uint32
	SetOpaque(opaque uint32)
}

type RequestContext struct {
	request    *proto.OperationalMessage
	chResponse chan IResponseContext
}

type ResponseContext struct {
	resp *proto.OperationalMessage
}

type ErrResponseContext struct {
	opaque uint32
	err    error
}

type ReaderResponse struct {
	response *proto.OperationalMessage
	err      error
}

func NewReaderResponse(resp *proto.OperationalMessage) *ReaderResponse {
	return &ReaderResponse{response: resp}
}

func NewErrorReaderResponse(err error) *ReaderResponse {
	return &ReaderResponse{err: err}
}

func NewRequestContext(m *proto.OperationalMessage, chResponse chan IResponseContext) *RequestContext {
	return &RequestContext{
		request:    m,
		chResponse: chResponse,
	}
}

func (r *ResponseContext) GetResponse() *proto.OperationalMessage {
	return r.resp
}

func (r *ResponseContext) SetOpaque(opaque uint32) {
	r.resp.SetOpaque(opaque)
}

func (r *ResponseContext) GetOpaque() uint32 {
	return r.resp.GetOpaque()
}

func (r *ResponseContext) GetError() error {
	return nil
}

func (r *ErrResponseContext) GetResponse() *proto.OperationalMessage {
	return nil
}

func (r *ErrResponseContext) SetOpaque(opaque uint32) {
	r.opaque = opaque
}

func (r *ErrResponseContext) GetOpaque() uint32 {
	return r.opaque
}

func (r *ErrResponseContext) GetError() error {
	return r.err
}

func (r *RequestContext) GetRequest() *proto.OperationalMessage {
	return r.request
}

func (r *RequestContext) Reply(response *proto.OperationalMessage) {
	//response.PrettyPrint(os.Stdout)
	if r.request == nil {
		glog.Fatal("nil request")
	}
	if response == nil {
		glog.Fatal("nil response")
	}
	response.SetOpaque(r.request.GetOpaque())
	r.chResponse <- &ResponseContext{response}
}

func (r *RequestContext) ReplyError(err error) {
	glog.DebugDepth(1, err)
	if r.request == nil {
		glog.Fatal("nil request") ///TODO
	}
	r.chResponse <- &ErrResponseContext{r.request.GetOpaque(), err}
}
