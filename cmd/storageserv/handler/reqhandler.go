package handler

import (
	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/storage"
	"juno/pkg/io"
	"juno/pkg/util"
)

var _ io.IRequestHandler = (*RequestHandler)(nil)

type RequestHandler struct {
	cnt                   util.AtomicCounter
	procPool              *storage.ReqProcCtxPool
	maxConcurrentRequests int32
}

func NewRequestHandler() *RequestHandler {
	rh := &RequestHandler{
		maxConcurrentRequests: int32(config.ServerConfig().MaxConcurrentRequests),
		procPool:              storage.NewReqProcCtxPool(int32(config.ServerConfig().ReqProcCtxPoolSize))}
	return rh
}

func (rh *RequestHandler) GetReqCtxCreator() io.InboundRequestContextCreator {
	glog.Verbosef("get ctx creator")
	return io.DefaultInboundRequestContexCreator
}

func (rh *RequestHandler) Process(reqCtx io.IRequestContext) error {

	glog.Verbosef("Process")
	if rh.cnt.Get() < rh.maxConcurrentRequests {
		rh.cnt.Add(1)
		proc := rh.procPool.Get()
		proc.Process(reqCtx)

		rh.cnt.Add(-1)
	}

	return nil
}

func (rh *RequestHandler) OnKeepAlive(connector *io.Connector, reqCtx io.IRequestContext) error {
	return nil
}

func (rh *RequestHandler) Init() {
}

func (rh *RequestHandler) Finish() {
}
