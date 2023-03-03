package io

import ()

type IRequestHandler interface {
	Init()
	GetReqCtxCreator() InboundRequestContextCreator

	Process(reqCtx IRequestContext) error
	Finish()

	OnKeepAlive(connector *Connector, reqCtx IRequestContext) error
}
