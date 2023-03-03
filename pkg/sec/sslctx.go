package sec

import (
	"fmt"
	"net"
	"time"
)

type (
	Conn interface {
		GetStateString() string
		IsServer() bool
		Handshake() error
		GetNetConn() net.Conn
		IsTLS() bool
	}
	tlsContextI interface {
		newServerConn(conn net.Conn) (Conn, error)
		dial(target string, timeout time.Duration) (conn Conn, err error)
		//	loadSessions(sessions [][]byte)
	}
)

func NewServerConn(conn net.Conn) (sConn Conn, err error) {
	var ctx tlsContextI
	if ctx, err = getServerTlsContext(); err == nil {
		sConn, err = ctx.newServerConn(conn)
	}
	return
}

func Dial(target string, timeout time.Duration) (conn Conn, err error) {
	var ctx tlsContextI
	if ctx, err = getClientTlsContext(); err == nil {
		conn, err = ctx.dial(target, timeout)
	}
	return
}

func getServerTlsContext() (ctx tlsContextI, err error) {
	gRwCtxMtx.RLock()
	ctx = gSvrTlsCtx
	gRwCtxMtx.RUnlock()
	if ctx == nil {
		err = fmt.Errorf("nil server TLS context")
	}
	return
}

func getClientTlsContext() (ctx tlsContextI, err error) {
	gRwCtxMtx.RLock()
	ctx = gCliTlsCtx
	gRwCtxMtx.RUnlock()
	if ctx == nil {
		err = fmt.Errorf("nil client TLS context")
	}
	return
}
