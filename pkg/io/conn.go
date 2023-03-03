package io

import (
	"fmt"
	"net"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/sec"
)

type Conn interface {
	GetStateString() string
	GetNetConn() net.Conn
	IsTLS() bool
}

type Connection struct {
	conn net.Conn
}

const (
	// See pkg/sec/tls/handshak_client.go for this string
	downGraded = "tls: downgrade attempt detected, possibly due to a MitM attack or a broken middlebox"
)

func (c *Connection) GetStateString() string {
	return ""
}

func (c *Connection) GetNetConn() net.Conn {
	return c.conn
}

func (c *Connection) IsTLS() bool {
	return false
}

func Connect(endpoint *ServiceEndpoint, connectTimeout time.Duration) (conn net.Conn, err error) {
	timeStart := time.Now()

	if endpoint.SSLEnabled {
		var sslconn sec.Conn

		if sslconn, err = sec.Dial(endpoint.Addr, connectTimeout); err == nil {
			conn = sslconn.GetNetConn()
			if glog.LOG_DEBUG {
				glog.DebugDepth(1, fmt.Sprintf("connected to %s ssl=%s", endpoint.GetConnString(), sslconn.GetStateString()))
			}
		} else {
			glog.ErrorDepth(1, fmt.Sprintf("fail to connect %s error: %s", endpoint.GetConnString(), err))
			// if err.Error() == downGraded {
			// 	sec.ResetSessionIdForClient()
			// }
		}

		if cal.IsEnabled() {
			status := cal.StatusSuccess
			b := logging.NewKVBuffer()
			if err != nil {
				status = cal.StatusError
				b.Add([]byte("err"), err.Error())
			} else {
				b.Add([]byte("ssl"), sslconn.GetStateString())
			}

			cal.AtomicTransaction(cal.TxnTypeConnect, endpoint.Addr, status, time.Since(timeStart), b.Bytes())
		}
	} else {
		if conn, err = net.DialTimeout("tcp", endpoint.Addr, connectTimeout); err == nil {
			if glog.LOG_DEBUG {
				glog.DebugDepth(1, fmt.Sprintf("connected to %s", endpoint.GetConnString()))
			}
		} else {
			glog.ErrorDepth(1, fmt.Sprintf("fail to connect %s error: %s", endpoint.GetConnString(), err.Error()))
		}
		if cal.IsEnabled() {
			status := cal.StatusSuccess
			var data []byte
			if err != nil {
				status = cal.StatusError
				data = []byte(err.Error())
			} else {

			}
			cal.AtomicTransaction(cal.TxnTypeConnect, endpoint.GetConnString(), status, time.Since(timeStart), data)
		}
	}

	return
}

func ConnectTo(endpoint *ServiceEndpoint, connectTimeout time.Duration) (conn Conn, err error) {
	if endpoint.SSLEnabled {
		var sslconn sec.Conn

		if sslconn, err = sec.Dial(endpoint.Addr, connectTimeout); err == nil {
			conn = sslconn
			if glog.LOG_DEBUG {
				glog.DebugDepth(1, fmt.Sprintf("connected to %s ssl=%s", endpoint.GetConnString(), sslconn.GetStateString()))
			}
		} else {
			glog.ErrorDepth(1, fmt.Sprintf("fail to connect %s error: %s", endpoint.GetConnString(), err))
			// if err.Error() == downGraded {
			// 	sec.ResetSessionIdForClient()
			// }
		}
	} else {
		var connection Connection
		if connection.conn, err = net.DialTimeout("tcp", endpoint.Addr, connectTimeout); err == nil {
			conn = &connection
			if glog.LOG_DEBUG {
				glog.DebugDepth(1, fmt.Sprintf("connected to %s", endpoint.GetConnString()))
			}
		} else {
			glog.ErrorDepth(1, fmt.Sprintf("fail to connect %s error: %s", endpoint.GetConnString(), err.Error()))
		}
	}

	return
}
