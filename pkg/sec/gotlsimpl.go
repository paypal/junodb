package sec

import (
	"crypto/x509"
	"fmt"
	"net"
	"sync"
	"time"

	"crypto/tls"
	"juno/third_party/forked/golang/glog"

	"juno/pkg/proto"
)

type (
	TlsConn struct {
		Conn
		isServer bool
		conn     *tls.Conn
	}
	tlsContextT struct {
		isServer bool
		config   *tls.Config
	}
	serverSideSessionIdCacheT struct {
		sessionStateMap sync.Map
	}
)

func (c *TlsConn) IsTLS() bool {
	return true
}

func (c *TlsConn) GetStateString() string {
	statStr := "GoTLS:"
	if c.conn != nil {
		stat := c.conn.ConnectionState()
		statStr += ":" + fmt.Sprint(stat.Version)
		statStr += ":" + fmt.Sprint(stat.CipherSuite)

		if stat.DidResume {
			statStr += ":ssl_r=1"
		} else {
			statStr += ":ssl_r=0"
		}
	}
	return statStr
}

func (c *TlsConn) IsServer() bool {
	return c.isServer
}

func (c *TlsConn) Handshake() error {
	if c.conn != nil {
		return c.conn.Handshake()
	}
	return fmt.Errorf("nil tls connection")
}

func (c *TlsConn) GetNetConn() net.Conn {
	return c.conn
}

func newGoTlsContext(server bool, certPEMBlock []byte, keyPEMBlock []byte, ks proto.IEncryptionKeyStore, done chan bool) (ctx tlsContextI, err error) {
	var tlscfg *tls.Config

	if server {
		authType := tls.NoClientCert
		var clientCAs *x509.CertPool = nil
		if config.ClientAuth {
			authType = tls.RequireAnyClientCert

			clientCAs, _ = x509.SystemCertPool()
			if clientCAs == nil {
				clientCAs = x509.NewCertPool()
			}
			if clientCAs == nil || !clientCAs.AppendCertsFromPEM(certPEMBlock) {
				glog.Errorf("Failed to append certificate to the clientCA")
				err = fmt.Errorf("Failed to append certificate to the clientCA")
				return
			}

		}

		var cert tls.Certificate
		if cert, err = tls.X509KeyPair(certPEMBlock, keyPEMBlock); err != nil {
			return
		}

		var key [32]byte
		var decryptionKey []byte
		if decryptionKey, err = ks.GetDecryptionKey(0); err != nil {
			return
		}
		copy(key[0:], decryptionKey[0:])
		tlscfg = &tls.Config{
			ClientAuth:         authType,
			ClientCAs:          clientCAs,
			Certificates:       []tls.Certificate{cert},
			InsecureSkipVerify: true,
		}
		tlscfg.SetSessionTicketKeys([][32]byte{key})
		ticker := time.NewTicker(time.Hour)

		go func() {
			for {
				select {
				case <-done:
					ticker.Stop()
					return
				case t := <-ticker.C:
					num := t.Hour()
					index := (uint32)(num % ks.NumKeys())
					decryptionKey, _ = ks.GetDecryptionKey(index)
					copy(key[0:], decryptionKey[0:])
					tlscfg.SetSessionTicketKeys([][32]byte{key})
				}
			}
		}()

	} else {
		rootCAs, _ := x509.SystemCertPool()
		if rootCAs == nil {
			rootCAs = x509.NewCertPool()
		}
		if ok := rootCAs.AppendCertsFromPEM(certPEMBlock); !ok {
			glog.Infoln("No certs appended, using system certs only")
			err = fmt.Errorf("fail to append certificate to the rootCA")
			return
		}

		var cert tls.Certificate
		if cert, err = tls.X509KeyPair(certPEMBlock, keyPEMBlock); err != nil {
			return
		}

		tlscfg = &tls.Config{
			RootCAs:                rootCAs,
			InsecureSkipVerify:     true,
			SessionTicketsDisabled: false,
			ClientSessionCache:     tls.NewLRUClientSessionCache(0),
			Certificates:           []tls.Certificate{cert},
		}

	}
	ctx = &tlsContextT{
		isServer: server,
		config:   tlscfg,
	}
	return
}

func (ctx *tlsContextT) newServerConn(conn net.Conn) (sconn Conn, err error) {
	if ctx.config == nil {
		err = fmt.Errorf("nil config")
		return
	}
	if ctx.isServer == false {
		err = fmt.Errorf("not server context")
		return
	}

	sconn = &TlsConn{
		isServer: true,
		conn:     tls.Server(conn, ctx.config),
	}
	return
}

func (ctx *tlsContextT) dial(target string, timeout time.Duration) (conn Conn, err error) {
	if ctx.config == nil {
		err = fmt.Errorf("nil config")
		return
	}
	if ctx.isServer {
		err = fmt.Errorf("not clientcontext")
		return
	}
	dialer := &net.Dialer{Timeout: timeout}
	var tlsconn *tls.Conn

	tlsconn, err = tls.DialWithDialer(dialer, "tcp", target, ctx.config)
	if err == nil {
		conn = &TlsConn{conn: tlsconn}
	}
	return
}
