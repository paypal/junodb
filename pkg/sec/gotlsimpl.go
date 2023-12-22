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

func (c *TlsConn) GetTLSVersion() string {
	if c.conn != nil {
		stat := c.conn.ConnectionState()
		return GetVersionName(stat.Version)
	}
	return "none"
}

func GetVersionName(ver uint16) string {
	switch ver {
	case tls.VersionTLS10:
		return "TLSv1"
	case tls.VersionTLS11:
		return "TLSv1.1"
	case tls.VersionTLS12:
		return "TLSv1.2"
	case tls.VersionTLS13:
		return "TLSv1.3"
	default:
		return ""
	}
}

func (c *TlsConn) GetCipherName() string {
	if c.conn != nil {
		stat := c.conn.ConnectionState()
		return GetCipherName(stat.CipherSuite)
	}
	return "none"
}

func GetCipherName(cipher uint16) string {
	switch cipher {
	case tls.TLS_RSA_WITH_RC4_128_SHA:
		return "RSA-RC4-128-SHA"
	case tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:
		return "RSA-3DES-EDE-CBC-SHA"
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA:
		return "AES128-SHA"
	case tls.TLS_RSA_WITH_AES_256_CBC_SHA:
		return "AES256-SHA"
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA256:
		return "RSA-AES-128-CBC-SHA256"
	case tls.TLS_RSA_WITH_AES_128_GCM_SHA256:
		return "RSA-AES-128-GCM-SHA256"
	case tls.TLS_RSA_WITH_AES_256_GCM_SHA384:
		return "RSA-AES-256-GCM-SHA384"
	case tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:
		return "ECDHE_ECDSA_RC4_128_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:
		return "ECDHE_ECDSA_AES128_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:
		return "ECDHE_ECDSA_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:
		return "ECDHE_RSA_RC4_128_SHA"
	case tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA:
		return "ECDHE_RSA_3DES_EDE_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:
		return "ECDHE_RSA_AES128_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:
		return "ECDHE_RSA_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256:
		return "ECDHE_ECDSA_AES128_CBC_SHA256"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256:
		return "ECDHE_RSA_AES128_CBC_SHA256"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
		return "ECDHE_RSA_AES128_GCM_SHA256"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:
		return "ECDHE_ECDSA_AES128_GCM_SHA256"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384:
		return "ECDHE_RSA_AES256_GCM_SHA384"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384:
		return "ECDHE_ECDSA_AES256_GCM_SHA384"
	case tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305:
		return "ECDHE_RSA_CHACHA20_POLY1305"
	case tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305:
		return "ECDHE_ECDSA_CHACHA20_POLY1305"
	case tls.TLS_AES_128_GCM_SHA256:
		return "TLS_AES_128_GCM_SHA256"
	case tls.TLS_AES_256_GCM_SHA384:
		return "TLS_AES_256_GCM_SHA384"
	case tls.TLS_CHACHA20_POLY1305_SHA256:
		return "TLS_CHACHA20_POLY1305_SHA256"
	case tls.TLS_FALLBACK_SCSV:
		return "TLS_FALLBACK_SCSV"
	default:
		return "unknown"
	}
}

func (c *TlsConn) DidResume() string {
	if c.conn != nil {
		stat := c.conn.ConnectionState()
		if stat.DidResume {
			return "Yes"
		} else {
			return "No"
		}
	}
	return "No"
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
