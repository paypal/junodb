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
package otel

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	cryptorand "crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	mathrand "math/rand"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	collectormetricpb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricpb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

const DefaultMetricsPath string = "/v1/metrics"

type mockCollector struct {
	endpoint string
	server   *http.Server

	spanLock       sync.Mutex
	metricsStorage MetricsStorage

	injectHTTPStatus  []int
	injectContentType string
	delay             <-chan struct{}

	clientTLSConfig *tls.Config
	expectedHeaders map[string]string
}

func (c *mockCollector) Stop() error {
	return c.server.Shutdown(context.Background())
}

func (c *mockCollector) MustStop(t *testing.T) {
	c.server.Shutdown(context.Background())
}

func (c *mockCollector) GetMetrics() []*metricpb.Metric {
	c.spanLock.Lock()
	defer c.spanLock.Unlock()
	return c.metricsStorage.GetMetrics()
}

func (c *mockCollector) Endpoint() string {
	return c.endpoint
}

func (c *mockCollector) ClientTLSConfig() *tls.Config {
	return c.clientTLSConfig
}

func (c *mockCollector) serveMetrics(w http.ResponseWriter, r *http.Request) {
	if c.delay != nil {
		select {
		case <-c.delay:
		case <-r.Context().Done():
			return
		}
	}

	if !c.checkHeaders(r) {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	response := collectormetricpb.ExportMetricsServiceResponse{}
	rawResponse, err := proto.Marshal(&response)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if injectedStatus := c.getInjectHTTPStatus(); injectedStatus != 0 {
		writeReply(w, rawResponse, injectedStatus, c.injectContentType)
		return
	}
	rawRequest, err := readRequest(r)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	// fmt.Println("---------------raw req--------------", rawRequest)
	request, err := unmarshalMetricsRequest(rawRequest, r.Header.Get("content-type"))
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	writeReply(w, rawResponse, 0, c.injectContentType)
	c.spanLock.Lock()
	defer c.spanLock.Unlock()

	fmt.Println("---------------serveMetrics--------------", request)
	c.metricsStorage.AddMetrics(request)
}

func unmarshalMetricsRequest(rawRequest []byte, contentType string) (*collectormetricpb.ExportMetricsServiceRequest, error) {
	request := &collectormetricpb.ExportMetricsServiceRequest{}
	if contentType != "application/x-protobuf" {
		return request, fmt.Errorf("invalid content-type: %s, only application/x-protobuf is supported", contentType)
	}
	err := proto.Unmarshal(rawRequest, request)
	return request, err
}

func (c *mockCollector) checkHeaders(r *http.Request) bool {
	for k, v := range c.expectedHeaders {
		got := r.Header.Get(k)
		if got != v {
			return false
		}
	}
	return true
}

func (c *mockCollector) getInjectHTTPStatus() int {
	if len(c.injectHTTPStatus) == 0 {
		return 0
	}
	status := c.injectHTTPStatus[0]
	c.injectHTTPStatus = c.injectHTTPStatus[1:]
	if len(c.injectHTTPStatus) == 0 {
		c.injectHTTPStatus = nil
	}
	return status
}

func readRequest(r *http.Request) ([]byte, error) {
	if r.Header.Get("Content-Encoding") == "gzip" {
		return readGzipBody(r.Body)
	}
	return ioutil.ReadAll(r.Body)
}

func readGzipBody(body io.Reader) ([]byte, error) {
	rawRequest := bytes.Buffer{}
	gunzipper, err := gzip.NewReader(body)
	if err != nil {
		return nil, err
	}
	defer gunzipper.Close()
	_, err = io.Copy(&rawRequest, gunzipper)
	if err != nil {
		return nil, err
	}
	return rawRequest.Bytes(), nil
}

func writeReply(w http.ResponseWriter, rawResponse []byte, injectHTTPStatus int, injectContentType string) {
	status := http.StatusOK
	if injectHTTPStatus != 0 {
		status = injectHTTPStatus
	}
	contentType := "application/x-protobuf"
	if injectContentType != "" {
		contentType = injectContentType
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write(rawResponse)
}

type mockCollectorConfig struct {
	MetricsURLPath    string
	Port              int
	InjectHTTPStatus  []int
	InjectContentType string
	Delay             <-chan struct{}
	WithTLS           bool
	ExpectedHeaders   map[string]string
}

func (c *mockCollectorConfig) fillInDefaults() {
	if c.MetricsURLPath == "" {
		c.MetricsURLPath = DefaultMetricsPath
	}
}

func runMockCollector(t *testing.T, cfg mockCollectorConfig) *mockCollector {
	cfg.fillInDefaults()
	ln, _ := net.Listen("tcp", fmt.Sprintf("localhost:%d", cfg.Port))
	// require.NoError(t, err)
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	// require.NoError(t, err)
	m := &mockCollector{
		endpoint:          fmt.Sprintf("localhost:%s", portStr),
		metricsStorage:    NewMetricsStorage(),
		injectHTTPStatus:  cfg.InjectHTTPStatus,
		injectContentType: cfg.InjectContentType,
		delay:             cfg.Delay,
		expectedHeaders:   cfg.ExpectedHeaders,
	}
	mux := http.NewServeMux()
	mux.Handle(cfg.MetricsURLPath, http.HandlerFunc(m.serveMetrics))
	server := &http.Server{
		Handler: mux,
	}
	if cfg.WithTLS {
		pem, _ := generateWeakCertificate()
		// require.NoError(t, err)
		tlsCertificate, _ := tls.X509KeyPair(pem.Certificate, pem.PrivateKey)
		// require.NoError(t, err)
		server.TLSConfig = &tls.Config{
			Certificates: []tls.Certificate{tlsCertificate},
		}

		m.clientTLSConfig = &tls.Config{
			InsecureSkipVerify: true,
		}
	}
	go func() {
		if cfg.WithTLS {
			_ = server.ServeTLS(ln, "", "")
		} else {
			_ = server.Serve(ln)
		}
	}()
	m.server = server
	return m
}

type mathRandReader struct{}

func (mathRandReader) Read(p []byte) (n int, err error) {
	return mathrand.Read(p)
}

var randReader mathRandReader

func generateWeakCertificate() (*pemCertificate, error) {
	priv, err := ecdsa.GenerateKey(elliptic.P256(), randReader)
	if err != nil {
		return nil, err
	}
	keyUsage := x509.KeyUsageDigitalSignature
	notBefore := time.Now()
	notAfter := notBefore.Add(time.Hour)
	serialNumberLimit := new(big.Int).Lsh(big.NewInt(1), 128)
	serialNumber, err := cryptorand.Int(randReader, serialNumberLimit)
	if err != nil {
		return nil, err
	}
	template := x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			Organization: []string{"otel-go"},
		},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		KeyUsage:              keyUsage,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{"localhost"},
		IPAddresses:           []net.IP{net.IPv6loopback, net.IPv4(127, 0, 0, 1)},
	}
	derBytes, err := x509.CreateCertificate(randReader, &template, &template, &priv.PublicKey, priv)
	if err != nil {
		return nil, err
	}
	certificateBuffer := new(bytes.Buffer)
	if err := pem.Encode(certificateBuffer, &pem.Block{Type: "CERTIFICATE", Bytes: derBytes}); err != nil {
		return nil, err
	}
	privDERBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	privBuffer := new(bytes.Buffer)
	if err := pem.Encode(privBuffer, &pem.Block{Type: "PRIVATE KEY", Bytes: privDERBytes}); err != nil {
		return nil, err
	}
	return &pemCertificate{
		Certificate: certificateBuffer.Bytes(),
		PrivateKey:  privBuffer.Bytes(),
	}, nil
}

type pemCertificate struct {
	Certificate []byte
	PrivateKey  []byte
}

// Collector is an interface that mock collectors should implements,
// so they can be used for the end-to-end testing.
type Collector interface {
	Stop() error
	GetMetrics() []*metricpb.Metric
}

// MetricsStorage stores the metrics. Mock collectors could use it to
// store metrics they have received.
type MetricsStorage struct {
	metrics []*metricpb.Metric
}

// NewMetricsStorage creates a new metrics storage.
func NewMetricsStorage() MetricsStorage {
	return MetricsStorage{}
}

// AddMetrics adds metrics to the metrics storage.
func (s *MetricsStorage) AddMetrics(request *collectormetricpb.ExportMetricsServiceRequest) {
	for _, rm := range request.GetResourceMetrics() {
		// TODO (rghetia) handle multiple resource and library info.
		fmt.Println("---------------AddMetrics------------------", rm)

		if len(rm.ScopeMetrics) > 0 {
			s.metrics = append(s.metrics, rm.ScopeMetrics[0].Metrics...)
			fmt.Println("Metric added successfully")
		} else {
			fmt.Println("Metrics added filed")
		}

		// if len(rm.InstrumentationLibraryMetrics) > 0 {
		// 	s.metrics = append(s.metrics, rm.InstrumentationLibraryMetrics[0].Metrics...)
		// }

	}
}

// GetMetrics returns the stored metrics.
func (s *MetricsStorage) GetMetrics() []*metricpb.Metric {
	// copy in order to not change.
	m := make([]*metricpb.Metric, 0, len(s.metrics))
	return append(m, s.metrics...)
}
