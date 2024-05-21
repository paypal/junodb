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
	"sync"
	"time"

	instrument "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/metric"
)

//*************************** Constants ****************************
const (
	RRDropMaxRetry CMetric = CMetric(iota)
	RRDropQueueFull
	RRDropRecExpired
	SSL_CLIENT_INFO
	CLIENT_INFO
	Accept
	Close
	RAPI
	ReqProc
	ProcErr
	SoftMark
	TLSStatus
	Inbound
	Replication
	OutboundConnection
	SSConnection
)

const (
	Target       = string("target")
	Endpoint     = string("target_ip_port")
	Operation    = string("operation")
	Client       = string("client_app")
	Status       = string("status")
	Fatal        = string("Fatal")
	Error        = string("Error")
	Warn         = string("Warning")
	Success      = string("Success")
	SS_RB_expire = string("SS_RB_expire")
	SSReqTimeout = string("SSReqTimeout")
	SSMarkDown   = string("Down")
	SSMarkUp     = string("Up")
	TLS_version  = string("tls_version")
	Cipher       = string("cipher")
	Ssl_r        = string("ssl_r")
)

const (
	SvrTypeProxy      = ServerType(1)
	SvrTypeStorage    = ServerType(2)
	SvrTypeClusterMgr = ServerType(3)
	SvrTypeAll        = ServerType(6)
)

// OTEl Status
const (
	StatusSuccess string = "SUCCESS"
	StatusFatal   string = "FATAL"
	StatusError   string = "ERROR"
	StatusWarning string = "WARNING"
	StatusUnknown string = "UNKNOWN"
)

const (
	MachineCpuUsed string = string("machineCpuUsed")
	ProcessCpuUsed string = string("machineCpuUsed")
	MachineMemoryUsed
	ProcessMemoryUsed
)

// default OTEL configurations point to QA collector
const DEFAULT_OTEL_COLLECTOR_PROTOCOL string = "http"
const DEFAULT_OTEL_COLLECTOR__IP string = "0.0.0.0"
const DEFAULT_GRPC_OTEL_COLLECTOR_PORT string = "4317"
const DEFAULT_HTTP_OTEL_COLLECTOR_PORT string = "4318"
const COLLECTOR_POLLING_INTERVAL_SECONDS int32 = 5

const JUNO_METRIC_PREFIX = "juno.server."
const MeterName = "juno-server-meter"
const histChannelSize = 1000
const counterChannelSize = 1000

//****************************** variables ***************************

var (
	apiHistogramOnce            sync.Once
	replicationHistogramOnce    sync.Once
	connectHistogramOnce        sync.Once
	ssConnectHistogramOnce      sync.Once
	rrDropMaxRetryCounterOnce   sync.Once
	rrDropQueueFullCounterOnce  sync.Once
	rrDropRecExpiredCounterOnce sync.Once
	acceptCounterOnce           sync.Once
	closeCounterOnce            sync.Once
	rapiCounterOnce             sync.Once
	reqProcCounterOnce          sync.Once
	procErrCounterOnce          sync.Once
	softMarkCounterOnce         sync.Once
	tlsStatusCounterOnce        sync.Once
	sslClientInfoOnce           sync.Once
	clientInfoOnce              sync.Once
)

var apiHistogram instrument.Int64Histogram
var replicationHistogram instrument.Int64Histogram
var connectHistogram instrument.Int64Histogram
var ssConnectHistogram instrument.Int64Histogram

var opsHistChannel chan DataPoint
var replHistChannel chan DataPoint
var ssConnHistChannel chan DataPoint
var outBoundHistChannel chan DataPoint
var counterChannel chan DataPoint

var opsHistDoneCh chan bool
var replHistCh chan bool
var ssConnHistCh chan bool
var outBoundHistCh chan bool

var countMetricMap map[CMetric]*countMetric = map[CMetric]*countMetric{
	RRDropMaxRetry:   {"RR_Drop_MaxRetry", "Records dropped in replication queue due to max retry failures", nil, &rrDropMaxRetryCounterOnce, nil, nil},
	RRDropQueueFull:  {"RR_Drop_QueueFull", "Records dropped in replication queue due to queue is full", nil, &rrDropQueueFullCounterOnce, nil, nil},
	RRDropRecExpired: {"RR_Drop_RecExpired", "Records dropped in replication queue due to expiry of records", nil, &rrDropRecExpiredCounterOnce, nil, nil},
	SSL_CLIENT_INFO:  {"SSL_CLIENT_INFO", "Client app Info", nil, &sslClientInfoOnce, nil, nil},
	CLIENT_INFO:      {"CLIENT_INFO", "Client app Info", nil, &clientInfoOnce, nil, nil},
	Accept:           {"accept", "Accepting incoming connections", nil, &acceptCounterOnce, nil, nil},
	Close:            {"close", "Closing incoming connections", nil, &closeCounterOnce, nil, nil},
	RAPI:             {"rapi", "Processing of replicated requests", nil, &rapiCounterOnce, nil, nil},
	ReqProc:          {"ReqProc", "Request processor", nil, &reqProcCounterOnce, nil, nil},
	ProcErr:          {"ProcErr", "Request processor Error", nil, &procErrCounterOnce, nil, nil},
	SoftMark:         {"SoftMark", "Proxy marks down storage instances", nil, &softMarkCounterOnce, nil, nil},
	TLSStatus:        {"TLS_Status", "TLS connection state", nil, &tlsStatusCounterOnce, nil, nil},
}

var histMetricMap map[CMetric]*histogramMetric = map[CMetric]*histogramMetric{
	Inbound:            {PopulateJunoMetricNamePrefix("inbound"), "Histogram for Juno API", "ms", nil, &apiHistogramOnce, nil, nil},
	Replication:        {PopulateJunoMetricNamePrefix("replication"), "Histogram for Juno replication", "ms", nil, &replicationHistogramOnce, nil, nil},
	OutboundConnection: {PopulateJunoMetricNamePrefix("outbound_connection"), "Histogram for Juno connection", "us", nil, &connectHistogramOnce, nil, nil},
	SSConnection:       {PopulateJunoMetricNamePrefix("ssConnection"), "Histogram for Juno SS connection", "us", nil, &ssConnectHistogramOnce, nil, nil},
}

var (
	meterProvider *metric.MeterProvider
)

var (
	machineCpuUsedOnce    sync.Once
	processCpuUsedOnce    sync.Once
	machineMemoryUsedOnce sync.Once
	processMemoryUsedOnce sync.Once
	diskIOUtilization     sync.Once
	badShardOnce          sync.Once
	alertShardOnce        sync.Once
	warningShardOnce      sync.Once
	keyCountOnce          sync.Once
	freeStorageOnce       sync.Once
	usedStorageOnce       sync.Once
	LNLevelOnce           sync.Once
	compSecOnce           sync.Once
	compCountOnce         sync.Once
	pendingCompOnce       sync.Once
	stallOnce             sync.Once
	TCPConnCountOnce      sync.Once
	SSLConnCountOnce      sync.Once
)

var (
	machTime    time.Time
	machCpuTick uint16
	machUser    uint64
	machSystem  uint64
)

var GaugeMetricList = []*GaugeMetric{
	{"pCPU", "proc_cpu_used", "CPU utilization of individual Juno instance", nil, nil, &processCpuUsedOnce, SvrTypeAll},
	{"pMem", "proc_mem_used", "Memory utilization of individual Juno instance", nil, nil, &processMemoryUsedOnce, SvrTypeAll},

	{"nBShd", "bad_shard", "Number of bad shards", nil, nil, &badShardOnce, SvrTypeProxy},
	{"nAShd", "alert_shard", "number of shards with no redundancy", nil, nil, &alertShardOnce, SvrTypeProxy},
	{"nWShd", "warning_shard", "number of shards with bad SS", nil, nil, &warningShardOnce, SvrTypeProxy},

	{"conns", "conns_count", "number of current TCP connections", nil, nil, &TCPConnCountOnce, SvrTypeProxy},
	{"ssl_conns", "conns_ssl_count", "number of current SSL connections", nil, nil, &SSLConnCountOnce, SvrTypeProxy},
	{"keys", "key_count", "Key Counte in rocksDB", nil, nil, &keyCountOnce, SvrTypeStorage},
	{"free", "free_mb_storage_space", "Free Storage Space (mbytes)", nil, nil, &freeStorageOnce, SvrTypeStorage},
	{"used", "storage_used_mb", "Used Storage Space (mbytes)", nil, nil, &usedStorageOnce, SvrTypeStorage},
	{"LN", "LN_level", "Max LN Level in Rocksdb", nil, nil, &LNLevelOnce, SvrTypeStorage},
	{"compSec", "compaction_sec", "Compaction Sec", nil, nil, &compSecOnce, SvrTypeStorage},
	{"compCount", "compaction_count", "Compaction Count", nil, nil, &compCountOnce, SvrTypeStorage},
	{"pCompKB", "pending_compaction", "Pending Compaction KBytes", nil, nil, &pendingCompOnce, SvrTypeStorage},
	{"stall", "stall_write_rate", "Actural Delayed Write Rate", nil, nil, &stallOnce, SvrTypeStorage},
}

var otelIngestToken string

// ************************************ Types ****************************
type CMetric int

type Tags struct {
	TagName  string
	TagValue string
}

type countMetric struct {
	metricName    string
	metricDesc    string
	counter       instrument.Int64Counter
	createCounter *sync.Once
	counterCh     chan DataPoint
	doneCh        chan bool
}

type histogramMetric struct {
	metricName      string
	metricDesc      string
	metricUnit      string
	histogram       instrument.Int64Histogram
	createHistogram *sync.Once
	histogramCh     chan DataPoint
	doneCh          chan bool
}

type (
	ServerType int
)

type GaugeMetric struct {
	MetricShortName string
	MetricName      string
	metricDesc      string
	gaugeMetric     instrument.Float64ObservableGauge
	stats           []StateData
	createGauge     *sync.Once
	stype           ServerType
}

// Represents stats by a worker
type StateData struct {
	Name       string
	Value      float64
	Dimensions instrument.MeasurementOption
}

type DataPoint struct {
	attr instrument.MeasurementOption
	data int64
}
