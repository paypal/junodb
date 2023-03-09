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
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	otelCfg "juno/pkg/logging/otel/config"
	"juno/third_party/forked/golang/glog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

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
)

var apiHistogram syncint64.Histogram
var replicationHistogram syncint64.Histogram
var connectHistogram syncint64.Histogram
var ssConnectHistogram syncint64.Histogram

type CMetric int

const (
	RRDropMaxRetry CMetric = CMetric(iota)
	RRDropQueueFull
	RRDropRecExpired
	Accept
	Close
	RAPI
	ReqProc
	ProcErr
)

type Tags struct {
	TagName  string
	TagValue string
}

const (
	Target       = string("target")
	Endpoint     = string("endpoint")
	Operation    = string("operation")
	Status       = string("status")
	Fatal        = string("Fatal")
	Error        = string("Error")
	Warn         = string("Warning")
	Success      = string("Success")
	SS_RB_expire = string("SS_RB_expire")
	SSReqTimeout = string("SSReqTimeout")
)

type countMetric struct {
	metricName    string
	metricDesc    string
	counter       syncint64.Counter
	createCounter *sync.Once
}

var countMetricMap map[CMetric]*countMetric = map[CMetric]*countMetric{
	RRDropMaxRetry:   {"RR_Drop_MaxRetry", "Records dropped in replication queue due to max retry failures", nil, &rrDropMaxRetryCounterOnce},
	RRDropQueueFull:  {"RR_Drop_QueueFull", "Records dropped in replication queue due to queue is full", nil, &rrDropQueueFullCounterOnce},
	RRDropRecExpired: {"RR_Drop_RecExpired", "Records dropped in replication queue due to expiry of records", nil, &rrDropRecExpiredCounterOnce},
	Accept:           {"accept", "Accepting incoming connections", nil, &acceptCounterOnce},
	Close:            {"close", "Closing incoming connections", nil, &closeCounterOnce},
	RAPI:             {"rapi", "Processing of replicated requests", nil, &rapiCounterOnce},
	ReqProc:          {"ReqProc", "Processing of replicated requests", nil, &reqProcCounterOnce},
	ProcErr:          {"ProcErr", "Processing of replicated requests", nil, &procErrCounterOnce},
}

type (
	ServerType int
)

const (
	SvrTypeProxy      = ServerType(1)
	SvrTypeStorage    = ServerType(2)
	SvrTypeClusterMgr = ServerType(3)
	SvrTypeAll        = ServerType(6)
)

// default OTEL configurations point to QA collector
var DEFAULT_OTEL_COLLECTOR_PROTOCOL string = "http"
var DEFAULT_OTEL_COLLECTOR__IP string = "otelmetrics-pp-observability.us-central1.gcp.dev.paypalinc.com"
var DEFAULT_GRPC_OTEL_COLLECTOR_PORT string = "30705"
var DEFAULT_HTTP_OTEL_COLLECTOR_PORT string = "30706"
var COLLECTOR_POLLING_INTERVAL_SECONDS int32 = 5

const JUNO_METRIC_PREFIX = "juno.server."
const MeterName = "juno-server-meter"

var OTEL_COLLECTOR_PROTOCOL string = DEFAULT_OTEL_COLLECTOR_PROTOCOL

// OTEl Status
const (
	StatusSuccess string = "SUCCESS"
	StatusFatal   string = "FATAL"
	StatusError   string = "ERROR"
	StatusWarning string = "WARNING"
	StatusUnknown string = "UNKNOWN"
)

var (
	meterProvider *metric.MeterProvider
)

func Initialize(args ...interface{}) (err error) {
	glog.Info("Juno OTEL initialized")
	sz := len(args)
	if sz == 0 || sz < 1 {
		err = fmt.Errorf("Otel config argument not as expected")
		glog.Error(err)
		return
	}
	var c *otelCfg.Config
	var ok bool
	if c, ok = args[0].(*otelCfg.Config); !ok {
		err = fmt.Errorf("wrong argument type")
		glog.Error(err)
		return
	}
	c.Dump()
	if c.Enabled {
		// Initialize only if OTEL is enabled
		InitMetricProvider(c)
	}
	return
}

func InitMetricProvider(config *otelCfg.Config) {
	if meterProvider != nil {
		fmt.Printf("Retrung as meter is already available")
		return
	}

	//TODO Remove this after testing
	otelCfg.OtelConfig = config

	ctx := context.Background()

	// View to customize histogram buckets and rename a single histogram instrument.
	repBucketsView := metric.NewView(
		metric.Instrument{
			Name:  "*replication*",
			Scope: instrumentation.Scope{Name: MeterName},
		},
		metric.Stream{
			Name: "replication",
			Aggregation: aggregation.ExplicitBucketHistogram{
				Boundaries: []float64{64, 128, 256, 512, 1024, 2048, 4096},
			},
		})

	provider, err := NewMeterProvider(ctx, *config, repBucketsView)
	if err != nil {
		log.Fatal(err)
	}
	provider.Meter(MeterName)
	global.SetMeterProvider(provider)
}

func NewMeterProvider(ctx context.Context, cfg otelCfg.Config, vis ...metric.View) (*metric.MeterProvider, error) {
	exp, err := NewHTTPExporter(ctx)
	if err != nil {
		return nil, err
	}

	res := getResourceInfo(cfg.Poolname)

	// Set the reader collection periord to 10 seconds (default 60).
	reader := metric.NewPeriodicReader(exp, metric.WithInterval(time.Duration(cfg.Resolution)*time.Second))
	meterProvider = metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(reader),
		metric.WithView(vis...),
	)

	return meterProvider, nil
}

func NewHTTPExporter(ctx context.Context) (metric.Exporter, error) {
	var deltaTemporalitySelector = func(metric.InstrumentKind) metricdata.Temporality { return metricdata.DeltaTemporality }
	if otelCfg.OtelConfig.UseTls == true {
		return otlpmetrichttp.New(
			ctx,
			otlpmetrichttp.WithEndpoint(otelCfg.OtelConfig.Host+":"+fmt.Sprintf("%d", otelCfg.OtelConfig.Port)),
			//otlpmetrichttp.WithInsecure(),
			// WithTimeout sets the max amount of time the Exporter will attempt an
			// export.
			//func(metric.InstrumentKindSyncHistogram){return metricdata.DeltaTemporality}
			otlpmetrichttp.WithTimeout(7*time.Second),
			otlpmetrichttp.WithCompression(otlpmetrichttp.NoCompression),
			otlpmetrichttp.WithTemporalitySelector(deltaTemporalitySelector),
			otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
				// Enabled indicates whether to not retry sending batches in case
				// of export failure.
				Enabled: true,
				// InitialInterval the time to wait after the first failure before
				// retrying.
				InitialInterval: 1 * time.Second,
				// MaxInterval is the upper bound on backoff interval. Once this
				// value is reached the delay between consecutive retries will
				// always be `MaxInterval`.
				MaxInterval: 10 * time.Second,
				// MaxElapsedTime is the maximum amount of time (including retries)
				// spent trying to send a request/batch. Once this value is
				// reached, the data is discarded.
				MaxElapsedTime: 240 * time.Second,
			}),
		)
	} else {
		return otlpmetrichttp.New(
			ctx,
			otlpmetrichttp.WithEndpoint(otelCfg.OtelConfig.Host+":"+fmt.Sprintf("%d", otelCfg.OtelConfig.Port)),
			otlpmetrichttp.WithInsecure(),
			otlpmetrichttp.WithTimeout(7*time.Second),
			otlpmetrichttp.WithCompression(otlpmetrichttp.NoCompression),
			otlpmetrichttp.WithTemporalitySelector(deltaTemporalitySelector),
			otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
				Enabled:         true,
				InitialInterval: 1 * time.Second,
				MaxInterval:     10 * time.Second,
				MaxElapsedTime:  240 * time.Second,
			}),
		)
	}

}

func handleErr(err error, message string) {
	if err != nil {
		log.Fatalf("%s: %v", message, err)
	}
}

func IsEnabled() bool {
	return meterProvider != nil
}

func GetHistogramForOperation() (syncint64.Histogram, error) {
	var err error
	apiHistogramOnce.Do(func() {
		meter := global.Meter(MeterName)
		apiHistogram, err = meter.SyncInt64().Histogram(
			PopulateJunoMetricNamePrefix("inbound"),
			instrument.WithDescription("Histogram for Juno API"),
			instrument.WithUnit(unit.Milliseconds),
		)

	})
	return apiHistogram, err
}

func GetHistogramForReplication() (syncint64.Histogram, error) {
	var err error
	replicationHistogramOnce.Do(func() {
		meter := global.Meter(MeterName)
		replicationHistogram, err = meter.SyncInt64().Histogram(
			PopulateJunoMetricNamePrefix("replication"),
			instrument.WithDescription("Histogram for Juno replication"),
			instrument.WithUnit(unit.Milliseconds),
		)

	})
	return replicationHistogram, err
}

func GetHistogramForReplicationConnect() (syncint64.Histogram, error) {
	var err error
	connectHistogramOnce.Do(func() {
		meter := global.Meter(MeterName)
		connectHistogram, err = meter.SyncInt64().Histogram(
			PopulateJunoMetricNamePrefix("outbound_connection"),
			instrument.WithDescription("Histogram for Juno connection"),
			instrument.WithUnit(unit.Milliseconds),
		)

	})
	return connectHistogram, err
}

func GetHistogramForSSConnect() (syncint64.Histogram, error) {
	var err error
	ssConnectHistogramOnce.Do(func() {
		meter := global.Meter(MeterName)
		ssConnectHistogram, err = meter.SyncInt64().Histogram(
			PopulateJunoMetricNamePrefix("ssConnection"),
			instrument.WithDescription("Histogram for Juno SS connection failure"),
			instrument.WithUnit(unit.Milliseconds),
		)

	})
	return ssConnectHistogram, err
}

func GetCounter(counterName CMetric) (syncint64.Counter, error) {
	if counterMetric, ok := countMetricMap[counterName]; ok {
		counterMetric.createCounter.Do(func() {
			meter := global.Meter(MeterName)
			counterMetric.counter, _ = meter.SyncInt64().Counter(
				PopulateJunoMetricNamePrefix(counterMetric.metricName),
				instrument.WithDescription(counterMetric.metricDesc),
			)
		})
		if counterMetric.counter != nil {
			return counterMetric.counter, nil
		} else {
			return nil, errors.New("Counter Object not Ready")
		}
	} else {
		return nil, errors.New("No Such counter exists")
	}
}

// This is the pp.app.intbound metric
func RecordOperation(opType string, status string, latency int64) {
	ctx := context.Background()
	if operation, err := GetHistogramForOperation(); err == nil {
		commonLabels := []attribute.KeyValue{
			attribute.String("operation", opType),
			attribute.String("status", status),
		}
		operation.Record(ctx, latency, commonLabels...)
	}
}

func RecordReplication(opType string, status string, destination string, latency int64) {
	ctx := context.Background()
	if replication, err := GetHistogramForReplication(); err == nil {
		commonLabels := []attribute.KeyValue{
			attribute.String("operation", opType),
			attribute.String("status", status),
			attribute.String("dest_az", destination),
		}
		replication.Record(ctx, latency, commonLabels...)
	}
}

func RecordSSConnection(endpoint string, status string, latency int64) {
	ctx := context.Background()
	if ssConnect, err := GetHistogramForSSConnect(); err == nil {
		commonLabels := []attribute.KeyValue{
			attribute.String("endpoint", endpoint),
			attribute.String("status", status),
		}
		ssConnect.Record(ctx, latency, commonLabels...)
	}
}

func RecordOutboundConnection(endpoint string, status string, latency int64) {
	ctx := context.Background()
	if requestLatency, err := GetHistogramForReplicationConnect(); err == nil {
		commonLabels := []attribute.KeyValue{
			attribute.String("endpoint", endpoint),
			attribute.String("status", status),
		}
		requestLatency.Record(ctx, latency, commonLabels...)
	}
}

func RecordCount(counterName CMetric, tags []Tags) {
	ctx := context.Background()
	if counter, err := GetCounter(counterName); err == nil {
		if len(tags) != 0 {
			// commonLabels := []attribute.KeyValue{
			// 	attribute.String("endpoint", endpoint),
			// }
			commonLabels := covertTagsToOTELAttributes(tags)
			counter.Add(ctx, 1, commonLabels...)
		} else {
			counter.Add(ctx, 1)
		}
	} else {
		glog.Error(err)
	}
}

func covertTagsToOTELAttributes(tags []Tags) (attr []attribute.KeyValue) {
	attr = make([]attribute.KeyValue, len(tags))
	for i := 0; i < len(tags); i++ {
		attr[i] = attribute.String(tags[i].TagName, tags[i].TagValue)
	}
	return
}

func PopulateJunoMetricNamePrefix(metricName string) string {
	return JUNO_METRIC_PREFIX + metricName
}

// getEnvFromSyshieraYaml returns the env: line from /etc/syshiera.yaml
func getEnvFromSyshieraYaml() (string, error) {
	filePath := "/etc/syshiera.yaml"
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	fileReader := bufio.NewReader(file)
	scanner := bufio.NewScanner(fileReader)
	for scanner.Scan() {
		line := scanner.Text()
		err = scanner.Err()
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}
		pos := strings.Index(line, "pp_az: ")
		if pos == -1 {
			continue
		}
		return strings.TrimSpace(line[3:len(line)]), nil
	}
	err = errors.New("dc: not found in /etc/syshiera.yaml")
	return "", err
}

func getResourceInfo(appName string) *resource.Resource {
	colo, _err := getEnvFromSyshieraYaml()
	if _err != nil {
		colo = "qa"
	}
	hostname, _ := os.Hostname()

	resource := resource.NewWithAttributes("empty resource",
		semconv.HostNameKey.String(hostname),
		semconv.HostTypeKey.String("BM"),
		semconv.ServiceNameKey.String(appName),
		attribute.String("az", colo),
		attribute.String("application", appName),
	)
	return resource
}

// func NewGRPCExporter(ctx context.Context) (metric.Exporter, error) {
// 	ctx, cancel := context.WithTimeout(ctx, time.Second*10)
// 	defer cancel()

// 	// Exponential back-off strategy.
// 	backoffConf := backoff.DefaultConfig
// 	// You can also change the base delay, multiplier, and jitter here.
// 	backoffConf.MaxDelay = 240 * time.Second

// 	conn, err := grpc.DialContext(
// 		ctx,
// 		string(DEFAULT_OTEL_COLLECTOR__IP+":"+DEFAULT_GRPC_OTEL_COLLECTOR_PORT),
// 		grpc.WithInsecure(),
// 		grpc.WithBlock(),
// 		grpc.WithConnectParams(grpc.ConnectParams{
// 			Backoff: backoffConf,
// 			// Connection timeout.
// 			MinConnectTimeout: 5 * time.Second,
// 		}),
// 	)
// 	if err != nil {
// 		return nil, err
// 	}

// 	return otlpmetricgrpc.New(ctx,
// 		otlpmetricgrpc.WithGRPCConn(conn),
// 		// WithTimeout sets the max amount of time the Exporter will attempt an
// 		// export.
// 		otlpmetricgrpc.WithTimeout(7*time.Second),
// 	)
// }
