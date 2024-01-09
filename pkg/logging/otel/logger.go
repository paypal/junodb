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
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"time"

	"juno/pkg/logging"
	otelCfg "juno/pkg/logging/otel/config"
	"juno/pkg/proto"
	"juno/third_party/forked/golang/glog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	instrument "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/aggregation"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

var OTEL_COLLECTOR_PROTOCOL string = DEFAULT_OTEL_COLLECTOR_PROTOCOL

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
	c.Validate()
	c.Dump()
	if c.Enabled {
		// Initialize only if OTEL is enabled
		InitMetricProvider(c)
	}
	return
}

func Finalize() {
	// Shutdown the Go routines for histograms
	for _, val := range histMetricMap {
		if val.histogram != nil && val.doneCh != nil {
			close(val.doneCh)
		}
	}

	// Shutdown the Go routines for counters that are active
	for _, val := range countMetricMap {
		if val.counter != nil && val.doneCh != nil {
			close(val.doneCh)
		}
	}
}

func InitMetricProvider(config *otelCfg.Config) {
	if meterProvider != nil {
		return
	}

	otelCfg.OtelConfig = config

	ctx := context.Background()
	// View to customize histogram buckets and rename a single histogram instrument.
	repBucketsView := metric.NewView(
		metric.Instrument{
			Name:  PopulateJunoMetricNamePrefix("replication"),
			Scope: instrumentation.Scope{Name: MeterName},
		},
		metric.Stream{
			Aggregation: aggregation.ExplicitBucketHistogram{
				Boundaries: config.HistogramBuckets.Replication,
			},
		},
	)

	ssConnBucketsView := metric.NewView(
		metric.Instrument{
			Name:  PopulateJunoMetricNamePrefix("ssConnection"),
			Scope: instrumentation.Scope{Name: MeterName},
		},
		metric.Stream{
			Aggregation: aggregation.ExplicitBucketHistogram{
				Boundaries: config.HistogramBuckets.SsConnect,
			},
		},
	)

	inboundBucketsView := metric.NewView(
		metric.Instrument{
			Name:  PopulateJunoMetricNamePrefix("inbound"),
			Scope: instrumentation.Scope{Name: MeterName},
		},
		metric.Stream{
			Aggregation: aggregation.ExplicitBucketHistogram{
				Boundaries: config.HistogramBuckets.Inbound,
			},
		},
	)

	outboundBucketsView := metric.NewView(
		metric.Instrument{
			Name:  PopulateJunoMetricNamePrefix("outbound_connection"),
			Scope: instrumentation.Scope{Name: MeterName},
		},
		metric.Stream{
			Aggregation: aggregation.ExplicitBucketHistogram{
				Boundaries: config.HistogramBuckets.OutboundConnection,
			},
		},
	)

	var err error
	meterProvider, err = NewMeterProvider(ctx, *config, repBucketsView, ssConnBucketsView, inboundBucketsView, outboundBucketsView)
	if err != nil {
		log.Fatal(err)
	}
}

func NewMeterProvider(ctx context.Context, cfg otelCfg.Config, vis ...metric.View) (*metric.MeterProvider, error) {
	exp, err := NewHTTPExporter(ctx)
	if err != nil {
		return nil, err
	}

	res := getResourceInfo(cfg.Poolname)

	// Set the reader collection periord to 10 seconds (default 60).
	reader := metric.NewPeriodicReader(exp, metric.WithInterval(time.Duration(cfg.Resolution)*time.Second))
	metProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(reader),
		metric.WithView(vis...),
	)

	return metProvider, nil
}

func NewHTTPExporter(ctx context.Context) (metric.Exporter, error) {
	header := make(map[string]string)
	var deltaTemporalitySelector = func(metric.InstrumentKind) metricdata.Temporality { return metricdata.DeltaTemporality }
	if otelCfg.OtelConfig.UseTls == true {
		return otlpmetrichttp.New(
			ctx,
			otlpmetrichttp.WithEndpoint(otelCfg.OtelConfig.Host+":"+fmt.Sprintf("%d", otelCfg.OtelConfig.Port)),
			// WithTimeout sets the max amount of time the Exporter will attempt an
			// export.
			otlpmetrichttp.WithTimeout(20*time.Second),
			otlpmetrichttp.WithCompression(otlpmetrichttp.NoCompression),
			otlpmetrichttp.WithTemporalitySelector(deltaTemporalitySelector),
			otlpmetrichttp.WithHeaders(header),
			otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
				// Enabled indicates whether to not retry sending batches in case
				// of export failure.
				Enabled: false,
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
				MaxElapsedTime: 20 * time.Second,
			}),
			otlpmetrichttp.WithURLPath(otelCfg.OtelConfig.UrlPath),
		)
	} else {
		return otlpmetrichttp.New(
			ctx,
			otlpmetrichttp.WithEndpoint(otelCfg.OtelConfig.Host+":"+fmt.Sprintf("%d", otelCfg.OtelConfig.Port)),
			otlpmetrichttp.WithInsecure(),
			otlpmetrichttp.WithTimeout(7*time.Second),
			otlpmetrichttp.WithCompression(otlpmetrichttp.NoCompression),
			otlpmetrichttp.WithTemporalitySelector(deltaTemporalitySelector),
			otlpmetrichttp.WithHeaders(header),
			otlpmetrichttp.WithRetry(otlpmetrichttp.RetryConfig{
				Enabled:         true,
				InitialInterval: 1 * time.Second,
				MaxInterval:     10 * time.Second,
				MaxElapsedTime:  240 * time.Second,
			}),
			otlpmetrichttp.WithURLPath(otelCfg.OtelConfig.UrlPath),
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

func getHistogram(histName CMetric) (chan DataPoint, error) {
	if histMetric, ok := histMetricMap[histName]; ok {
		histMetric.createHistogram.Do(func() {
			meter := meterProvider.Meter(MeterName)
			histMetric.histogram, _ = meter.Int64Histogram(
				histMetric.metricName,
				instrument.WithDescription(histMetric.metricDesc),
				instrument.WithUnit(histMetric.metricUnit),
			)
			histMetric.doneCh = make(chan bool)
			histMetric.histogramCh = make(chan DataPoint, histChannelSize)
			go doWriteHistogram(histMetric.histogramCh, histMetric.doneCh, histMetric.histogram)
		})
		if histMetric.histogramCh != nil {
			return histMetric.histogramCh, nil
		} else {
			return nil, errors.New("Histogram Object not Ready")
		}
	} else {
		return nil, errors.New("No Such Histogram exists")
	}
}

func GetCounter(counterName CMetric) (chan DataPoint, error) {
	if counterMetric, ok := countMetricMap[counterName]; ok {
		counterMetric.createCounter.Do(func() {
			meter := meterProvider.Meter(MeterName)
			counterMetric.counter, _ = meter.Int64Counter(
				PopulateJunoMetricNamePrefix(counterMetric.metricName),
				instrument.WithDescription(counterMetric.metricDesc),
			)
			counterMetric.doneCh = make(chan bool)
			counterMetric.counterCh = make(chan DataPoint, counterChannelSize)
			go doWriteCounter(counterMetric.counterCh, counterMetric.doneCh, counterMetric.counter)
		})
		if counterMetric.counterCh != nil {
			return counterMetric.counterCh, nil
		} else {
			return nil, errors.New("Counter Object not Ready")
		}
	} else {
		return nil, errors.New("No Such counter exists")
	}
}

// This is the pp.app.intbound metric
func RecordOperation(opType string, status proto.OpStatus, latency int64) {
	if IsEnabled() {
		if opsHistChannel, err := getHistogram(Inbound); err == nil {
			commonLabels := instrument.WithAttributes(
				attribute.String("endpoint", opType),
				attribute.String("error_reason", status.ShortNameString()),
				attribute.String("status", logging.CalStatus(status).CalStatus()),
			)
			dataPoint := DataPoint{commonLabels, latency}
			if opsHistChannel != nil && len(opsHistChannel) < histChannelSize {
				opsHistChannel <- dataPoint
			}
		}
	}
}

func RecordReplication(opType string, status string, destination string, latency int64) {
	if IsEnabled() {
		if replHistChannel, err := getHistogram(Replication); err == nil {
			commonLabels := instrument.WithAttributes(
				attribute.String("operation", opType),
				attribute.String("status", status),
				attribute.String("dest_az", destination),
			)
			dataPoint := DataPoint{commonLabels, latency}
			if replHistChannel != nil && len(replHistChannel) < histChannelSize {
				replHistChannel <- dataPoint
			}
		}
	}
}

func RecordSSConnection(endpoint string, status string, latency int64) {
	if IsEnabled() {
		if ssConnHistChannel, err := getHistogram(SSConnection); err == nil {
			commonLabels := instrument.WithAttributes(
				attribute.String("endpoint", endpoint),
				attribute.String("status", status),
			)
			dataPoint := DataPoint{commonLabels, latency}
			if ssConnHistChannel != nil && len(ssConnHistChannel) < histChannelSize {
				ssConnHistChannel <- dataPoint
			}
		}
	}
}

func RecordOutboundConnection(endpoint string, status string, latency int64) {
	if IsEnabled() {
		if outBoundHistChannel, err := getHistogram(OutboundConnection); err == nil {
			commonLabels := instrument.WithAttributes(
				attribute.String(Endpoint, endpoint),
				attribute.String("status", status),
			)
			dataPoint := DataPoint{commonLabels, latency}
			if outBoundHistChannel != nil && len(outBoundHistChannel) < histChannelSize {
				outBoundHistChannel <- dataPoint
			}
		}
	}
}

func RecordCount(counterName CMetric, tags []Tags) {
	if IsEnabled() {
		if counterChannel, err := GetCounter(counterName); err == nil {
			var commonLabels instrument.MeasurementOption
			if len(tags) != 0 {
				commonLabels = covertTagsToOTELAttributes(tags)
			}
			dataPoint := DataPoint{commonLabels, 1}
			if counterChannel != nil && len(counterChannel) < counterChannelSize {
				counterChannel <- dataPoint
			}
		} else {
			glog.Error(err)
		}
	}
}

func covertTagsToOTELAttributes(tags []Tags) instrument.MeasurementOption {
	attr := make([]attribute.KeyValue, len(tags))
	for i := 0; i < len(tags); i++ {
		attr[i] = attribute.String(tags[i].TagName, tags[i].TagValue)
	}
	return instrument.WithAttributes(attr...)
}

func PopulateJunoMetricNamePrefix(metricName string) string {
	return JUNO_METRIC_PREFIX + metricName
}

func getResourceInfo(appName string) *resource.Resource {
	hostname, _ := os.Hostname()
	resource := resource.NewWithAttributes("empty resource",
		attribute.String("host", hostname),
		attribute.String("application", appName),
	)
	return resource
}

func doWriteHistogram(histChannel chan DataPoint, doneCh chan bool, hist instrument.Int64Histogram) {
	ctx := context.Background()
	for {
		select {
		case dataPoint := <-histChannel:
			hist.Record(ctx, dataPoint.data, dataPoint.attr)
		case <-doneCh:
			return
		}
	}
}

func doWriteCounter(counterChannel chan DataPoint, doneCh chan bool, count instrument.Int64Counter) {
	ctx := context.Background()
	for {
		select {
		case dataPoint := <-counterChannel:
			if dataPoint.attr != nil {
				count.Add(ctx, dataPoint.data, dataPoint.attr)
			} else {
				count.Add(ctx, dataPoint.data)
			}
		case <-doneCh:
			return
		}
	}
}
