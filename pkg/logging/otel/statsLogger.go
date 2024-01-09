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
	"juno/pkg/stats"
	"juno/third_party/forked/golang/glog"
	"runtime"
	"strconv"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func InitSystemMetrics(serverType ServerType, workerStats [][]stats.IState) {
	meter := meterProvider.Meter(MeterName)
	var metricList []metric.Observable = make([]metric.Observable, 0, len(GaugeMetricList))
	for _, element := range GaugeMetricList {
		if element.stype == serverType || element.stype == SvrTypeAll {
			element.createGauge.Do(func() {
				var err error
				element.gaugeMetric, err = meter.Float64ObservableGauge(
					PopulateJunoMetricNamePrefix(element.MetricName),
					metric.WithDescription(element.metricDesc),
				)
				metricList = append(metricList, element.gaugeMetric)
				if err != nil {
					glog.Error("FloatObservable creation failed : ", err.Error())
				}
			})
		}
	}

	if _, err := meter.RegisterCallback(func(_ context.Context, o metric.Observer) error {
		getMetricData(workerStats)
		for _, metric := range GaugeMetricList {
			if metric.gaugeMetric != nil {
				for _, stat := range metric.stats {
					o.ObserveFloat64(metric.gaugeMetric, stat.Value, stat.Dimensions)
				}
			}
		}
		return nil
	}, metricList...); err != nil {
		glog.Error("Error Registering call back : ", err.Error())
	}

}

func getMetricData(workerStats [][]stats.IState) {
	numWorkers := len(workerStats)
	for _, metric := range GaugeMetricList {
		if metric.gaugeMetric != nil {
			metric.stats = make([]StateData, 0, numWorkers)
			for wi := 0; wi < numWorkers; wi++ { // For number of workers
				for _, v := range workerStats[wi] { // For number of statistics
					if metric.MetricShortName == v.Header() {
						if fl, err := strconv.ParseFloat(v.State(), 64); err == nil {
							if wrstats, err := writeMetricsData(wi, v.Header(), fl); err == nil {
								metric.stats = append(metric.stats, wrstats)
							}
						}
					}
				}
			}
		}
	}
	return
}

func writeMetricsData(wid int, key string, value float64) (StateData, error) {
	var data StateData

	if (key == "free" || key == "used") && wid != 0 {
		// Log system metric only for worker 0
		return data, errors.New("Do not log machine resource utilization for all workers except for worker 0")
	}

	data.Name = key
	data.Value = value
	data.Dimensions = metric.WithAttributes(attribute.String("id", fmt.Sprintf("%d", wid)))

	return data, nil
}

func getMemoryUtilization() float64 {
	// Get memory usage stats from the runtime package
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Calculate memory utilization percentage
	totalAllocated := m.Alloc + m.StackInuse + m.HeapInuse
	utilization := float64(totalAllocated) / float64(m.Sys) * 100.0

	return utilization
}
