// Copyright 2023 PayPal Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package otel

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/paypal/junodb/pkg/stats"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/asyncfloat64"
)

const (
	MachineCpuUsed string = string("machineCpuUsed")
	ProcessCpuUsed string = string("machineCpuUsed")
	MachineMemoryUsed
	ProcessMemoryUsed
)

var (
	machineCpuUsedOnce    sync.Once
	processCpuUsedOnce    sync.Once
	machineMemoryUsedOnce sync.Once
	processMemoryUsedOnce sync.Once
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
)

var (
	machTime    time.Time
	machCpuTick uint16
	machUser    uint64
	machSystem  uint64
)

type GaugeMetric struct {
	MetricName  string
	metricDesc  string
	gaugeMetric asyncfloat64.Gauge
	createGauge *sync.Once
	stype       ServerType
}

var GaugeMetricMap map[string]*GaugeMetric = map[string]*GaugeMetric{
	"mCPU": {"machCpuUsed", "CPU utilization of the host", nil, &machineCpuUsedOnce, SvrTypeAll},
	"pCPU": {"procCpuUsed", "CPU utilization of individual Juno instance", nil, &processCpuUsedOnce, SvrTypeAll},
	"mMem": {"machMemUsed", "Memory utilization of the host", nil, &machineMemoryUsedOnce, SvrTypeAll},
	"pMem": {"procMemUsed", "Memory utilization of individual Juno instance", nil, &processMemoryUsedOnce, SvrTypeAll},

	"nBShd": {"badShard", "Number of bad shards", nil, &badShardOnce, SvrTypeProxy},
	"nAShd": {"alertShard", "number of shards with no redundancy", nil, &alertShardOnce, SvrTypeProxy},
	"nWShd": {"warningShard", "number of shards with bad SS", nil, &warningShardOnce, SvrTypeProxy},

	"keys":      {"key_count", "Key Counte in rocksDB", nil, &keyCountOnce, SvrTypeStorage},
	"free":      {"free_mb_storage_space", "Free Storage Space (mbytes)", nil, &freeStorageOnce, SvrTypeStorage},
	"used":      {"storage_used_mb", "Used Storage Space (mbytes)", nil, &usedStorageOnce, SvrTypeStorage},
	"LN":        {"LN_level", "Max LN Level in Rocksdb", nil, &LNLevelOnce, SvrTypeStorage},
	"compSec":   {"comp_sec", "Compaction Sec", nil, &compSecOnce, SvrTypeStorage},
	"compCount": {"comp_count", "Compaction Count", nil, &compCountOnce, SvrTypeStorage},
	"pCompKB":   {"pending_comp_kbytes", "Pending Compaction KBytes", nil, &pendingCompOnce, SvrTypeStorage},
	"stall":     {"stall_write_rate", "Actural Delayed Write Rate", nil, &stallOnce, SvrTypeStorage},
}

// Represents the list of workers with stats
type CurrentStatsData struct {
	WorkerState []WorkerStats
}

// Represents list of stats emitted by a worker
type WorkerStats struct {
	StatData []StateData
}

// Represents stats by worker
type StateData struct {
	Name       string
	Value      float64
	Dimensions []attribute.KeyValue
}

var CurrStatsData CurrentStatsData

func InitSystemMetrics(serverType ServerType, workerStats [][]stats.IState) {
	meter := global.Meter(MeterName)
	var stateLogGauge []instrument.Asynchronous = make([]instrument.Asynchronous, len(GaugeMetricMap))
	var i int = 0
	//InitMachCpuUsage()
	for _, element := range GaugeMetricMap {
		if element.stype == serverType || element.stype == SvrTypeAll {
			element.createGauge.Do(func() {
				// TODO instead of element use GaugeMetricMap[index]
				element.gaugeMetric, _ = meter.AsyncFloat64().Gauge(
					PopulateJunoMetricNamePrefix(element.MetricName),
					//instrument.WithUnit(unit.Dimensionless),
					instrument.WithDescription(element.metricDesc),
				)
				stateLogGauge[i] = element.gaugeMetric
				i++
			})
		}
	}

	if err := meter.RegisterCallback(
		stateLogGauge,
		func(ctx context.Context) {
			wstate := getMetricData(workerStats)
			for _, workerState := range wstate {
				for _, state := range workerState.StatData {
					gMetric, ok := GaugeMetricMap[state.Name]
					if ok {
						if gMetric.gaugeMetric != nil {
							gMetric.gaugeMetric.Observe(ctx, state.Value, state.Dimensions...)
						}
					}
				}
			}
		},
	); err != nil {
		///Just ignore
	}
}

func getMetricData(workerStats [][]stats.IState) []WorkerStats {
	numWorkers := len(workerStats)
	var wsd []WorkerStats
	wsd = make([]WorkerStats, numWorkers)
	for wi := 0; wi < numWorkers; wi++ { // For number of workers
		var sdata []StateData
		sdata = make([]StateData, 0, len(workerStats[wi]))
		for _, v := range workerStats[wi] { // For number of statistics
			if fl, err := strconv.ParseFloat(v.State(), 64); err == nil {
				if wrstats, err := writeMetricsData(wi, v.Header(), fl); err == nil {
					sdata = append(sdata, wrstats)
				}
			}
		}
		wsd[wi].StatData = sdata
	}

	return wsd
}

func writeMetricsData(wid int, key string, value float64) (StateData, error) {
	var data StateData
	_, ok := GaugeMetricMap[key]
	if !ok {
		// Only record the metrics in the map
		return data, errors.New("Metirc not found in Map")
	}

	if (key == "mCPU" || key == "mMem") && wid != 0 {
		// Log system metric only for worker 0
		return data, errors.New("Do not log machine resource utilization for all workers except for worker 0")
	}

	data.Name = key
	data.Value = value
	data.Dimensions = []attribute.KeyValue{attribute.String("id", fmt.Sprintf("%d", wid))}

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
