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

// -*- tab-width: 2 -*-

package sherlock

import (
	"fmt"
	"reflect"
	"runtime"
	"sort"
	"strings"

	frontier "juno/pkg/logging/sherlock/sherlockproto"

	sync "sync"
	"time"
)

//import sync "github.com/sasha-s/go-deadlock"

var defaultPercentiles = []float64{0.5, 0.99, 0.999, 0.9999}

// Dims is type of a map of string dimensions used for aggregation
// in Sherlock
type Dims map[string]string

type incoming struct {
	value float64
	name  string
}

type timeSeries struct {
	dims     Dims
	data     map[string][]float64
	i        chan incoming
	f        *FrontierClient
	finished chan bool
	mutex    *sync.RWMutex
}

// TimeSeriesRecorder is used to accumlate and then Sherlock post data about
// Timeseries without too much fuss.
type TimeSeriesRecorder interface {
	WithSherlockTimed(fType)
	MarkDistribution(float64, string)
	GetData(string) []float64
	Stop()
}

// NewTimeSeriesRecorder inits a struct to record TS data with given
// frontier Client
func NewTimeSeriesRecorder(c *FrontierClient, d Dims) TimeSeriesRecorder {
	ts := timeSeries{d,
		make(map[string][]float64, 2000000),
		make(chan incoming, 100000),
		c,
		make(chan bool, 2),
		&sync.RWMutex{}}
	go ts.sendData()
	go ts.markDistributionReal()
	return &ts
}

func percentiles(percents []float64,
	vals []float64) []float64 {
	var res = make([]float64, len(percents))
	l := len(vals)
	sort.Float64s(vals)
	if l > 0 {
		for i, v := range percents {
			index := int(v * float64(l))
			res[i] = vals[index]
		}
	} else {
		for i := range percents {
			res[i] = 0
		}
	}
	return res
}

// MarkDistribution adds a point to the distribution whose
// extrema will be sent to Sherlock (p50 p95 p99).
func (m *timeSeries) MarkDistribution(value float64, name string) {
	select {
	case m.i <- incoming{value, name + "_"}:
		// ok
	default:
		// eh
	}
}

func (m *timeSeries) markDistributionReal() {
	for {
		select {
		case inc := <-m.i:
			m.mutex.Lock()
			if len(m.data[inc.name]) < 2000000 {
				m.data[inc.name] = append(m.data[inc.name], inc.value) // TODO online percentile algorithm
			}
			m.mutex.Unlock()
		case <-m.finished:
			return
		default:
		}
	}
}

func (m *timeSeries) GetData(k string) []float64 {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if v, ok := m.data[k]; ok {
		tmp := make([]float64, len(v))
		copy(tmp, v)
		return tmp
	}
	return nil
}

func (m *timeSeries) getStopping() bool {
	select {
	case d := <-m.finished:
		return d
	default:
		return false
	}
	return true // not sure this can happen
}

var timeToSleep = 60

// sendData actually does the sending and summarizing of the distributions
func (m *timeSeries) sendData() {
	for {
		if m.getStopping() {
			return
		}
		time.Sleep(time.Duration(timeToSleep) * time.Second)
		m.mutex.Lock()
		data := make([]FrontierData, 0)
		for k, v := range m.data { ///
			d := FrontierData{}
			res := percentiles(defaultPercentiles, v)
			for i, p := range defaultPercentiles {
				d.Name = k + "p" + fmt.Sprintf("%.0f", 10000*p)
				d.Value = res[i]
				d.MetricType = frontier.MetricProto_GAUGE
				data = append(data, d)
			}
			d.Name = k + "count"
			d.Value = float64(len(v))
			d.MetricType = frontier.MetricProto_GAUGE
			data = append(data, d)
			m.f.Send(m.dims, data, time.Now(), 60)
			m.data[k] = make([]float64, 2000000)
		}
		m.mutex.Unlock()
	}
}

// Stop requests the frontier client and goroutines to stop
func (m *timeSeries) Stop() {
	m.finished <- true
	m.finished <- true
}

type fType func()

// WithSherlockTimed takes a function and then sends info about the
// latency distribution of its execution to Sherlock.
func (m *timeSeries) WithSherlockTimed(f fType) {

	nameOfF := runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
	namesOfF := strings.Split(nameOfF, ".")
	nameOfF = namesOfF[len(namesOfF)-1]
	now := float64(time.Now().UnixNano()) * float64(time.Nanosecond) / float64(time.Millisecond)
	f()
	then := float64(time.Now().UnixNano()) * float64(time.Nanosecond) / float64(time.Millisecond)
	m.MarkDistribution(float64(then-now), "_timed_"+nameOfF)
}
