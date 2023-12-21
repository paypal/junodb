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
package config

import (
	"juno/third_party/forked/golang/glog"
)

var OtelConfig *Config

type HistBuckets struct {
	Replication        []float64
	SsConnect          []float64
	Inbound            []float64
	OutboundConnection []float64
}

type Config struct {
	Host             string
	Port             uint32
	UrlPath          string
	Environment      string
	Poolname         string
	Enabled          bool
	Resolution       uint32
	UseTls           bool
	HistogramBuckets HistBuckets
}

func (c *Config) Validate() {
	if len(c.Poolname) <= 0 {
		glog.Fatal("Error: Otel Poolname is required.")
	}
	c.setDefaultIfNotDefined()
}

func (c *Config) setDefaultIfNotDefined() {
	if c.Host == "" {
		c.Host = "127.0.0.1"
	}
	if c.Port == 0 {
		c.Port = 4318
	}
	if c.Resolution == 0 {
		c.Resolution = 60
	}
	if c.Environment == "" {
		c.Environment = "PayPal"
	}
	if c.UrlPath == "" {
		c.UrlPath = "v1/datapoint"
	}
	if c.HistogramBuckets.Inbound == nil {
		c.HistogramBuckets.Inbound = []float64{200, 400, 800, 1200, 2400, 3600, 7200, 10800, 21600, 43200, 86400, 172800}
	}
	if c.HistogramBuckets.OutboundConnection == nil {
		c.HistogramBuckets.OutboundConnection = []float64{200, 400, 800, 1200, 2400, 3600, 7200, 10800, 21600, 43200, 86400, 172800}
	}
	if c.HistogramBuckets.Replication == nil {
		c.HistogramBuckets.Replication = []float64{200, 400, 800, 1200, 2400, 3600, 7200, 10800, 21600, 43200, 86400, 172800}
	}
	if c.HistogramBuckets.SsConnect == nil {
		c.HistogramBuckets.SsConnect = []float64{100, 200, 300, 400, 800, 1200, 2400, 3600, 10800, 21600, 86400, 172800}
	}
}

func (c *Config) Dump() {
	glog.Infof("Host : %s", c.Host)
	glog.Infof("Port: %d", c.Port)
	glog.Infof("Environment: %s", c.Environment)
	glog.Infof("Poolname: %s", c.Poolname)
	glog.Infof("Resolution: %d", c.Resolution)
	glog.Infof("UseTls: %t", c.UseTls)
	glog.Infof("UrlPath: %s", c.UrlPath)
	glog.Info("Inbound Bucket: ", c.HistogramBuckets.Inbound)
	glog.Info("OutboundConnection Bucket: ", c.HistogramBuckets.OutboundConnection)
	glog.Info("Replication Bucket: ", c.HistogramBuckets.Replication)
	glog.Info("SsConnect Bucket: ", c.HistogramBuckets.SsConnect)
}
