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

type Config struct {
	Host        string
	Port        uint32
	UrlPath     string
	Environment string
	Poolname    string
	Enabled     bool
	Resolution  uint32
	UseTls      bool
}

func (c *Config) Validate() {
	if len(c.Poolname) <= 0 {
		glog.Fatal("Error: Otel Poolname is required.")
	}
}

func (c *Config) SetPoolName(name string) {
	c.Poolname = name
}

func (c *Config) Default() {
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
		c.Environment = "OpenSource"
	}
	if c.UrlPath == "" {
		c.UrlPath = "v1/datapoint"
	}
}

func (c *Config) Dump() {
	glog.Infof("Host : %s", c.Host)
	glog.Infof("Port: %d", c.Port)
	glog.Infof("Environment: %s", c.Environment)
	glog.Infof("Poolname: %s", c.Poolname)
	glog.Infof("Resolution: %d", c.Resolution)
	glog.Info("UseTls: %b", c.UseTls)
}
