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

var CalConfig *Config

type Config struct {
	Host              string
	Port              uint32
	Environment       string
	Poolname          string
	ConnectionTimeout uint32
	Label             string
	MessageQueueSize  uint32
	CalType           string
	CalLogFile        string
	Enabled           bool
	LogLevel          string
	LogInfoPercent    float32
	NumberConnections uint32
}

func (c *Config) Validate() {
	if len(c.Poolname) <= 0 {
		glog.Fatal("Error: Cal Poolname is required.")
	}
	if c.NumberConnections > 5 {
		c.NumberConnections = 5
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
		c.Port = 1118
	}
	if c.Environment == "" {
		c.Environment = "PayPal"
	}
	if c.ConnectionTimeout == 0 {
		c.ConnectionTimeout = 1
	}
	if c.MessageQueueSize == 0 {
		c.MessageQueueSize = 10000
	}
	if c.CalType == "" {
		c.CalType = "socket"
	}
	if c.CalLogFile == "" {
		c.CalLogFile = "callog.txt"
	}
	if c.LogLevel == "" {
		c.LogLevel = "info"
	}
	if c.LogInfoPercent == 0 {
		c.LogInfoPercent = 0.1
	}
}

func (c *Config) Dump() {
	glog.Infof("Host : %s", c.Host)
	glog.Infof("Port: %d", c.Port)
	glog.Infof("Environment: %s", c.Environment)
	glog.Infof("Poolname: %s", c.Poolname)
	glog.Infof("ConnectionTimeout:%d", c.ConnectionTimeout)
	glog.Infof("Label: %s", c.Label)
	glog.Infof("MessageQueueSize: %d", c.MessageQueueSize)
	glog.Infof("CalType: %s", c.CalType)
	glog.Infof("CalLogFile: %s\n", c.CalLogFile)
	glog.Infof("CalEnabled: %v\n", c.Enabled)
}
