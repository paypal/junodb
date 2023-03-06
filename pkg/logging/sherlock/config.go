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
  
package sherlock

import (
	"juno/pkg/logging/cal"
	"time"

	"juno/third_party/forked/golang/glog"
)

var ShrLockConfig *Config

type Config struct {
	SherlockEndpoint string
	SherlockPort     uint32
	SherlockSvc      string
	SherlockProfile  string
	Enabled          bool
	Resolution       uint32
	ConnectTimeout   time.Duration

	ClientType string

	DatapointEndpoint   string
	EventEndpoint       string
	MainWriteQueueSize  uint32
	RetryWriteQueueSize uint32
	RetryCount          uint32
	RmCount             uint32
	MaxBackoff          time.Duration
	Timeout             time.Duration
}

func (c *Config) Validate() {
	if c.ClientType == "sherlock" {
		if len(c.SherlockSvc) <= 0 {
			c.Enabled = false
			glog.Error("Error: Sherlock service name is required.")
			if cal.IsEnabled() {
				evType := "SHRLCK_Config"
				cal.Event(evType, "FailToGetSherlockName", cal.StatusError, []byte("Error: Sherlock service name is required."))
			}
		}
		if len(c.SherlockProfile) <= 0 {
			c.Enabled = false
			glog.Error("Error: Sherlock Profile name is required.")
			if cal.IsEnabled() {
				evType := "SHRLCK_Config"
				cal.Event(evType, "FailToGetSherlockProfile", cal.StatusError, []byte("Error: Sherlock Profile name is required."))
			}
		}
	} else if c.ClientType == "sfxclient" {
		if len(c.DatapointEndpoint) == 0 {
			c.Enabled = false
			glog.Error("Error: sfxclient DatapointEndpoint is required.")
			if cal.IsEnabled() {
				evType := "SHRLCK_Config"
				cal.Event(evType, "FailToGetSherlockName", cal.StatusError, []byte("Error: sfxclient DatapointEndpoint is required."))
			}
		}
	}
	if c.ConnectTimeout == 0 {
		c.ConnectTimeout = 3 * time.Second
	}
}

func (c *Config) Default() {
	//	if c.Enabled == "" {
	//		c.Enabled = true
	//	}
	if c.Resolution == 0 {
		c.Resolution = 60
	}

	if c.SherlockEndpoint == "" {
		c.SherlockEndpoint = "sherlock-frontier-vip.qa.paypal.com"
	}

	if c.SherlockPort == 0 {
		c.SherlockPort = 80
	}

	if c.ClientType == "sfxclient" {
		if c.MainWriteQueueSize == 0 {
			c.MainWriteQueueSize = 20000
		}
		if c.RetryWriteQueueSize == 0 {
			c.RetryWriteQueueSize = 20000
		}
		if c.RetryCount == 0 {
			c.RetryCount = 1
		}
		if c.Timeout == 0 {
			c.Timeout = 1 * time.Second
		}
		if c.RmCount == 0 {
			c.RmCount = 1000
		}
		if c.MaxBackoff == 0 {
			c.MaxBackoff = 1 * time.Second
		}
	}
}

func (c *Config) Dump() {
	glog.Infof("Sherlock service : %s\n", c.SherlockSvc)
	glog.Infof("Sherlock Profile: %s\n", c.SherlockProfile)
	glog.Infof("Sherlock Enabled: %v\n", c.Enabled)
	glog.Infof("Sherlock Resolution: %v\n", c.Resolution)
}
