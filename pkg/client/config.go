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

package client

import (
	"fmt"
	"time"

	"juno/pkg/io"
	"juno/pkg/util"
)

type Duration = util.Duration

type Config struct {
	Server             io.ServiceEndpoint
	Appname            string
	Namespace          string
	RetryCount         int
	DefaultTimeToLive  int
	ConnectTimeout     Duration
	ReadTimeout        Duration
	WriteTimeout       Duration
	RequestTimeout     Duration
	ConnRecycleTimeout Duration
}

var defaultConfig = Config{
	RetryCount:         1,
	DefaultTimeToLive:  1800,
	ConnectTimeout:     Duration{100 * time.Millisecond},
	ReadTimeout:        Duration{500 * time.Millisecond},
	WriteTimeout:       Duration{500 * time.Millisecond},
	RequestTimeout:     Duration{1000 * time.Millisecond},
	ConnRecycleTimeout: Duration{9 * time.Second},
}

func SetDefaultTimeToLive(ttl int) {
	defaultConfig.DefaultTimeToLive = ttl
}

func SetDefaultTimeout(connect, read, write, request, connRecycle time.Duration) {
	defaultConfig.ConnectTimeout.Duration = connect
	defaultConfig.ReadTimeout.Duration = read
	defaultConfig.WriteTimeout.Duration = write
	defaultConfig.RequestTimeout.Duration = request
	defaultConfig.ConnRecycleTimeout.Duration = connRecycle
}

func (c *Config) SetDefault() {
	*c = defaultConfig
}

func (c *Config) validate() error {
	if err := c.Server.Validate(); err != nil {
		return err
	}
	if len(c.Appname) == 0 {
		return fmt.Errorf("Config.AppName not specified.")
	}
	if len(c.Namespace) == 0 {
		return fmt.Errorf("Config.Namespace not specified.")
	}
	/// TODO to validate others
	return nil
}
