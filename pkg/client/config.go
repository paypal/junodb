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

// Package client handles the configuration for a Juno client.
package client

import (
	"fmt"

	"github.com/paypal/junodb/pkg/io"
	"github.com/paypal/junodb/pkg/util"
	cal "github.com/paypal/junodb/pkg/logging/cal/config"
)

// Duration is a type alias for util.Duration.
type Duration = util.Duration

// Config holds the configuration values for the Juno client.
type Config struct {
	Server    io.ServiceEndpoint
	Appname   string
	Namespace string

	DefaultTimeToLive int
	ConnPoolSize      int
	ConnectTimeout    Duration
	ResponseTimeout   Duration
	BypassLTM         bool
	Cal               cal.Config
}

func (c *Config) validate(useGetTLS bool) error {
	if err := c.Server.Validate(); err != nil {
		return err
	}
	if len(c.Appname) == 0 {
		return fmt.Errorf("Config.AppName not specified.")
	}
	if len(c.Namespace) == 0 {
		return fmt.Errorf("Config.Namespace not specified.")
	}
	if c.DefaultTimeToLive < 0 {
		return fmt.Errorf("Config.DefaultTimeToLive is negative.")
	}
	return nil
}
