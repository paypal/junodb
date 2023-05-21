// Copyright 2023 PayPal Inc.
//
// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package client handles the configuration for a Juno client.
package client

import (
	"fmt"
	"time"

	"github.com/paypal/junodb/pkg/io"
	"github.com/paypal/junodb/pkg/util"
)

// Duration is a type alias for util.Duration.
type Duration = util.Duration

// Config holds the configuration values for the Juno client.
type Config struct {
	Server             io.ServiceEndpoint // Server defines the ServiceEndpoint of the Juno server.
	Appname            string             // Appname is the name of the application.
	Namespace          string             // Namespace is the namespace of the application.
	RetryCount         int                // RetryCount is the maximum number of retries.
	DefaultTimeToLive  int                // DefaultTimeToLive is the default TTL (time to live) for requests.
	ConnectTimeout     Duration           // ConnectTimeout is the timeout for establishing connections.
	ReadTimeout        Duration           // ReadTimeout is the timeout for read operations.
	WriteTimeout       Duration           // WriteTimeout is the timeout for write operations.
	RequestTimeout     Duration           // RequestTimeout is the timeout for each request.
	ConnRecycleTimeout Duration           // ConnRecycleTimeout is the timeout for connection recycling.
}

// defaultConfig defines the default configuration values.
var defaultConfig = Config{
	RetryCount:         1,
	DefaultTimeToLive:  1800,
	ConnectTimeout:     Duration{100 * time.Millisecond},
	ReadTimeout:        Duration{500 * time.Millisecond},
	WriteTimeout:       Duration{500 * time.Millisecond},
	RequestTimeout:     Duration{1000 * time.Millisecond},
	ConnRecycleTimeout: Duration{9 * time.Second},
}

// SetDefaultTimeToLive sets the default time to live (TTL) for the configuration.
func SetDefaultTimeToLive(ttl int) {
	defaultConfig.DefaultTimeToLive = ttl
}

// SetDefaultTimeout sets the default timeout durations for the configuration.
func SetDefaultTimeout(connect, read, write, request, connRecycle time.Duration) {
	defaultConfig.ConnectTimeout.Duration = connect
	defaultConfig.ReadTimeout.Duration = read
	defaultConfig.WriteTimeout.Duration = write
	defaultConfig.RequestTimeout.Duration = request
	defaultConfig.ConnRecycleTimeout.Duration = connRecycle
}

// SetDefault updates the current Config to match the default Config.
func (c *Config) SetDefault() {
	*c = defaultConfig
}

// validate checks if the required fields of the Config are correctly populated.
// It validates the Server field and checks if Appname and Namespace are specified.
// It returns an error if any of the above conditions are not met.
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
	// TODO to validate others
	return nil
}
