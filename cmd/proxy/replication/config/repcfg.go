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

package repconfig

import (
	"fmt"
	"strings"
	"time"

	"juno/pkg/io"
	"juno/pkg/util"
)

var (
	kDefaultName = "default"

	kDefaultReplicationIoConfig = io.OutboundConfig{
		ConnectTimeout:        util.Duration{1 * time.Second},
		ReqChanBufSize:        8092,
		MaxPendingQueSize:     8092,
		PendingQueExtra:       300,
		MaxBufferedWriteSize:  64 * 1024, // default 64k
		ReconnectIntervalBase: 100,       // 100ms
		ReconnectIntervalMax:  20000,     // 20 seconds
		NumConnsPerTarget:     1,
		IOBufSize:             64 * 1024,
		ConnectRecycleT:       util.Duration{180 * time.Second},
		EnableConnRecycle:     true,
		GracefulShutdownTime:  util.Duration{2 * time.Second},
	}
	DefaultConfig = Config{
		IO: io.OutboundConfigMap{kDefaultName: kDefaultReplicationIoConfig},
	}
)

type (
	ReplicationTarget struct {
		Name string
		io.ServiceEndpoint
		UseMayflyProtocol bool
		Namespaces        []string
		BypassLTMEnabled  bool
	}

	Config struct {
		Targets []ReplicationTarget
		IO      io.OutboundConfigMap
	}
)

func (c *Config) GetIoConfig(target *ReplicationTarget) *io.OutboundConfig {
	if target != nil {

		if cfg, ok := c.IO[target.Name]; ok {
			return &cfg
		} else {
			if cfg, ok = c.IO[kDefaultName]; ok {
				return &cfg
			}
		}
	}
	return &kDefaultReplicationIoConfig
}

func (c *Config) Validate() {

	for i := len(c.Targets) - 1; i >= 0; i-- {
		t := &c.Targets[i]
		if strings.TrimSpace(t.Addr) == "" {
			c.Targets = append(c.Targets[:i], c.Targets[i+1:]...)
		} else {
			if len(t.Name) == 0 {
				c.Targets[i].Name = fmt.Sprintf("t%d", i)
			}
			if len(t.Network) == 0 {
				c.Targets[i].Network = "tcp"
			}
		}
	}
	c.IO.SetDefaultIfNotDefined()
}
