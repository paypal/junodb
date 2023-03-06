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
  
package app

import (
	"fmt"
	"strings"
	"sync"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/config"
	"juno/cmd/proxy/stats"
	"juno/pkg/initmgr"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/logging/sherlock"
)

type (
	MonitoringWorker struct {
		CmdProxyCommon
		optWkrMonAddrs string
		optListenAddr  string
		optIsChild     bool
		httpHandler    stats.HandlerForMonitor
	}
)

func (c *MonitoringWorker) Init(name string, desc string) {
	c.CmdProxyCommon.Init(name, desc)
	c.StringOption(&c.optWkrMonAddrs, "worker-monitoring-addresses", "", "specify the monitoring addresses of all the workers as comma separated string")
	c.StringOption(&c.optListenAddr, "listen", "", "specify listening address. Override HttpMonAddr in config file")
	c.BoolOption(&c.optIsChild, "child", false, "specify if the worker was started by a parent process")
}

func (c *MonitoringWorker) Parse(args []string) (err error) {
	if err = c.CmdProxyCommon.Parse(args); err != nil {
		return
	}
	return
}

func (c *MonitoringWorker) Exec() {
	appName := fmt.Sprintf("[proxy h] ")

	initmgr.Register(config.Initializer, c.optConfigFile)
	initmgr.Init() //initalize config first as others depend on it

	if len(config.Conf.LogLevel) == 0 || c.optLogLevel != kDefaultLogLevel {
		config.Conf.LogLevel = c.optLogLevel
	}
	if len(c.optListenAddr) != 0 {
		config.Conf.HttpMonAddr = c.optListenAddr
	}
	if !strings.Contains(config.Conf.HttpMonAddr, ":") {
		config.Conf.HttpMonAddr = ":" + config.Conf.HttpMonAddr
	}

	initmgr.RegisterWithFuncs(glog.Initialize, glog.Finalize, config.Conf.LogLevel, appName)
	initmgr.RegisterWithFuncs(cal.Initialize, nil, &config.Conf.CAL, false)
	initmgr.RegisterWithFuncs(sherlock.Initialize, nil, &config.Conf.Sherlock)
	initmgr.RegisterWithFuncs(stats.InitializeForMonitor, stats.FinalizeForMonitor)

	initmgr.Init()

	logging.LogWorkerStart(-1)
	defer logging.LogWorkerExit(-1)

	glog.Debugf("monaddrs: %s", c.optWkrMonAddrs)
	addrs := strings.Split(c.optWkrMonAddrs, ",")

	c.httpHandler.Init(c.optIsChild, addrs)

	if len(config.Conf.HttpMonAddr) == 0 {
		config.Conf.HttpMonAddr = "127.0.0.1:0"
	}
	stats.RunMonitorLogger()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		addr := config.Conf.HttpMonAddr
		if err := c.httpHandler.ListenAndServe(addr); err != nil {
			glog.Warningf("fail to serve HTTP on %s, err: %s", addr, err)
		}
	}()
	wg.Wait()
}
