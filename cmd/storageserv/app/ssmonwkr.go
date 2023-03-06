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
	"net/http"
	"strings"
	"sync"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/stats"
	"juno/pkg/initmgr"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/logging/sherlock"
)

type (
	MonitoringWorker struct {
		CmdStorageCommon
		optWkrMonAddrs string
		optListenAddr  string
		optIsChild     bool

		workerUrls  []string
		httpHandler stats.HttpHandlerForMonitor
	}
)

func (c *MonitoringWorker) Init(name string, desc string) {
	c.CmdStorageCommon.Init(name, desc)
	c.StringOption(&c.optWkrMonAddrs, "worker-monitoring-addresses", "", "specify the monitoring addresses of all the workers as comma separated string")
	c.StringOption(&c.optListenAddr, "listen", "", "specify listening address. Override HttpMonAddr in config file")
	c.BoolOption(&c.optIsChild, "child", false, "specify if the worker was started by a parent process")
}

func (c *MonitoringWorker) Parse(args []string) (err error) {
	if err = c.CmdStorageCommon.Parse(args); err != nil {
		return
	}
	return
}

func (c *MonitoringWorker) Exec() {
	initmgr.Register(config.Initializer, c.optConfigFile)
	initmgr.Init() //initalize config first as others depend on it

	cfg := config.ServerConfig()

	if len(cfg.LogLevel) == 0 || c.optLogLevel != kDefaultLogLevel {
		cfg.LogLevel = c.optLogLevel
	}
	initmgr.RegisterWithFuncs(glog.Initialize, glog.Finalize, cfg.LogLevel, "[monitor] ")

	initmgr.RegisterWithFuncs(cal.Initialize, nil, &cfg.Cal, false)
	initmgr.RegisterWithFuncs(sherlock.Initialize, nil, &cfg.Sherlock)
	initmgr.RegisterWithFuncs(stats.InitializeForMonitor, stats.FinalizeForMonitor, nil)

	initmgr.Init()

	logging.LogWorkerStart(-1)
	defer logging.LogWorkerExit(-1)

	if len(c.optListenAddr) != 0 {
		cfg.HttpMonAddr = c.optListenAddr
	}
	if !strings.Contains(cfg.HttpMonAddr, ":") {
		cfg.HttpMonAddr = ":" + cfg.HttpMonAddr
	}

	addrs := strings.Split(c.optWkrMonAddrs, ",")

	c.httpHandler.Init(c.optIsChild, addrs)
	if len(cfg.HttpMonAddr) == 0 {
		cfg.HttpMonAddr = "127.0.0.1:0"
	}
	stats.RunMonitorLogger()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		addr := cfg.HttpMonAddr
		glog.Infof("to serve HTTP on %s", addr)
		if err := http.ListenAndServe(addr, &stats.HttpServerMux); err != nil {
			glog.Warningf("fail to serve HTTP on %s, err: %s", addr, err)
		}
	}()
	wg.Wait()
}
