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
	"net"
	"net/http"
	"os"
	"strconv"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/dbscanserv/patch"
	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/handler"
	"juno/cmd/storageserv/redist"
	"juno/cmd/storageserv/stats"
	"juno/cmd/storageserv/storage"

	"juno/cmd/storageserv/compact"
	"juno/cmd/storageserv/watcher"
	"juno/pkg/cluster"
	"juno/pkg/initmgr"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/logging/otel"
	"juno/pkg/service"
	"juno/pkg/util"
)

type Worker struct {
	CmdStorageCommon
	optWorkerId        uint
	optListenAddresses util.StringListFlags
	optIsChild         bool
	optHttpMonAddr     string
	optZoneId          uint
	optMachineIndex    uint
	optLRUCacheSize    uint
}

func (c *Worker) Init(name string, desc string) {
	c.CmdStorageCommon.Init(name, desc)
	c.UintOption(&c.optWorkerId, "id|worker-id", 0, "specify the ID of the worker")
	c.ValueOption(&c.optListenAddresses, "listen", "specify listening address. Override Listener in config file")
	c.BoolOption(&c.optIsChild, "child", false, "specify if the worker was started by a parent process")
	c.StringOption(&c.optHttpMonAddr, "mon-addr|monitoring-address", "", "specify the http monitoring address. \n\toverride HttpMonAddr in config file")
	c.UintOption(&c.optZoneId, "zone-id", 0, "specify zone id")
	c.UintOption(&c.optMachineIndex, "machine-index", 0, "specify machine index")
	c.UintOption(&c.optLRUCacheSize, "lru-cache-mb", 0, "specify lru cache size")
}

func (c *Worker) Parse(args []string) (err error) {
	if err = c.CmdStorageCommon.Parse(args); err != nil {
		return
	}
	return
}

func (c *Worker) Exec() {
	numInheritedFDs := util.GetNumOpenFDs()

	initmgr.Register(config.Initializer, c.optConfigFile)
	initmgr.Init() //initalize config first as others depend on it

	cfg := config.ServerConfig()
	if len(c.optListenAddresses) != 0 {
		cfg.SetListeners(c.optListenAddresses)
	}
	if len(c.optHttpMonAddr) != 0 {
		cfg.HttpMonAddr = c.optHttpMonAddr
	}

	if _, err := strconv.Atoi(cfg.HttpMonAddr); err == nil {
		cfg.HttpMonAddr = ":" + cfg.HttpMonAddr
	}

	name := fmt.Sprintf("[storage %d-%d] ", c.optZoneId, c.optMachineIndex)

	initmgr.RegisterWithFuncs(glog.Initialize, glog.Finalize, cfg.LogLevel, name)
	initmgr.RegisterWithFuncs(cal.Initialize, nil, &cfg.Cal)

	initmgr.RegisterWithFuncs(stats.InitForWorker, stats.Finalize, c.optIsChild, int(c.optWorkerId), uint32(c.optZoneId), uint32(c.optMachineIndex))
	initmgr.RegisterWithFuncs(otel.Initialize, nil, &cfg.OTEL)
	initmgr.RegisterWithFuncs(storage.Initialize, storage.Finalize, int(c.optZoneId), int(c.optMachineIndex), int(c.optLRUCacheSize))
	initmgr.Init()

	stats.RunCollector(cfg.DB.GetPaths())

	logging.LogWorkerStart(int(c.optWorkerId))
	defer logging.LogWorkerExit(int(c.optWorkerId))

	patch.Init(&cfg.DbScan) // for namespace migration
	if cfg.EtcdEnabled {
		watcher.Init(cfg.ClusterName,
			uint16(c.optZoneId),
			uint16(c.optMachineIndex),
			&(cfg.Etcd),
			cfg.ShardMapUpdateDelay.Duration,
			cluster.Version)

		redist.Init(cfg.ClusterName,
			uint16(c.optZoneId),
			uint16(c.optMachineIndex),
			&(cfg.Etcd))
	}

	reqHandler := handler.NewRequestHandler()

	service, suspend := service.NewService(cfg.Config, reqHandler)

	if len(cfg.HttpMonAddr) != 0 {
		if c.optIsChild {
			if numInheritedFDs > 3 {
				if f := os.NewFile(3, ""); f != nil && util.IsSocket(f) {
					if httpListener, err := net.FileListener(f); err == nil {
						go func() {
							http.Serve(httpListener, &stats.HttpServerMux)
						}()
					}
				}
			} else {
				glog.Warningf("no inherited fds")
			}
		} else {
			go func() {
				glog.Infof("to serve HTTP on %s", cfg.HttpMonAddr)
				if err := http.ListenAndServe(cfg.HttpMonAddr, &stats.HttpServerMux); err != nil {
					glog.Warningf("fail to serve HTTP on %s, err: %s", cfg.HttpMonAddr, err)
				}
			}()
		}
	}

	service.Zoneid = int(c.optZoneId)
	if cfg.DbWatchEnabled {
		go compact.Watch(int(c.optZoneId), int(c.optMachineIndex), suspend)
	}
	service.Run()
}
