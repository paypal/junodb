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

package stats

import (
	"fmt"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/config"
	"juno/cmd/storageserv/stats/shmstats"
	"juno/pkg/debug"
)

func InitForManager(numChildren int) (err error) {
	return shmstats.InitForManager(numChildren)
}

func initForWorker(isChild bool, workerId int, zoneId uint32, machineIndex uint32) (err error) {
	cfg := config.ServerConfig()
	if cfg == nil {
		err = fmt.Errorf("nil config")
		return
	}

	if err = shmstats.InitForWorker(isChild, workerId, zoneId, machineIndex); err != nil {
		return
	}
	htmlstats.ClusterName = cfg.ClusterName
	htmlstats.AddSection(&htmlSectServerInfoT{})
	if cfg.StateLogEnabled {
		htmlstats.AddSection(&htmlSectReqProcStatsT{})
	}
	workerIdString = fmt.Sprintf("%d", workerId)
	HttpServerMux.HandleFunc("/", indexHandler)

	initDbIndexTemplate(workerIdString)

	addPage("/stats", httpStatsHandler)

	addPage("/debug/dbstats/", httpDebugDbStatsHandler)
	addPage("/debug/config", debugConfigHandler)

	if debug.DEBUG {
		addPage("/debug/memstats", debugMemStatsHandler)
	}
	return
}

func InitForWorker(args ...interface{}) (err error) {
	sz := len(args)
	if sz < 4 {
		err = fmt.Errorf("4 argument expected")
		glog.Error(err)
		return
	}
	var (
		isChild      bool
		workerId     int
		zoneId       uint32
		machineIndex uint32
		ok           bool
	)
	if isChild, ok = args[0].(bool); !ok {
		err = fmt.Errorf("wrong argument 0 type, bool expected")
		glog.Error(err)
		return
	}
	if workerId, ok = args[1].(int); !ok {
		err = fmt.Errorf("wrong argument 1 type, int expected")
		glog.Error(err)
		return
	}
	if zoneId, ok = args[2].(uint32); !ok {
		err = fmt.Errorf("wrong argument 2 type, uint expected")
		glog.Error(err)
		return
	}
	if machineIndex, ok = args[3].(uint32); !ok {
		err = fmt.Errorf("wrong argument 3 type, uint expected")
		glog.Error(err)
		return
	}

	return initForWorker(isChild, workerId, zoneId, machineIndex)
}

func Finalize() {
	shmstats.Finalize()
}
