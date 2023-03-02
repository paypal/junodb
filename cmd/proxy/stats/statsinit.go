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
//  Package utility provides the utility interfaces for mux package
//  
package stats

import (
	"fmt"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/stats/shmstats"
)

const (
	KTypeManager          = Type(1)
	KTypeMonitor          = Type(2)
	KTypeWorker           = Type(3)
	KTypeStandAloneWorker = Type(4)
)

type (
	Type int
)

/*
	Arguments
	arg 0: Type stats.Type
	arg 1: WorkerId int, if Type == KTypeWorker
*/
func Initialize(args ...interface{}) (err error) {
	var (
		workerId  int
		ok        bool
		statsType Type
	)

	if len(args) > 0 {
		if statsType, ok = args[0].(Type); !ok {
			err = fmt.Errorf("a stats.Type argument expected")
			glog.Error(err)
			return
		}
		if statsType == KTypeWorker && len(args) > 1 {
			if workerId, ok = args[1].(int); !ok {
				err = fmt.Errorf("an integer argument expected. args[1]: %v", args[1])
				glog.Error(err)
				return
			}
		}
	}

	switch statsType {
	case KTypeManager:
		err = shmstats.InitForManager()
	case KTypeMonitor:
		err = shmstats.InitForMonitor()
	case KTypeWorker:
		initStatsForWorker(workerId)
		err = shmstats.InitForWorker(true, workerId)
	case KTypeStandAloneWorker:
		initStatsForWorker(0)
		err = shmstats.InitForWorker(false, 0)
	}
	return
}

func Finalize() {
	shmstats.Finalize()
}
