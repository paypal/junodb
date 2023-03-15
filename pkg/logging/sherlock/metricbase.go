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
	"fmt"
	"juno/third_party/forked/golang/glog"
	"os"
	sync "sync"
	"time"

	frontier "juno/pkg/logging/sherlock/sherlockproto"
)

var clientID uint32
var clientIDMutex = sync.RWMutex{}

// FrontierData is used to pass name/type/value to library
type FrontierData struct {
	Name       string
	MetricType frontier.MetricProto_MetricTypeProto
	Value      float64
}

// MetricSender API
type MetricSender interface {
	SendMetric(dim map[string]string, data []FrontierData, when time.Time) error
	Stop()
}

var hostName string

func init() {
	var err error
	hostName, err = os.Hostname()
	if err != nil {
		hostName = "unknown"
	}
}

// SherlockClient should be the only metric client interface right now to be
// used in other components. // It will be initialized in InitWithConfig upon
// the configuration to a specific implementation
var SherlockClient MetricSender

func Initialize(args ...interface{}) (err error) {
	var isServerManager bool
	sz := len(args)
	if sz == 0 {
		err = fmt.Errorf("config argument expected")
		glog.Error(err)
		return
	}
	var c *Config
	var ok bool
	if c, ok = args[0].(*Config); !ok {
		err = fmt.Errorf("wrong argument type")
		glog.Error(err)
		return
	}
	if len(args) > 1 {
		if isServerManager, ok = args[1].(bool); !ok {
			err = fmt.Errorf("bool argument expected")
			return
		}
	}
	if !isServerManager {
		InitWithConfig(c)
	}
	return
}

// InitWithConfig is a top level intializer for metric log.
func InitWithConfig(conf *Config) {
	if conf != nil {
		if len(conf.ClientType) != 0 {
			switch conf.ClientType {
			case "sherlock":
				initFCWithConfig(conf)
			case "sfxclient":
				initSfxWithConfig(conf)
			default:
				glog.Errorln("unknow client type")
				initFCWithConfig(conf)
			}
		} else {
			initFCWithConfig(conf)
		}
	}
}
