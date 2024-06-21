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

package main

import (
	"flag"
	"fmt"

	"github.com/paypal/junodb/test/testutil/mock"
	"github.com/paypal/junodb/third_party/forked/golang/glog"
)

func main() {

	var port int

	config := mock.DefaultSSConfig
	flag.IntVar(&port, "p", 10070, "port")
	flag.IntVar(&config.MeanDelay, "delay", 0, "desiredMeanDelay") // in us
	flag.IntVar(&config.StdDevDelay, "delay_sd", 0, "desiredMeanDelay")
	flag.IntVar(&config.ValueSize, "size", 1024, "desiredMeanSize")
	flag.IntVar(&config.StdDevSize, "size_sd", 100, "desiredMeanSize")

	flag.Parse() // Scan the arguments list
	flag.Lookup("logtostderr").Value.Set("true")

	glog.InitLogging(config.LogLevel, " [ss] ")

	glog.Info("Starting juno mockss")
	glog.Infof("MeanDelay: %d, sdv: %d, size: %d, sdv: %d ",
		config.MeanDelay, config.StdDevDelay, config.ValueSize, config.StdDevSize)

	var listenAddr = fmt.Sprintf(":%d", uint16(port))
	config.SetListeners([]string{listenAddr})
	service := mock.NewMockStorageService(config)
	service.Run()
}
