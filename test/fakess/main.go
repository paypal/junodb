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

//go:build ignore
// +build ignore

package main

import (
	"flag"
	"net"
	"strconv"

	"github.com/paypal/junodb/pkg/logging/cal"
	"github.com/paypal/junodb/pkg/service"

	"github.com/paypal/junodb/third_party/forked/golang/glog"
)

var (
	meanDelay   int
	stdDevDelay int
	valueSize   int
	stdDevSize  int
	value       []byte
)

func main() {

	var configFlag = flag.String("config", "./ss_config.toml", "configfile")
	var portFlag = flag.Int("p", 10070, "port")
	var meanFlag = flag.Int("delay", 0, "desiredMeanDelay") // in us
	var mean_sdFlag = flag.Int("delay_sd", 0, "desiredMeanDelay")
	var sizeFlag = flag.Int("size", 1024, "desiredMeanSize")
	var size_sdFlag = flag.Int("size_sd", 100, "desiredMeanSize")
	var configPath string = string(*configFlag)

	flag.Parse() // Scan the arguments list
	flag.Lookup("logtostderr").Value.Set("true")

	initialize(configPath)
	glog.Infof("Starting juno fakess")

	var (
		meanDelay   int
		stdDevDelay int
		valueSize   int
		stdDevSize  int
	)

	meanDelay = int(*meanFlag)
	stdDevDelay = int(*mean_sdFlag)
	valueSize = int(*sizeFlag)
	stdDevSize = int(*size_sdFlag)

	var listenAddr = ":" + strconv.Itoa(*portFlag)

	l, err := net.Listen("tcp", listenAddr)
	if err != nil {
		glog.Fatal("Cannot Listen on ", listenAddr)
	}

	reqHandler := NewRequestHandler(meanDelay, stdDevDelay, valueSize, stdDevSize)

	service := service.New(&Conf.Inbound, reqHandler, nil, nil, l)
	service.Run()
}

func initialize(configPath string) {
	if err := LoadConfig(configPath); err != nil {
		glog.Fatal("Failed to load ", configPath, " .", err)
	}
	glog.InitLogging(Conf.LogLevel, " [ss] ")
	cal.InitWithConfig(&Conf.CAL)
}
