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
	"errors"
	"flag"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/logging/sherlock"
)

var f sherlock.MetricSender

func Init(endpoint string, pool string) {
	sherlock.ShrLockConfig = &sherlock.Config{
		SherlockEndpoint: endpoint,
		SherlockSvc:      pool,
		SherlockProfile:  "junoserv",
		Enabled:          true,
		Resolution:       60,
	}
	var err error
	sherlock.Initialize(sherlock.ShrLockConfig)
	// f, err = sherlock.NewFrontierClientNormalEndpoints(
	// 	pool,
	// 	"junoserv")
	f = sherlock.SherlockClient

	if err != nil {
		glog.Errorf("Sherlock client init error=%s", err)
		f = nil
	}
	time.Sleep(10 * time.Second)
}

func Send(count float64, pool string, host string) error {
	dims := sherlock.Dims{
		sherlock.GetDimName(): pool,
		"host":                host}
	var data [1]sherlock.FrontierData
	data[0].Name = "etcd_up_count"
	data[0].Value = count
	data[0].MetricType = sherlock.Gauge

	if f == nil {
		return errors.New("Sherlock client not initialized.")
	}
	err := f.SendMetric(dims, data[:1], time.Now())
	if err != nil {
		glog.Errorf("Sherlock send error=%s", err)
	}
	return err
}

func main() {
	var activeCount float64
	var endpoint, pool, host string

	flag.Float64Var(&activeCount, "a", 0, "active count")
	flag.StringVar(&endpoint, "e", "sherlock-frontier-vip.qa.paypal.com", "endpoint")
	flag.StringVar(&pool, "p", "junoclusterserv-gen", "pool")
	flag.StringVar(&host, "h", "localhost", "host")
	flag.Parse()

	Init(endpoint, pool)
	if f == nil {
		return
	}

	for i := 0; i < 2; i++ {
		if err := Send(activeCount, pool, host); err == nil {
			break
		}
		time.Sleep(2 * time.Second)
	}
	time.Sleep(5 * time.Second)
	f.Stop()
	time.Sleep(2 * time.Second)
}
