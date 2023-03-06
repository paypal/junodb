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
  
package unittest

import (
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/config"
	"juno/pkg/client"
	"juno/pkg/cluster"
	"juno/pkg/etcd"
	"juno/pkg/io"
	"juno/pkg/logging/cal"
	calcfg "juno/pkg/logging/cal/config"
	"juno/pkg/sec"
	"juno/pkg/util"

	"juno/pkg/logging/sherlock"
	sherlockcfg "juno/pkg/logging/sherlock"
	"juno/test/testutil/mock"
	"juno/test/testutil/server"
)

var testConfig = server.ClusterConfig{
	ProxyAddress: io.ServiceEndpoint{Addr: "127.0.0.1:8082"},
	ProxyConfig:  &config.Conf,
	StorageServer: server.ServerDef{
		Type: "mockss",
	},
	CAL: calcfg.Config{
		Host:             "127.0.0.1",
		Port:             1118,
		Environment:      "PayPal",
		Poolname:         "fakess",
		MessageQueueSize: 10000,
		Enabled:          true,
		CalType:          "file",
		CalLogFile:       "cal.log",
	},
	LogLevel: "warning",
	Sec:      sec.DefaultConfig,
	Sherlock: sherlockcfg.Config{
		Enabled: false,
	},
}

var TestCluster *server.Cluster
var Mockclient *mock.MockClient
var hostip = "127.0.0.1"

func mainSetup() {
	testConfig.ProxyConfig.ClusterInfo.ConnInfo = [][]string{
		{"localhost:5010", "localhost:5011", "localhost:5012"},
		{"localhost:6010", "localhost:6011", "localhost:6012"},
		{"localhost:7010", "localhost:7011", "localhost:7012"},
		{"localhost:8010", "localhost:8011", "localhost:8012"},
		{"localhost:9010", "localhost:9011", "localhost:9012"}}

	testConfig.ProxyConfig.ClusterInfo.NumShards = 100
	testConfig.ProxyConfig.ClusterInfo.NumZones = 5
	cal.InitWithConfig(&testConfig.CAL)
	sec.Initialize(&testConfig.Sec, sec.KFlagClientTlsEnabled|sec.KFlagEncryptionEnabled)
	sherlock.InitWithConfig(&testConfig.Sherlock)

	var chWatch chan int
	var rw cluster.IReader
	clusterInfo := &cluster.ClusterInfo[0]

	if testConfig.ProxyConfig.EtcdEnabled {
		chWatch = etcd.WatchForProxy()
		etcd.Connect(&testConfig.ProxyConfig.Etcd, testConfig.ProxyConfig.ClusterName)
		rw = etcd.GetClsReadWriter()
		if rw == nil {
			glog.Exitf("no etcd setup")
		}
		clusterInfo.Read(rw)
	} else {

		clusterInfo.PopulateFromConfig()
	}
	cluster.Initialize(&cluster.ClusterInfo[0], &testConfig.ProxyConfig.Outbound, chWatch, rw)

	TestCluster = server.NewClusterWithConfig(&testConfig)

	TestCluster.Start()

	cliCfg := client.Config{
		Server:            testConfig.ProxyAddress,
		Appname:           "mockclient",
		Namespace:         "ns",
		DefaultTimeToLive: 3600,
		ConnectTimeout:    util.Duration{4000 * time.Millisecond},
		ReadTimeout:       util.Duration{1500 * time.Millisecond},
		WriteTimeout:      util.Duration{1500 * time.Millisecond},
		RequestTimeout:    util.Duration{3000 * time.Millisecond},
	}
	Mockclient = mock.NewMockClient(config.Conf.ClusterInfo.ConnInfo, cliCfg)
}

func mainTeardown() {
	TestCluster.Stop()
}

func TestMain(m *testing.M) {
	var (
		logLevel    string
		httpEnabled bool
	)
	flag.StringVar(&logLevel, "log_level", "", "specify log level")
	flag.BoolVar(&httpEnabled, "http", true, "enable http")
	flag.Parse()

	if httpEnabled {
		go func() {
			http.ListenAndServe(":6060", nil)
		}()
	}

	testConfig.LogLevel = "warning"
	if logLevel != "" {
		testConfig.LogLevel = logLevel
	}
	glog.InitLogging(testConfig.LogLevel, " [ut] ")

	//temporary signal handling
	sigs := make(chan os.Signal)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func(sigCh chan os.Signal) {
	loop:
		for {
			select {
			case sig := <-sigCh:
				glog.Debug("signal: ", sig)
				break loop
			}
		}
		os.Exit(0)
	}(sigs)

	mainSetup()
	rc := m.Run()
	mainTeardown()
	os.Exit(rc)
}
