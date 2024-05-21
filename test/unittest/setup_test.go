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

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/paypal/junodb/cmd/proxy/config"
	"github.com/paypal/junodb/pkg/client"
	"github.com/paypal/junodb/pkg/cluster"
	"github.com/paypal/junodb/pkg/etcd"
	"github.com/paypal/junodb/pkg/io"
	"github.com/paypal/junodb/pkg/util"

	"github.com/paypal/junodb/test/testutil/mock"
	"github.com/paypal/junodb/test/testutil/server"
)

var testConfig = server.ClusterConfig{
	ProxyAddress: io.ServiceEndpoint{Addr: "127.0.0.1:8082"},
	ProxyConfig:  &config.Conf,
	StorageServer: server.ServerDef{
		Type: "mockss",
	},
	LogLevel: "warning",
}

var TestCluster *server.Cluster
var Mockclient *mock.MockClient
var hostip = "127.0.0.1"

func mainSetup() {
	testConfig.ProxyConfig.ClusterInfo.ConnInfo = [][]string{
		{"127.0.0.1:5010", "127.0.0.1:5011", "127.0.0.1:5012"},
		{"127.0.0.1:6010", "127.0.0.1:6011", "127.0.0.1:6012"},
		{"127.0.0.1:7010", "127.0.0.1:7011", "127.0.0.1:7012"},
		{"127.0.0.1:8010", "127.0.0.1:8011", "127.0.0.1:8012"},
		{"127.0.0.1:9010", "127.0.0.1:9011", "127.0.0.1:9012"}}

	testConfig.ProxyConfig.ClusterInfo.NumShards = 100
	testConfig.ProxyConfig.ClusterInfo.NumZones = 5
	testConfig.ProxyConfig.Outbound.ReconnectIntervalBase = 15000 //time to wait for proxy to connect to ss

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

	TestCluster = server.NewClusterWithConfig(&testConfig)
	cluster.Initialize(&cluster.ClusterInfo[0], &testConfig.ProxyConfig.Outbound, chWatch, rw, nil, nil)
	glog.Info("wait 20 secs for storageserv start up, then start TestCluster")
	time.Sleep(20 * time.Second)
	TestCluster.Start() //wait and start TestCluster after ss port is up

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
	glog.Warning("start server, please wait")
	mainSetup()
	rc := m.Run()
	mainTeardown()
	os.Exit(rc)
}
