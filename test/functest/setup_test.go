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
package functest

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"

	"juno/third_party/forked/golang/glog"

	"github.com/BurntSushi/toml"

	"juno/cmd/proxy/config"
	"juno/pkg/client"
	"juno/pkg/cluster"
	"juno/pkg/etcd"
	"juno/pkg/io"
	"juno/pkg/logging/cal"
	"juno/pkg/sec"
	"juno/pkg/util"
	"juno/test/testutil/server"
)

var testConfig = server.ClusterConfig{
	ProxyAddress: io.ServiceEndpoint{
		Addr:       "127.0.0.1:26969",
		SSLEnabled: false,
	},
	ProxyConfig: &config.Conf,
	Sec:         sec.DefaultConfig,
}

var ProxyAddr = testConfig.ProxyAddress

var (
	proxyNS             string
	cfg                 client.Config
	cfgShare            client.Config
        TestCluster         *server.Cluster
	proxyClient         client.IClient
	diffNSClient        client.IClient
	diffAppClient       client.IClient
	diffNSDiffAppClient client.IClient
	clientArrays        [6]client.IClient
)

var (
	defaultClientConfig = client.Config{
		DefaultTimeToLive: 1800,
		ConnectTimeout:    util.Duration{4000 * time.Millisecond},
		ReadTimeout:       util.Duration{1500 * time.Millisecond},
		WriteTimeout:      util.Duration{1500 * time.Millisecond},
		RequestTimeout:    util.Duration{3000 * time.Millisecond},
	}
)

func setup() {

	client.SetDefaultTimeToLive(defaultClientConfig.DefaultTimeToLive)
	client.SetDefaultTimeout(defaultClientConfig.ConnectTimeout.Duration,
		defaultClientConfig.ReadTimeout.Duration,
		defaultClientConfig.WriteTimeout.Duration,
		defaultClientConfig.RequestTimeout.Duration,
		defaultClientConfig.ConnRecycleTimeout.Duration)
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

	index := strings.LastIndex(testConfig.ProxyAddress.Addr, ":")
	if index < 0 {
		fmt.Println("Invalid proxy address: ", testConfig.ProxyAddress)
		os.Exit(-1)
	}

        TestCluster = server.NewClusterWithConfig(&testConfig)

	testConfig.ProxyConfig.DefaultTimeToLive = defaultClientConfig.DefaultTimeToLive
	proxyNS = "NS1"
	cfg := defaultClientConfig
	cfg.Server = testConfig.ProxyAddress
	cfg.Namespace = proxyNS
	cfg.Appname = "APP1"
	var err error

	if proxyClient, err = client.New(cfg); err != nil {
		glog.Exitf("proxyClient create in set up is null, fail")
	}
	cfgShare = cfg
	cfgShare.Namespace = "NS2"
	cfgShare.Appname = "APP1"
	if diffNSClient, err = client.New(cfgShare); err != nil {
		glog.Exitf("diffNSClient create in set up is null, fail")
	}
	cfgShare.Namespace = proxyNS
	cfgShare.Appname = "APP2"
	if diffAppClient, err = client.New(cfgShare); err != nil {
		glog.Exitf("diffAppClient create in set up is null, fail")
	}
	cfgShare.Appname = "APP3"
	cfgShare.Namespace = "NS3"
	if diffNSDiffAppClient, err = client.New(cfgShare); err != nil {
		glog.Exitf("diffNSDiffAppClient create in set up is null, fail")
	}

	clientArrays = [6]client.IClient{proxyClient, diffNSClient, diffAppClient, diffNSDiffAppClient}
}

func TestMain(m *testing.M) {
	var (
		configFile  string
		logLevel    string
		longevity   string
		httpEnabled bool
	)
	flag.StringVar(&logLevel, "log_level", "", "specify log level")
	flag.StringVar(&configFile, "config", "", "specify config file")
	flag.StringVar(&longevity, "long", "false", "specify longevity test or not")
	flag.BoolVar(&httpEnabled, "http", true, "enable http")
	flag.Parse()
	if len(configFile) == 0 {
		printUsage()
		os.Exit(-1)
	}
	if httpEnabled {
		go func() {
			http.ListenAndServe(":6060", nil)
		}()
	}

	testConfig.LogLevel = "warning"
	testConfig.ProxyConfig.Outbound.ReconnectIntervalBase = 50

	if _, err := toml.DecodeFile(configFile, &testConfig); err != nil {
		fmt.Printf("fail to read %s. error: %s", configFile, err)
		os.Exit(-1)
	}
	if logLevel != "" {
		testConfig.LogLevel = logLevel
	}
	glog.InitLogging(testConfig.LogLevel, " [ft] ")
	if testConfig.CAL.Enabled {
		cal.InitWithConfig(&testConfig.CAL)
	}

	sec.Initialize(&testConfig.Sec, sec.KFlagClientTlsEnabled|sec.KFlagEncryptionEnabled)

	ProxyAddr = testConfig.ProxyAddress
	os.Unsetenv("JUNO_PIN")

	var chWatch chan int
	clusterInfo := &cluster.ClusterInfo[0]
	glog.Info("preCluster info: ", clusterInfo)
	if testConfig.ProxyConfig.EtcdEnabled {
		chWatch = etcd.WatchForProxy()
		etcd.Connect(&testConfig.ProxyConfig.Etcd, testConfig.ProxyConfig.ClusterName)
		rw := etcd.GetClsReadWriter()
		if rw == nil {
			glog.Exitf("no etcd setup")
		}
		clusterInfo.Read(rw)
	} else {
		clusterInfo.PopulateFromConfig()
	}
	cluster.Initialize(&cluster.ClusterInfo[0], &testConfig.ProxyConfig.Outbound, chWatch, etcd.GetClsReadWriter())
	glog.Info("postCluster info: ", clusterInfo)

	setup()
	rc := m.Run()
	os.Exit(rc)
}

func printUsage() {
	flag.PrintDefaults()
	fmt.Print("\n\n\n")
	fmt.Println("**********************************************************************")
	fmt.Println("*  REQUIRE a toml file specifying the test framework configuration.  *")
	fmt.Println("*  A sample, config.toml.sample, can be found in this directroy for  *")
	fmt.Println("*  your reference.                                                   *")
	fmt.Println("**********************************************************************")
}

func waitForRepairToComplete() {
	proxyClient.Get([]byte("aKey"))
	proxyClient.Get([]byte("aKey"))
	proxyClient.Get([]byte("aKey"))
}
