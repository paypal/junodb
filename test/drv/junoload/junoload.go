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
	"bytes"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/BurntSushi/toml"

	"github.com/paypal/junodb/pkg/client"
	"github.com/paypal/junodb/pkg/cmd"
	"github.com/paypal/junodb/pkg/logging/cal"
	"github.com/paypal/junodb/pkg/sec"
	"github.com/paypal/junodb/pkg/version"
)

type (
	SyncTestDriver struct {
		cmd.Command

		cmdOpts CmdOptions
		config  Config

		reqSequence RequestSequence
		stats       Statistics
		movingStats Statistics
		tmStart     time.Time

		client    client.IClient
		validKeys bool
		randgen   *RandomGen
	}
	CmdOptions struct {
		cfgFile string

		server          string
		requestPattern  string
		sslEnabled      bool
		numExecutor     int
		payloadLen      int
		numReqPerSecond int
		runningTime     int
		statOutputRate  int
		timeToLive      int
		httpMonAddr     string
		version         bool
		numKeys         int
		dbpath          string
		logLevel        string
		isVariable      bool
		disableGetTTL   bool
	}
)

var (
	td                     = SyncTestDriver{}
	kDefaultServerAddr     = "127.0.0.1:8080"
	kDefaultRequestPattern = "C:1,G:1,U:1,S:1,D:1"
)

const (
	kDefaultPayloadLength    = 2048
	kDefaultRecordTimeToLive = 1800
	kDefaultNumReqPerSecond  = 1000
	kDefaultNumExecutor      = 1
	kDefaultRunningTime      = 100
	kDefaultStatOutputRate   = 10
	kDefaultSSLEanbled       = false
)

func (d *SyncTestDriver) setDefaultConfig() {
	d.config.SetDefault()
	d.config.Sec = sec.DefaultConfig
	d.config.Cal.Default()
	d.config.Cal.Poolname = "junoload"
	d.config.Cal.Enabled = false

	d.config.Server.Addr = kDefaultServerAddr
	d.config.Server.SSLEnabled = kDefaultSSLEanbled
	d.config.RequestPattern = kDefaultRequestPattern
	d.config.NumExecutor = kDefaultNumExecutor
	d.config.PayloadLen = kDefaultPayloadLength
	d.config.NumReqPerSecond = kDefaultNumReqPerSecond
	d.config.TimeToLive = kDefaultRecordTimeToLive
	d.config.RunningTime = kDefaultRunningTime
	d.config.StatOutputRate = kDefaultStatOutputRate
	d.config.isVariable = false
	d.config.disableGetTTL = false
}

func (d *SyncTestDriver) Init(name string, desc string) {
	d.Command.Init(name, desc)
	d.StringOption(&d.cmdOpts.server, "s|server", kDefaultServerAddr, "specify proxy address")
	d.StringOption(&d.cmdOpts.cfgFile, "c|config", "", "specify toml configuration file name")
	d.StringOption(&d.cmdOpts.requestPattern, "p|request-pattern", kDefaultRequestPattern, `specify request pattern, a sequence of requests to be
	invoked in format
	  <Req>:<num>[{,<Req>:<num>}]
	Supported type of Requests:
	  C    CREATE
	  G    GET
	  S    SET
	  U    UPDATE
	  D    DESTROY
	`)
	d.BoolOption(&d.cmdOpts.isVariable, "var-load|variable-load", false, "specify if you wants to vary the payload length, throughput and ttl throught the test")
	d.BoolOption(&d.cmdOpts.sslEnabled, "ssl", kDefaultSSLEanbled, "specify if enabling SSL")
	d.IntOption(&d.cmdOpts.numExecutor, "n|num-executor", kDefaultNumExecutor, "specify the number of executors to be running in parallel")
	d.IntOption(&d.cmdOpts.payloadLen, "l|payload-length", kDefaultPayloadLength, "specify payload length")
	d.IntOption(&d.cmdOpts.numReqPerSecond, "f|num-req-per-second", kDefaultNumReqPerSecond, "specify expected throughput (number of requests per second)")
	d.IntOption(&d.cmdOpts.runningTime, "t|running-time", kDefaultRunningTime, "specify driver's running time in second")
	d.IntOption(&d.cmdOpts.timeToLive, "ttl|record-time-to-live", kDefaultRecordTimeToLive, "specify record TTL in second")
	d.IntOption(&d.cmdOpts.statOutputRate, "o|stat-output-rate", kDefaultStatOutputRate, "specify how often to output statistic information in second\n\tfor the period of time.")
	d.StringOption(&d.cmdOpts.httpMonAddr, "mon-addr|monitoring-address", "", "specify the http monitoring address. \n\toverride HttpMonAddr in config file")
	d.BoolOption(&d.cmdOpts.version, "version", false, "display version information.")
	d.StringOption(&d.cmdOpts.dbpath, "dbpath", "", "to display rocksdb stats")
	d.StringOption(&d.cmdOpts.logLevel, "log-level", "info", "specify log level")
	d.BoolOption(&d.cmdOpts.disableGetTTL, "disableGetTTL", false, "not use random ttl for get operation")

	t := &SyncTestDriver{}
	t.setDefaultConfig()

	cfg := t.config
	cfg.Appname = "junoload"
	cfg.Namespace = "ns"

	var buf bytes.Buffer
	encoder := toml.NewEncoder(&buf)
	encoder.Encode(&cfg)
	d.AddDetails("\tConfig properties and default values if not defined:\n" +
		"\t\t" + strings.Replace(buf.String(), "\n", "\n\t\t", -1))

	d.AddExample(name+" -s 127.0.0.1:8080",
		"\trun the driver against server listening on 127.0.0.1:8080 with default \n\toptions")
	d.AddExample(name+" -s 127.0.0.1:8080 -ssl",
		"\trun the driver with SSL")
	d.AddExample(name+" -c config.toml", "\trun the driver with options specified in config.toml")

}

func (d *SyncTestDriver) Parse(args []string) (err error) {
	if err = d.FlagSet.Parse(args); err != nil {
		return
	}
	d.setDefaultConfig()

	if len(d.cmdOpts.cfgFile) != 0 {
		if _, err := toml.DecodeFile(d.cmdOpts.cfgFile, &d.config); err != nil {
			glog.Exitf("failed to local config file %s. %s", d.cmdOpts.cfgFile, err)
		}
	}

	if d.cmdOpts.server != kDefaultServerAddr {
		d.config.Server.Addr = d.cmdOpts.server
	}
	if d.cmdOpts.sslEnabled != kDefaultSSLEanbled {
		d.config.Server.SSLEnabled = d.cmdOpts.sslEnabled
	}
	if len(d.config.Appname) == 0 {
		d.config.Appname = "junoload"
	}
	if len(d.config.Namespace) == 0 {
		d.config.Namespace = "junoTest"
	}

	if d.config.Server.SSLEnabled {
		if err = sec.Initialize(&d.config.Sec, sec.KFlagClientTlsEnabled); err != nil {
			fmt.Println(err)
			return
		}
	}

	if d.cmdOpts.disableGetTTL {
		d.config.disableGetTTL = d.cmdOpts.disableGetTTL
	}
	if d.cmdOpts.isVariable {
		d.config.isVariable = d.cmdOpts.isVariable
	}
	if d.cmdOpts.requestPattern != kDefaultRequestPattern {
		d.config.RequestPattern = d.cmdOpts.requestPattern
	}
	if d.cmdOpts.numExecutor != kDefaultNumExecutor {
		d.config.NumExecutor = d.cmdOpts.numExecutor
	}
	if d.cmdOpts.payloadLen != kDefaultPayloadLength {
		d.config.PayloadLen = d.cmdOpts.payloadLen
	}
	if d.cmdOpts.numReqPerSecond != kDefaultNumReqPerSecond {
		d.config.NumReqPerSecond = d.cmdOpts.numReqPerSecond
	}
	if d.cmdOpts.timeToLive != kDefaultRecordTimeToLive {
		d.config.TimeToLive = d.cmdOpts.timeToLive
	}
	if d.cmdOpts.runningTime != kDefaultRunningTime {
		d.config.RunningTime = d.cmdOpts.runningTime
	}
	if d.cmdOpts.numKeys > 0 { // number of preloaded keys
		if d.cmdOpts.numKeys < 100 {
			d.cmdOpts.numKeys = 100
		}
		glog.Infof("client_cfg=%v", d.config.Config)
	}
	if d.cmdOpts.statOutputRate != kDefaultStatOutputRate {
		d.config.StatOutputRate = d.cmdOpts.statOutputRate
	}
	if d.cmdOpts.httpMonAddr != "" {
		d.config.HttpMonAddr = d.cmdOpts.httpMonAddr
	}
	if d.config.HttpMonAddr != "" && !strings.Contains(d.config.HttpMonAddr, ":") {
		d.config.HttpMonAddr = ":" + d.config.HttpMonAddr
	}

	d.config.Cal.Default()

	if d.config.Cal.Enabled {
		cal.InitWithConfig(&d.config.Cal)
	}

	glog.InitLogging(d.cmdOpts.logLevel, " [junoload] ")
	return
}

func (d *SyncTestDriver) PrintTestSetup() {
	fmt.Println(`
Test Configuration:
--------------------------------------------------------------------`)
	fmt.Printf("To invoke the following request(s) in sequence repeatedly with %d test executor(s)\n", d.config.NumExecutor)
	d.reqSequence.PrettyPrint(os.Stdout)

	if d.cmdOpts.isVariable {
		fmt.Printf("at variable rate of requests with mean of (%d) request(s) per second for one test executor\n", d.config.NumReqPerSecond)
		fmt.Printf("for about (%d) second(s).\n\n", d.config.RunningTime)
		fmt.Printf("The payload length is also variable with mean size of (%d) byte(s). \n\n", d.config.PayloadLen)
	} else {
		fmt.Printf("at the rate of no more than (%d) request(s) per second for one test executor\n", d.config.NumReqPerSecond)
		fmt.Printf("for about (%d) second(s).\n\n", d.config.RunningTime)
		fmt.Printf("The payload length is fixed at (%d) byte(s). \n\n", d.config.PayloadLen)
	}
	fmt.Printf("Statistic information will be printed for every (%d) second(s).\n\n\n\n", d.config.StatOutputRate)

}

func (d *SyncTestDriver) Prepare() bool {

	// Create random bytes payload thrice the length of the requested length
	payload := make([]byte, d.config.PayloadLen*3)
	rand.Read(payload)

	// Create random number object for creating random lengths
	seed := rand.NewSource(time.Now().UnixNano())
	randNum := rand.New(seed)

	d.randgen = &RandomGen{
		payload:       payload,
		randNum:       randNum,
		payloadLen:    d.config.PayloadLen,
		ttl:           uint32(d.config.TimeToLive),
		tp:            d.config.NumReqPerSecond,
		isVariable:    d.config.isVariable,
		disableGetTTL: d.config.disableGetTTL}

	d.Validate()
	d.reqSequence.initFromPattern(d.config.RequestPattern)
	d.PrintTestSetup()

	// Display rocksdb stats
	if d.cmdOpts.dbpath != "" {
		fmt.Printf("rocksdb stats:\n%s\n", "--------------------------------------------------------------------")
		PrintDbStats(d.cmdOpts.dbpath)
	}

	return true
}

func (d *SyncTestDriver) Exec() {

	//	d.reqSequence.PrettyPrint(os.Stdout)
	var wg sync.WaitGroup
	chDone := make(chan bool)

	if d.config.NumExecutor > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			timer := time.NewTimer(time.Duration(d.config.RunningTime) * time.Second)
			ticker := time.NewTicker(time.Duration(d.config.StatOutputRate) * time.Second)
		loop:
			for {
				select {
				case <-timer.C:
					timer.Stop()
					ticker.Stop()
					close(chDone)
					break loop
				case <-chDone:
					break loop
				case <-ticker.C:
					d.movingStats.PrettyPrint(os.Stdout)
					d.movingStats.Reset()
				}
			}

		}()
	} else {
		glog.Errorf("number of executor specified is zero")
		return
	}
	d.tmStart = time.Now()
	d.stats.Init()
	d.movingStats.Init()
	for i := 0; i < d.config.NumExecutor; i++ {
		size := d.cmdOpts.numKeys / 2
		num := size / d.config.NumExecutor
		offGet := i*num + size

		if size > MaxDeletes {
			size = MaxDeletes
		}
		offDel := i * (size / d.config.NumExecutor)
		cli, err := client.New(d.config.Config)
		if err != nil {
			glog.Error(err)
			return
		}
		eng := &TestEngine{
			rdgen: d.randgen,
			recStore: RecordStore{
				numKeys:   num,
				offsetDel: offDel,
				offsetGet: offGet},
			reqSequence: d.reqSequence,
			//			chDone:      chDone,
			client:          cli,
			stats:           &d.stats,
			movingStats:     &d.movingStats,
			numReqPerSecond: d.config.NumReqPerSecond,
		}
		eng.Init()
		wg.Add(1)
		go eng.Run(&wg, chDone)
	}
	if d.config.HttpMonAddr != "" {
		go func() {
			if err := http.ListenAndServe(d.config.HttpMonAddr, nil); err != nil {
				glog.Warningf("fail to serve HTTP on %s, err: %s", d.config.HttpMonAddr, err)
			}
		}()
	}
	wg.Wait()
}

func main() {
	td.Init("junoload", "test driver")
	if err := td.Parse(os.Args[1:]); err != nil {
		glog.Exitf("failed with %s", err.Error())
	}

	if td.cmdOpts.version {
		version.PrintVersionInfo()
		return
	}

	if td.Prepare() &&
		td.cmdOpts.runningTime > 0 {
		td.Exec()
		fmt.Println("\n\nFINAL")
		td.stats.PrettyPrint(os.Stdout)
	}
	PrintDbStats(td.cmdOpts.dbpath)
}
