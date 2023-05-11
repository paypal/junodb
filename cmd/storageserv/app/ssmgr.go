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

package app

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/config"
	"juno/pkg/cmd"
	"juno/pkg/initmgr"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/net/netutil"
)

const (
	kDefaultNumChild = 1
)

var (
	kDefaultLogLevel = "info"
)

type (
	CmdStorageCommon struct {
		cmd.Command
		optConfigFile string
		optLogLevel   string
	}

	Manager struct {
		CmdStorageCommon
		optNumChildren uint
		optIpAddress   string
		cmdArgs        []string
	}
)

func (c *CmdStorageCommon) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.StringOption(&c.optConfigFile, "c|config", "", "specify toml config file")
	c.StringOption(&c.optLogLevel, "log-level", kDefaultLogLevel, "specify log level")
}

func (c *CmdStorageCommon) Parse(args []string) (err error) {
	if err = c.Command.Parse(args); err != nil {
		return
	}
	if len(c.optConfigFile) == 0 {
		fmt.Fprintf(os.Stderr, "\n\n*** missing config option ***\n\n")
		c.FlagSet.Usage()
		os.Exit(-1)
	}

	if _, err := os.Stat(c.optConfigFile); errors.Is(err, fs.ErrNotExist) {
		fmt.Fprintf(os.Stderr, "\n\n***  config file \"%s\" not found ***\n\n", c.optConfigFile)
		os.Exit(-1)
	}

	return
}

func (c *Manager) Init(name string, desc string) {
	c.CmdStorageCommon.Init(name, desc)
	c.UintOption(&c.optNumChildren, "n|num-children", kDefaultNumChild, "specify the number of worker process(es)")
	c.StringOption(&c.optIpAddress, "ipaddress", "", "ip address of the host, this option is only for testing")
}

func (c *Manager) Parse(args []string) (err error) {
	if err = c.CmdStorageCommon.Parse(args); err != nil {
		return
	}
	c.cmdArgs = args
	return
}

///TODO refactoring
func (c *Manager) Exec() {

	initmgr.Register(config.Initializer, c.optConfigFile)
	initmgr.Init() //initalize config first as others depend on it

	cfg := config.ServerConfig()
	initmgr.RegisterWithFuncs(glog.Initialize, glog.Finalize, cfg.LogLevel, "[ServerMgr] ")
	initmgr.RegisterWithFuncs(cal.Initialize, nil, &cfg.Cal, false)

	var connInfo []ConnectInfo
	if c.optIpAddress == "" {
		for row := 0; row < len(cfg.ClusterInfo.ConnInfo); row++ {
			for col := 0; col < len(cfg.ClusterInfo.ConnInfo[row]); col++ {
				ipport := cfg.ClusterInfo.ConnInfo[row][col]
				if ip, _, err := net.SplitHostPort(ipport); err == nil {
					if netutil.IsLocalAddress(ip) {
						connInfo = append(connInfo, ConnectInfo{Listener: ipport, ZoneId: row, MachineIndex: col})
					} else {
						// for K8s storage pod initialization check in GKE
						k8sPodName, ok := os.LookupEnv("POD_NAME")
						if ok {
							k8sPodFqdn := k8sPodName
							k8sPodDomain, ok := os.LookupEnv("POD_DOMAIN")
							if ok {
								k8sPodFqdn = k8sPodName + "." + k8sPodDomain
							}
							if ip == k8sPodFqdn {
								connInfo = append(connInfo, ConnectInfo{Listener: ipport, ZoneId: row, MachineIndex: col})
							} else {
								glog.Errorf("[K8s] ConnInfo Ip of pod (%s) doesn't match pod fqdn (%s)", ip, k8sPodFqdn)
							}
						}
					}
				} else {
					glog.Errorf("wrong connect info string %s [%d][%d]", ipport, row, col)
				}

			}
		}
	} else {
		for row := 0; row < len(cfg.ClusterInfo.ConnInfo); row++ {
			for col := 0; col < len(cfg.ClusterInfo.ConnInfo[row]); col++ {
				ipport := cfg.ClusterInfo.ConnInfo[row][col]
				if ip, _, err := net.SplitHostPort(ipport); err == nil {
					if ip == c.optIpAddress && netutil.IsLocalAddress(ip) {
						connInfo = append(connInfo, ConnectInfo{Listener: ipport, ZoneId: row, MachineIndex: col})
					}
				} else {
					glog.Errorf("wrong connect info string %s [%d][%d]", ipport, row, col)
				}
			}
		}
	}
	if len(connInfo) == 0 {
		glog.Errorf("No cluster info found for this host, exit")
		return
	}

	initmgr.Init()

	logging.LogManagerStart()
	defer logging.LogManagerExit()

	// Parent process
	var cmdArgs []string

	if c.optConfigFile != "" {
		cmdArgs = append(cmdArgs, fmt.Sprintf("-config=%s", c.optConfigFile))
	}
	if c.optLogLevel != kDefaultLogLevel {
		cmdArgs = append(cmdArgs, fmt.Sprintf("-log-level=%s", c.optLogLevel))
	}
	servermgr := NewServerManager(len(connInfo), cfg.PidFileName, os.Args[0], cmdArgs, connInfo,
		cfg.HttpMonAddr, int(cfg.DbScan.ListenPort), cfg.CloudEnabled)
	servermgr.Run()
}
