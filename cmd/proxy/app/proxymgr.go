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
	"os"
	"strconv"
	"strings"
	"syscall"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/config"
	"juno/cmd/proxy/stats"
	"juno/pkg/cmd"
	"juno/pkg/initmgr"
	"juno/pkg/logging"
	"juno/pkg/logging/cal"
	"juno/pkg/sec"
	"juno/pkg/service"
	"juno/pkg/util"
)

const (
	kDefaultNumChild = 1
)

var (
	kDefaultLogLevel = "info"
)

type (
	CmdProxyCommon struct {
		cmd.Command
		optConfigFile string
		optLogLevel   string
	}

	Manager struct {
		CmdProxyCommon
		optNumChildren     uint
		optListenAddresses util.StringListFlags
		optHttpMonAddr     string
		cmdArgs            []string
	}
)

func (c *CmdProxyCommon) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.StringOption(&c.optConfigFile, "c|config", "", "specify toml config file")
	c.StringOption(&c.optLogLevel, "log-level", kDefaultLogLevel, "specify log level")
}

func (c *CmdProxyCommon) Parse(args []string) (err error) {
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
	c.CmdProxyCommon.Init(name, desc)
	c.UintOption(&c.optNumChildren, "n|num-children", kDefaultNumChild, "specify the number of worker process(es)")
	c.FlagSet.Var(&c.optListenAddresses, "listen", "specify listening address")
	c.StringOption(&c.optHttpMonAddr, "mon-addr|monitoring-address", "", "specify the http monitoring address. \n\toverride HttpMonAddr in config file")
}

func (c *Manager) Parse(args []string) (err error) {
	if err = c.CmdProxyCommon.Parse(args); err != nil {
		return
	}
	c.cmdArgs = args
	return
}

func (c *Manager) Exec() {
	appName := "[proxy m] "

	initmgr.Register(config.Initializer, c.optConfigFile)
	initmgr.Init() //initalize config first as others depend on it

	cfg := &config.Conf

	pidFile := cfg.PidFileName

	if data, err := os.ReadFile(pidFile); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
			if process, err := os.FindProcess(pid); err == nil {
				if err := process.Signal(syscall.Signal(0)); err == nil {
					glog.Exitf("process pid: %d in %s is still running\n", pid, pidFile)
					///TODO check if it is proxy process
				}
			}
		}
	}
	os.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644)
	defer os.Remove(pidFile)

	if len(cfg.LogLevel) == 0 || c.optLogLevel != kDefaultLogLevel {
		cfg.LogLevel = c.optLogLevel
	}
	if cfg.NumChildren == 0 || c.optNumChildren != kDefaultNumChild {
		cfg.NumChildren = int(c.optNumChildren)
	}
	if len(c.optHttpMonAddr) != 0 {
		cfg.HttpMonAddr = c.optHttpMonAddr
	}

	if !strings.Contains(cfg.HttpMonAddr, ":") {
		cfg.HttpMonAddr = ":" + cfg.HttpMonAddr
	}
	if len(c.optListenAddresses) != 0 {
		cfg.SetListeners(c.optListenAddresses)
	}

	initmgr.RegisterWithFuncs(glog.Initialize, glog.Finalize, cfg.LogLevel, appName)

	initmgr.RegisterWithFuncs(cal.Initialize, nil, &cfg.CAL, false)
	initmgr.RegisterWithFuncs(stats.Initialize, stats.Finalize, stats.KTypeManager)

	initmgr.Register(sec.Initializer, &cfg.Sec, cfg.GetSecFlag(), true)

	initmgr.Init()

	logging.LogManagerStart()
	defer logging.LogManagerExit()

	servermgr := service.NewServerManager(int(cfg.NumChildren), os.Args[0], c.cmdArgs,
		cfg.Config, cfg.HttpMonAddr, cfg.CloudEnabled)
	servermgr.Run()
}
