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

/*
Juno Proxy Server
*/
package app

import (
	"errors"
	"fmt"
	"io/fs"
	_ "net/http/pprof"
	"os"
	"os/exec"
	"path/filepath"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/config"
	"juno/pkg/cmd"
	"juno/pkg/initmgr"
	"juno/pkg/sec"
	"juno/pkg/version"
)

func Main() {
	defer initmgr.Finalize()

	var (
		cmdManager          Manager
		cmdWorker           Worker
		cmdMonitoringWorker MonitoringWorker
	)
	cmdManager.Init("manager", "start as proxy server manager")
	cmdWorker.Init("worker", "start as proxy worker")
	cmdMonitoringWorker.Init("monitor", "start as proxy monitoring worker")
	cmd.Register(&cmdManager)
	cmd.Register(&cmdWorker)
	cmd.Register(&cmdMonitoringWorker)

	if command, args := cmd.ParseCommandLine(); command != nil {
		if err := command.Parse(args); err == nil {
			command.Exec()
		} else {
			fmt.Printf("* command '%s' failed. %s\n", command.GetName(), err)
		}
	} else {
		execDefault()
	}
}

func execDefault() {
	progName := filepath.Base(os.Args[0])
	var option cmd.Option
	var displayVersion bool
	var configFilename string
	option.BoolOption(&displayVersion, "version", false, "display version info")
	option.StringOption(&configFilename, "c|config", "", "specify toml config file")

	option.Usage = func() {
		fmt.Printf(`
NAME
  %s - Juno proxy

USAGE
  %s <-version>
  %s <-c|-config=<config file>
  %s <options> <command> 
  %s <command> <options> 
`, progName, progName, progName, progName, progName)
		cmd.WriteCommand(os.Stdout)
	}
	if err := option.Parse(os.Args[1:]); err == nil {
		if displayVersion {
			version.PrintVersionInfo()
			if configFilename == "" {
				return
			}
		}
		if configFilename == "" {
			glog.Exitf("\n\n*** missing config option ***\n\n")
		}
		if _, err := os.Stat(configFilename); errors.Is(err, fs.ErrNotExist) {
			glog.Exitf("\n\n***  config file \"%s\" not found ***\n\n", configFilename)
		}
		appName := "[" + progName + "] "

		initmgr.Register(config.Initializer, configFilename)
		initmgr.Init() //initalize config first as others depend on it

		cfg := &config.Conf

		initmgr.RegisterWithFuncs(glog.Initialize, glog.Finalize, cfg.LogLevel, appName)

		if cfg.GetSecFlag() != 0 {
			initmgr.Register(sec.Initializer, &cfg.Sec, cfg.GetSecFlag(), true)
		}

		initmgr.Init()

		cmd := exec.Command(os.Args[0], "manager", fmt.Sprintf("-config=%s", configFilename))
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		err := cmd.Start()
		if err != nil {
			glog.Exitf("Failed to launch Manager process, error: %s\n", err.Error())
		}
	}
}
