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
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/dbscanserv/app"
	"juno/cmd/dbscanserv/prime"
	"juno/pkg/version"
)

func parseKeyRange(keyRange string, cmd string) (start int, stop int, skip int) {

	if (cmd == "run" || cmd == "copy_ns") && len(keyRange) == 0 {
		glog.Exitf("[ERROR] Parameter -r is required to specify shard range.")
	}

	if keyRange == "all" || len(keyRange) == 0 {
		start = 0
		stop = 1 << 16
		return
	}

	list := strings.Split(keyRange, ",")
	start, _ = strconv.Atoi(list[0])
	skip = 0

	if len(list) < 2 {
		stop = start + 1
	} else {
		stop, _ = strconv.Atoi(list[1])
	}

	if len(list) == 3 {
		skip, _ = strconv.Atoi(list[2])
	}

	if start < 0 || stop < 0 {
		glog.Exitf("[ERROR] Parameter -r cannot be negative.")
	}
	return
}

func main() {

	var (
		cfgFile string

		serverType string
		cmdString  string
		scanRange  string
		ns         string

		zoneid int
		addr   string

		incExpireKeys bool
		priorDate     string
		showVersion   bool
	)

	glog.InitLogging("info", "[dbscanserv] ")
	defer glog.Finalize()

	flag.StringVar(&cfgFile, "c", "", "specify config file.")
	flag.StringVar(&cfgFile, "config", "", "specify config file.")
	flag.StringVar(&serverType, "type", "", "For testing.")
	flag.StringVar(&cmdString, "cmd", "none", "specify command.")

	flag.StringVar(&scanRange, "r", "", "specify shard range: <startid>,<stopid>")
	flag.IntVar(&zoneid, "zone", 0, "zoneid where drivers run.")
	flag.StringVar(&addr, "s", "", "specify ip:port")
	flag.StringVar(&ns, "ns", "", "specify namespaces: <name1>|<name2>")

	flag.BoolVar(&incExpireKeys, "ie", false, "true: Include expired keys.")
	flag.StringVar(&priorDate, "pd", "", "specify prior scan date: YYYY-MM-DD")
	flag.BoolVar(&showVersion, "version", false, "display version info")

	flag.Parse()
	if showVersion {
		version.PrintVersionInfo()
		if len(cfgFile) == 0 {
			return
		}
	}

	start, stop, skip := parseKeyRange(scanRange, cmdString)

	if len(cfgFile) == 0 {
		printUsage()
		return
	}

	if len(ns) > 0 {
		prime.SetExpireBeforeTime(incExpireKeys, true)
	} else if !incExpireKeys {
		prime.SetModTimeEnd()
		prime.SetExpireBeforeTime(false, false)
	}

	switch serverType {
	case "driver":
		if len(addr) > 0 && app.IsValidAddr(addr) {
			prime.SetProxyAddr(addr)
		}
		prime.SetNamespaceNames(ns, incExpireKeys)
		app.Collect(cfgFile, start, stop, skip, true)

	default: // Command line
		list := strings.Split(cmdString, ":")
		cmd := list[0]

		if cmd == "none" {
			app.Collect(cfgFile, start, stop, 0, false)
			return
		}

		if len(priorDate) > 0 {
			prime.AddModTimeBegin(priorDate)
		}

		cmdLine := app.NewCmdLine(cfgFile, cmd, zoneid, addr, start, stop, ns, incExpireKeys)
		cmdLine.HandleCommand()
	}
}

func printUsage() {
	progName := filepath.Base(os.Args[0])
	fmt.Printf("Usage:           ./%s <-c|-config> <cfg_file> [<options>]\n\n",
		progName)
	fmt.Printf("Scan/Repair:     "+
		"./%s -c <cfg_file> -cmd run -r <range> [-s <ip:port>]\n", progName)
	fmt.Printf("Copy namespace:  "+
		"./%s -c <cfg_file> -cmd copy_ns -r <range> -ns <name>\n", progName)
	fmt.Printf("Patch namespace: "+
		"./%s -c <cfg_file> -cmd patch\n", progName)
	fmt.Printf("Check status:    ./%s -c <cfg_file> -cmd status\n", progName)
	fmt.Printf("Ping servers:    ./%s -c <cfg_file> -cmd ping\n", progName)
	fmt.Printf("Stop servers:    ./%s -c <cfg_file> -cmd stop\n", progName)
}
