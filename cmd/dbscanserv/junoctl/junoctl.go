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

	"github.com/paypal/junodb/cmd/dbscanserv/app"
	"github.com/paypal/junodb/pkg/version"
	"github.com/paypal/junodb/third_party/forked/golang/glog"
)

func main() {

	var (
		cfgFile     = "./config.toml"
		cmd         string
		zoneid      int
		nsFile      string
		showVersion bool
	)

	glog.InitLogging("info", "[junoctl] ")
	defer glog.Finalize()

	flag.StringVar(&cmd, "cmd", "", "specify command.")
	flag.IntVar(&zoneid, "start", 0, "specify starting zone for ns_delete.")
	flag.StringVar(&nsFile, "f", "", "specify ns file for ns_delete.")
	flag.BoolVar(&showVersion, "version", false, "display version info")

	flag.Parse()
	if showVersion {
		version.PrintVersionInfo()
		if len(cmd) == 0 {
			return
		}
	}

	if len(cmd) == 0 {
		printUsage()
		return
	}

	if cmd != "delete_ns" {
		glog.Exitf("[ERROR] Invalid cmd: %s.", cmd)
		return
	}

	cmdLine := app.NewCmdLine2(cfgFile, cmd, zoneid, nsFile)
	cmdLine.HandleCommand()
}

func printUsage() {
	progName := filepath.Base(os.Args[0])
	fmt.Printf("Usage:        ./%s -cmd delete_ns -f <file>\n",
		progName)
}
