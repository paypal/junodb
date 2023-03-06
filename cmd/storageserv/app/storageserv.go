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
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"

	"juno/pkg/cmd"
	"juno/pkg/initmgr"
	"juno/pkg/version"
)

func init() {
	var (
		cmdManager          Manager
		cmdWorker           Worker
		cmdMonitoringWorker MonitoringWorker
	)
	cmdManager.Init("manager", "start as storage server manager")
	cmdWorker.Init("worker", "start as storage worker")
	cmdMonitoringWorker.Init("monitor", "start as storage monitoring worker")
	cmd.Register(&cmdManager)
	cmd.Register(&cmdWorker)
	cmd.Register(&cmdMonitoringWorker)
}

func Main() {
	defer initmgr.Finalize()

	var versionFlag bool
	var help bool

	flag.BoolVar(&versionFlag, "version", false, "display version information.")
	flag.BoolVar(&help, "h", false, "help")
	flag.BoolVar(&help, "help", false, "help")
	flag.Parse()

	if versionFlag {
		version.PrintVersionInfo()
		return
	}
	if help {
		printUsage()
	}
	numArgs := len(os.Args)

	if numArgs < 2 {
		fmt.Println("command is required")
		printUsage()
		os.Exit(1)
	}
	indexCommand := 1

	for i := 1; i < numArgs; i++ {
		if strings.HasPrefix(os.Args[i], "-") {
			indexCommand++
		} else {
			break
		}
	}

	if indexCommand < numArgs {
		cmd := cmd.GetCommand(os.Args[indexCommand])
		if cmd != nil {
			if err := cmd.Parse(os.Args[indexCommand+1:]); err == nil {
				cmd.Exec()
			} else {
				fmt.Printf("* command '%s' failed. %s\n", cmd.GetName(), err)
			}
		} else {
			fmt.Printf("command '%s' not specified", os.Args[indexCommand])
			return
		}
	}

}

//TODO may customize this or remove inappalicable glob flags
func printUsage() {
	progName := filepath.Base(os.Args[0])
	fmt.Printf(`
USAGE
  %s <command> <-c|-config>=<config file> [<options>]

`, progName)
	fmt.Printf(`OPTION
  -version
        print version info
  -h
        print usage info
`)
	cmd.PrintUsage()
}
