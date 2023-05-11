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
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/version"

	"juno/cmd/clustermgr/cmd"
)

var (
	flagConfig      string
	flagConfigNew   string
	flagDryrun      bool
	flagVerbose     bool
	flagCmd         string
	flagType        string
	flagZoneid      int
	flagSkipZone    int
	flagMaxFailures int
	flagMinWait     int
	flagCache       string
	flagVersion     bool
	flagRateLimit   int
	flagAMarkdown   bool
)

func main() {
	glog.InitLogging("debug", "clustermgr")
	defer glog.Finalize()

	flag.StringVar(&flagConfig, "config", "", "configfile")
	flag.StringVar(&flagConfigNew, "new_config", "", "new configfile")
	flag.BoolVar(&flagDryrun, "dryrun", false, "dry run -- do not save to etcd")
	flag.BoolVar(&flagVerbose, "verbose", false, "verbose -- print more info")
	flag.StringVar(&flagCmd, "cmd", "", "command -- store, redist")
	flag.StringVar(&flagType, "type", "cluster_info", "type -- cluster_info, auto, abort")
	flag.IntVar(&flagZoneid, "zone", -1, "specify zone id")
	flag.IntVar(&flagSkipZone, "skipzone", -1, "specify zone id to skip")
	flag.IntVar(&flagMaxFailures, "max_failures", 0, "Max number of snapshot failures allowed")
	flag.IntVar(&flagMinWait, "min_wait", 10, "A snapshot not completed within min_wait is considered as a failure.")
	flag.StringVar(&flagCache, "cache", "", "cache name")
	flag.BoolVar(&flagVersion, "version", false, "display version information.")
	flag.IntVar(&flagRateLimit, "ratelimit", 0, "rate limit for redistribution in KB, 0 means not set")
	flag.BoolVar(&flagAMarkdown, "automarkdown", true, "mark down during redistribution")

	flag.Parse()

	if flagVersion {
		version.PrintVersionInfo()
		return
	}

	if flagCmd == "redist" {
		flagConfig = flagConfigNew
	}

	if len(flagConfig) == 0 {
		flagConfig = defaultConfig(os.Args[0])
	}
	glog.Infof("config=%s cmd=%s type=%s zone=%d", flagConfig, flagCmd, flagType, flagZoneid)

	if flagCmd == "store" {
		if flagType == "cluster_info" {
			cmd.StoreClusterInfo(flagConfig, flagDryrun, flagVerbose)
		} else {
			printUsage()
			return
		}
	} else if flagCmd == "load" {
		if len(flagConfig) == 0 {
			printUsage()
			return
		}

		cmd.LoadClusterInfo(flagConfig)

	} else if flagCmd == "swaphost" {
		if len(flagConfig) == 0 {
			printUsage()
			return
		}
		cmd.SwapHost(flagConfig)

	} else if flagCmd == "redist" {

		switch flagType {
		case "auto":
			cmd.RedistAuto(flagConfig, flagZoneid, flagSkipZone, flagDryrun, flagMaxFailures, flagMinWait, false, flagRateLimit, flagAMarkdown)
		case "autonocommit":
			cmd.RedistAutoNoCommit(flagConfig, flagZoneid, flagSkipZone, flagDryrun, flagMaxFailures, flagMinWait, false, flagRateLimit, flagAMarkdown)
		case "abort":
			cmd.RedistAbort(flagConfig, flagDryrun)
		case "prepare":
			cmd.RedistPrepare(flagConfig, flagZoneid, flagDryrun, false /*swaphost*/)
		case "start_src":
			cmd.RedistStart(flagConfig, true, flagZoneid, true, flagDryrun, flagRateLimit, flagAMarkdown)
		case "start_tgt":
			cmd.RedistStart(flagConfig, true, flagZoneid, false, flagDryrun, flagRateLimit, false)
		case "commit":
			cmd.RedistCommit(flagConfig, flagZoneid, flagDryrun, true, flagMaxFailures, flagAMarkdown)
		case "test": // for test only
			cmd.RedistAuto(flagConfig, flagZoneid, flagSkipZone, flagDryrun, flagMaxFailures, flagMinWait, true, flagRateLimit, flagAMarkdown)
		case "resume":
			cmd.RedistResume(flagConfig, flagZoneid, flagDryrun, flagRateLimit)
		default:
			printUsage()
			return
		}
	} else if flagCmd == "restore" {
		cmd.RestoreCache(flagConfig, flagCache, flagDryrun)
	} else if flagCmd == "zonemarkdown" {
		cmd.ZoneMarkDown(flagConfig, flagType, flagZoneid)
	} else {
		printUsage()
		return
	}
}

func defaultConfig(progName string) (config string) {
	path, err := filepath.Abs(progName)
	if err != nil {
		glog.Exitf("%s", err)
	}

	dirName := filepath.Dir(path)
	config = fmt.Sprintf("%s/config.toml", dirName)

	_, err = os.Stat(config)
	if errors.Is(err, fs.ErrNotExist) {
		glog.Exitf("[ERROR] %s does not exist.", config)
	}

	if err != nil {
		glog.Exitf("%s", err)
	}
	return
}

func printUsage() {
	progName := filepath.Base(os.Args[0])
	fmt.Printf("1) USAGE: ./%s --config [configfile] --cmd [store] --type [cluster_info] --dryrun\n\n", progName)
	fmt.Printf("Upload cluster config to etcd:   ./%s --config config.toml --cmd store --type cluster_info\n", progName)
	fmt.Printf("Dump cluster config to stdout:   ./%s --config config.toml --cmd store --type cluster_info --dryrun\n\n", progName)

	fmt.Printf("\n2) USAGE: ./%s --new_config [configfile] --cmd [redist] --type [auto|abort]\n\n", progName)
	fmt.Printf("redist:                          ./%s --new_config redist.toml --cmd redist --type auto --zone [z] --skipzone [s] --max_failures [n] --min_wait [m]\n", progName)
	fmt.Printf("Abort redist:                    ./%s --new_config redist.toml --cmd redist --type abort\n", progName)

	fmt.Printf("\n3) USAGE: ./%s --new_config [configfile] --cmd [redist] --type [abort|prepare|start|commit] --dryrun\n\n", progName)
	fmt.Printf("Dump redist abort to stdout:     ./%s --new_config redist.toml --cmd redist --type abort --dryrun\n", progName)
	fmt.Printf("Dump redist prepare to stdout:   ./%s --new_config redist.toml --cmd redist --type prepare --dryrun\n", progName)
	fmt.Printf("Dump redist start_tgt to stdout: ./%s --new_config redist.toml --cmd redist --type start_tgt --zone [n] --dryrun\n", progName)
	fmt.Printf("Dump redist start_src to stdout: ./%s --new_config redist.toml --cmd redist --type start_src --zone [n] --dryrun --automarkdown=false\n", progName)
	fmt.Printf("Dump redist commit to stdout:    ./%s --new_config redist.toml --cmd redist --type commit --dryrun\n", progName)
	fmt.Printf("Dump redist resume: ./%s --new_config redist.toml --cmd redist --type resume --zone [n] --ratelimit 10000 (optional, in kb)\n", progName)
	fmt.Printf("Zone markdown:    ./%s --config config.toml --cmd markdown --type set/get/delete --zone [n] (--zone -1 disables markdwon)\n", progName)
}
