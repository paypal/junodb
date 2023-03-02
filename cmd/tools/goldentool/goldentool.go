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
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"juno/cmd/tools/goldendata"
	"juno/third_party/forked/golang/glog"
)

func main() {
	defer glog.Finalize()

	var server string
	var payloadLen int
	var ttl, start, end int
	var numShards int
	var numMShardGroups int
	var flagType string
	var flagOp string
	var flagExcludes string

	flag.StringVar(&server, "s", "127.0.0.1:8080", "proxy addr")
	flag.StringVar(&flagType, "type", "goldenset", "type of dataset - goldenset or redistset")
	flag.StringVar(&flagOp, "op", "insert", "operation type - insert, delete, get")
	flag.IntVar(&payloadLen, "len", 2000, "payload length")
	flag.IntVar(&ttl, "ttl", 36000, "record time to live")
	flag.IntVar(&numShards, "nshards", 1024, "num of shards")
	flag.IntVar(&numMShardGroups, "nmshardgroups", 16, "num of micro shard groups")
	flag.IntVar(&start, "start", 0, "start seed id")
	flag.IntVar(&end, "end", 10000, "end seed id")
	flag.StringVar(&flagExcludes, "exclude", "", "exlude a list of microshards, e.g. 0,4-7")

	flag.Parse()

	var op int = -1
	const (
		opCreate = 0
		opGet    = 1
		opDelete = 2
	)

	if flagOp == "insert" {
		op = opCreate
	} else if flagOp == "delete" {
		op = opDelete
	} else if flagOp == "get" {
		op = opGet
	}

	fmt.Printf("op: %s %s, num shards: %d\n", flagOp, flagType, numShards)
	if op < 0 {
		printUsage()
		return
	}

	var ds gld.IDataSet
	var ns string
	if flagType == "goldenset" {
		ds = gld.NewGoldenSet(numShards, start, payloadLen)
		ns = "gld"
	} else if flagType == "redistset" {
		ds = gld.NewRedistSet(numShards, numMShardGroups, start, end, payloadLen, flagExcludes)
		ns = "redist"
	} else {
		printUsage()
		return
	}

	var cli gld.JunoClient
	cli.Init(server, ns, ttl)

	switch op {
	case opCreate: // create
		ds.Insert(&cli)
		break

	case opGet: // get
		ds.Get(&cli)
		ds.Dump()
		break

	case opDelete: // delete
		ds.Delete(&cli)
		break

	default:
	}

//	ds.Dump()
}

func printUsage() {
	progName := filepath.Base(os.Args[0])
	fmt.Printf("\nExample 1: Use junoserver at <ip:port>.  Create golden data set, which can be accessed by [-get|-delete] in a subsequent command.\n\n")
	fmt.Printf("./%s -s <ip:port> -ttl <ttl> -len <payload-len> -type goldenset -op insert\n", progName)
	fmt.Printf("./%s -s <ip:port> -type goldenset -op get\n", progName)
	fmt.Printf("./%s -s <ip:port> -type goldenset -op delete\n", progName)
	fmt.Printf("./%s -s <ip:port> -ttl <ttl> -len <payload-len> -nshards 1024 -type goldenset -op insert\n", progName)

	fmt.Printf("\nExample 2: Create redistribution data sets of unique random keys, which can be accessed by [-get|-delete] in a subsequent command.\n\n")
	fmt.Printf("./%s -s <ip:port> -ttl <ttl> -len <payload-len> -type redistset -op insert\n", progName)
	fmt.Printf("./%s -s <ip:port> -type redistset -op get\n", progName)
	fmt.Printf("./%s -s <ip:port> -type redistset -op delete\n", progName)
	fmt.Printf("./%s -s <ip:port> -ttl <ttl> -len <payload-len> -nshards 1024 -nmshardgroups 8 -type redistset -op insert -start 0 -end 10000\n", progName)
}
