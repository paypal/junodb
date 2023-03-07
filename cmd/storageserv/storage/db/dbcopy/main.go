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
  
// main.go
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"juno/third_party/forked/golang/glog"
)

func parseKeyRange(key string) (start int, stop int) {

	list := strings.Split(key, ",")
	start, _ = strconv.Atoi(list[0])
	if len(list) < 2 {
		stop = start + 1
	} else {
		stop, _ = strconv.Atoi(list[1])
	}
	return
}

// Return true if nodeid in db name is in range.
func inRange(name string, start int, stop int) bool {

	tokens := strings.Split(name, "-")
	if len(tokens) < 3 {
		return false
	}

	num, err := strconv.Atoi(tokens[1])
	if err == nil && num >= start && num < stop {
		return true
	}

	return false
}

// Return source db paths and target db paths
func getDbSet(dbPath string, tgtPath string, zone, node int, dbRange string) (dbset, tgtset []string) {

	start := 0
	stop := 12

	pattern := fmt.Sprintf("%s/*.db", dbPath)
	if zone >= 0 {
		if node >= 0 {
			pattern = fmt.Sprintf("%s/%d-%d*.db", dbPath, zone, node)
		} else {
			pattern = fmt.Sprintf("%s/%d-*.db", dbPath, zone)
		}
	}
	pathList, _ := filepath.Glob(pattern)

	if node < 0 && dbRange != "" {
		start, stop = parseKeyRange(dbRange)
	}

	for i, path := range pathList {
		if i < start || i >= stop {
			continue
		}
		info, err := os.Stat(path)
		if err != nil {
			glog.Exitf("[ERROR] Unable to access db.  %s", err)
		}

		if !info.IsDir() {
			// Not a directory
			continue
		}

		modTime := info.ModTime()
		if modTime.Year() < 2020 {
			// Skip obsolete db
			continue
		}

		dbset = append(dbset, path)

		if tgtPath == "" {
			tgtset = append(tgtset, "")
			continue
		}

		target := fmt.Sprintf("%s/%s", tgtPath, filepath.Base(path))
		if _, err := os.Stat(target); os.IsNotExist(err) {
			err = os.MkdirAll(target, 0777)
			if err != nil {
				glog.Exitf("[ERROR] %s", err)
			}
		}
		tgtset = append(tgtset, target)
	}

	glog.Infof("dbset=%v", dbset)
	return dbset, tgtset
}

func main() {
	defer glog.Finalize()

	var (
		cmdString  string
		shardRange string
		zone       int
		node       int

		dbRange string
		dbPath  string
		tgtPath string
		nsList  string

		prefixLen int
		seqScan   = true
		resume    bool
		compact   bool
		keep      bool
	)

	flag.StringVar(&cmdString, "cmd", "none", "specify command")
	flag.StringVar(&shardRange, "shard", "", "shard range: start,stop")
	flag.IntVar(&zone, "zone", -1, "zone id")
	flag.IntVar(&node, "node", -1, "node id")
	flag.StringVar(&dbRange, "db", "", "db range: start,stop")
	flag.StringVar(&nsList, "ns", "", "ns|ns2|ns3")

	// For internal use
	flag.StringVar(&dbPath, "dbpath", "", "dbpath")
	flag.Parse()

	if cmdString == "comp_keep" {
		cmdString = "comp"
		keep = true
	}

	switch cmdString {
	case "scan":
		compact = false
		if len(shardRange) == 0 {
			glog.Exitf("[ERROR] -shard [range] is missing.")
		}
	case "comp":
		compact = true
	default:
		printUsage()
		return
	}

	if zone < 0 {
		glog.Exitf("[ERROR] -zone is missing or invalid.")
	}

	ns := strings.Split(nsList, "|")
	InitCompact(ns, keep)

	path := LoadConfig("config.toml")
	if len(dbPath) == 0 {
		dbPath = path
	}

	end := len(dbPath) - 1
	if dbPath[end] == '/' {
		dbPath = dbPath[:end]
	}

	if len(tgtPath) > 0 &&
		zone < 0 {
		glog.Exitf("[ERROR] -zone is not specified in command line. Copy is restricted to one zone at a time.")
	}

	if len(tgtPath) > 0 && tgtPath[0] != '.' &&
		tgtPath[0] != '/' {
		tgtPath = fmt.Sprintf("%s/%s", filepath.Dir(dbPath), tgtPath)
	}

	if dbPath == tgtPath {
		glog.Exitf("[ERROR] Source and target paths are identical.")
	}

	newDir := fmt.Sprintf("%s/dbcopy_%s", filepath.Dir(dbPath), filepath.Base(dbPath))
	if len(tgtPath) > 0 {
		_, err := os.Stat(newDir)
		if !os.IsNotExist(err) {
			glog.Exitf("[ERROR] %s from last dbcopy was left over.", newDir)
		}
	}

	prefixLen = 3

	var start, stop int
	if len(shardRange) > 0 {
		start, stop = parseKeyRange(shardRange)
		if start < 0 || stop < 0 || stop <= start {
			glog.Exitf("[ERROR] Invalid shard range.")
		}
	}

	dbset, tgtset := getDbSet(dbPath, tgtPath, zone, node, dbRange)
	errset := make([]error, len(dbset))
	cmdlines := make([]CmdLine, len(dbset))

	skip := 0
	for i := 0; i < len(dbset); i++ {
		errset[i] = cmdlines[i].Init(dbset[i], prefixLen, compact)
		if errset[i] != nil {
			skip++
		}
	}

	wg := new(sync.WaitGroup)
	if !seqScan {
		wg.Add(len(dbset) - skip)
	}

	for i := 0; i < len(dbset); i++ {
		glog.Infof("dbpath=%s", dbset[i])

		if errset[i] != nil {
			continue
		}

		if seqScan {
			errset[i] = cmdlines[i].Work(start, stop, tgtset[i], compact)
			continue
		}

		go func(ix int) {
			defer wg.Done()
			errset[ix] = cmdlines[ix].Work(start, stop, tgtset[ix], compact)
		}(i)
	}

	wg.Wait()

	glog.Info("Result Summary:")
	errFound := false
	for i := 0; i < len(errset); i++ {
		result := "Completed without error"
		if errset[i] != nil {
			result = fmt.Sprintf("Failed. %s", errset[i])
			errFound = true
		}
		glog.Infof("db=%s result: %s", dbset[i], result)
	}

	if len(dbset) == 0 {
		glog.Infof("Done")
		return
	}

	if len(tgtPath) == 0 || errFound || resume {
		return
	}

	// Add a tag file.
	tagFile := fmt.Sprintf("%s/microshard_enabled.txt", tgtPath)
	f, err := os.OpenFile(tagFile, os.O_CREATE, 0755)
	if err != nil {
		glog.Exitf("[ERROR] Create failed: %s", err)
	}
	f.Close()

	// Rename directories
	glog.Infof("Rename %s %s", dbPath, newDir)
	err = os.Rename(dbPath, newDir)
	if err != nil {
		glog.Exitf("[ERROR] Rename failed: %s", err)
	}

	glog.Infof("Rename %s %s", tgtPath, dbPath)
	err = os.Rename(tgtPath, dbPath)
	if err != nil {
		glog.Exitf("[ERROR] Rename failed: %s", err)
	}
}

func printUsage() {
	progName := filepath.Base(os.Args[0])
	fmt.Printf("\nUsage:\n")
	fmt.Printf("Scan db:    ./%s -cmd scan -shard [range] -zone [n]\n", progName)
	fmt.Printf("Scan db:    ./%s -cmd scan -shard [range] -zone [n] -db [range]\n\n", progName)

	fmt.Printf("Compact db: ./%s -cmd comp -zone [n]\n", progName)
	fmt.Printf("Compact db: ./%s -cmd comp -zone [n] -db [range]\n\n", progName)
}
