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
	"fmt"

	"juno/cmd/tools/cmd/stats"
	"juno/pkg/cmd"
)

func main() {
	if command, args := cmd.ParseCommandLine(); command != nil {
		if err := command.Parse(args); err == nil {
			command.Exec()
		} else {
			fmt.Printf("* command '%s' failed. %s\n", command.GetName(), err)
		}
	} else {
		cmd.PrintVersionOrUsage()
	}
}

func init() {
	pstats := &stats.CmdProxyStats{}
	pstats.Init("proxy", "get proxy statistics")
	cmd.Register(pstats)
	sstats := &stats.CmdStorageStats{}
	sstats.Init("storage", "get storageserv statistics")
	cmd.Register(sstats)
}
