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

package cfg

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/paypal/junodb/pkg/cfg"
	"github.com/paypal/junodb/pkg/cmd"
)

type cmdConfUnify struct {
	cmd.Command
	optOutFileName string
	optOutFormat   string
}

func (c *cmdConfUnify) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.StringOption(&c.optOutFileName, "o|output-filename", "config.toml", "output filename")
	c.StringOption(&c.optOutFormat, "f|output-format", "toml", "output format {toml|text}")
	c.SetSynopsis("[options] <toml file name> [<toml file name>]")
}

func (c *cmdConfUnify) Exec() {
	c.Validate()
	c.optOutFormat = strings.ToLower(c.optOutFormat)
	file, err := os.Create(c.optOutFileName)
	if err != nil {
		fmt.Printf("fail to create file %s\n", c.optOutFileName)
		return
	}
	defer file.Close()

	if c.NArg() < 1 {
		fmt.Println("no input file")
		return
	}

	var unified cfg.Config

	for _, f := range c.Args() {
		var cfg cfg.Config
		if err := cfg.ReadFromTomlFile(f); err != nil {
			fmt.Printf("%s, Error: %s", f, err)
			return
		}
		unified.Merge(&cfg)
	}
	writer := bufio.NewWriter(file)

	if c.optOutFormat == "toml" {
		unified.WriteToToml(writer)

	} else {
		unified.WriteToKVList(writer)
	}
	writer.Flush()
}

func init() {
	c := &cmdConfUnify{}
	c.Init("config", "unify the given toml configuration file(s)")

	cmd.Register(c)
}
