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

	"github.com/BurntSushi/toml"

	"github.com/paypal/junodb/cmd/proxy/config"
	sscfg "github.com/paypal/junodb/cmd/storageserv/config"
	"github.com/paypal/junodb/pkg/cmd"
)

type cfgTypeT int

const (
	cfgTypeProxy cfgTypeT = iota
	cfgTypeStorage
	cfgTypeNumTypes
)

var cfgTypeNames []string = []string{
	"proxy config",
	"ss config",
}

func (t cfgTypeT) String() string {
	if t < cfgTypeNumTypes {
		return cfgTypeNames[t]
	} else {
		return "not supported"
	}
}

type cmdCfgGenT struct {
	cmd.Command
	cfgType     cfgTypeT
	outFileName string
}

func (c *cmdCfgGenT) Init(name string, desc string) {
	c.Command.Init(name, desc)
	cfgTypeUsage := "config type. Supported configuration type:\n"
	for i := cfgTypeProxy; i < cfgTypeNumTypes; i++ {
		cfgTypeUsage += fmt.Sprintf("    \t%d: %s\n", i, i.String())
	}
	cfgTypeUsage += "   \t"
	c.IntOption((*int)(&c.cfgType), "t|type", int(cfgTypeProxy), cfgTypeUsage)
	c.StringOption(&c.outFileName, "f|file-name", "config.toml", "output filename")
	c.SetSynopsis("<type-option> [<filename-option>]")
}

func (c *cmdCfgGenT) Exec() {
	c.Validate()
	if c.cfgType >= cfgTypeNumTypes {
		fmt.Print("unsupported type")
		return
	}
	file, err := os.Create(c.outFileName)
	if err != nil {
		fmt.Print("fail to create file ", c.outFileName)
		return
	}
	defer file.Close()
	writer := bufio.NewWriter(file)
	encoder := toml.NewEncoder(writer)
	switch cfgTypeT(c.cfgType) {
	case cfgTypeProxy:
		encoder.Encode(&config.Conf)
	case cfgTypeStorage:
		encoder.Encode(sscfg.ServerConfig())
	}
	writer.Flush()
}

func init() {
	c := &cmdCfgGenT{}
	c.Init("cfggen", "generate default configuration file")

	cmd.Register(c)
}
