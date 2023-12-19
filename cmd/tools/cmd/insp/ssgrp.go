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

package insp

import (
	"encoding/hex"
	"fmt"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/BurntSushi/toml"

	"github.com/paypal/junodb/pkg/cluster"
	"github.com/paypal/junodb/pkg/cmd"
	"github.com/paypal/junodb/pkg/util"
)

type (
	cmdSsGroupT struct {
		cmd.Command
		cfg struct {
			ClusterInfo cluster.Config
		}
		optCfgFile  string
		optHexKey   bool
		optNumZones int
		key         []byte
	}
)

func (c *cmdSsGroupT) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.StringOption(&c.optCfgFile, "c|config", "", "specify toml configuration file name")
	c.BoolOption(&c.optHexKey, "hex", true, "specify if key is in hex")
	c.SetSynopsis("-c <config file> [-hex=false] <key>")
}

func (c *cmdSsGroupT) Parse(args []string) (err error) {
	if err = c.FlagSet.Parse(args); err != nil {
		return
	}
	n := c.NArg()
	if n < 1 {
		err = fmt.Errorf("missing key")
		return
	}
	if c.optCfgFile == "" {
		err = fmt.Errorf("missing config file")
		return
	}
	if c.optHexKey {
		if c.key, err = hex.DecodeString(c.Arg(0)); err != nil {
			return
		}
	} else {
		c.key = []byte(c.Arg(0))
	}
	if len(c.key) == 0 {
		err = fmt.Errorf("emptry key")
		return
	}
	if _, err = toml.DecodeFile(c.optCfgFile, &c.cfg); err != nil {
		glog.Exitf("failed to load config file %s. %s", c.optCfgFile, err.Error())
	}
	err = c.cfg.ClusterInfo.Validate()
	return
}

func (c *cmdSsGroupT) Exec() {
	c.Validate()

	id, start_zoneid := util.GetShardInfoByKey(c.key, uint32(c.cfg.ClusterInfo.NumShards), uint32(c.cfg.ClusterInfo.NumZones), c.cfg.ClusterInfo.AlgVersion)
	cls := cluster.Cluster{
		Config: c.cfg.ClusterInfo,
	}
	cluster.SetMappingAlg(cls.AlgVersion)
	cls.PopulateFromConfig()
	shardMap := cluster.NewShardMap(&cls)
	if zones, nodes, err := shardMap.GetNodes(uint32(id), start_zoneid); err == nil {
		if len(zones) != len(nodes) {
			return
		}
		fmt.Printf("\tShareId   : %d\n", id)
		fmt.Printf("\t------------------\n")
		for i := 0; i < len(zones); i++ {
			fmt.Printf("\tSS[%d:%d-%02d]: %s\n", i, zones[i], nodes[i], cls.ConnInfo[zones[i]][nodes[i]])
		}
	}
}

func init() {
	c := &cmdSsGroupT{}
	c.Init("ssgrp", "print the Shard ID and the SS group of a given key")

	cmd.Register(c)
}
