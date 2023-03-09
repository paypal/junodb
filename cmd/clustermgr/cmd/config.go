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

package cmd

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/BurntSushi/toml"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/cluster"
	"juno/pkg/etcd"
)

var (
	clusterInfo    [2]cluster.Cluster
	NumNodesByZone []string
)

type Config struct {
	ClusterName    string
	ClusterInfo    *cluster.Config
	K8sClusterInfo *K8sCluster
	Etcd           etcd.Config
}

type K8sCluster struct {
	NumZones         uint32
	NumShards        uint32
	SSHostNameFormat string // "ss-%d-%d.ss-%d.ss"
	SSPorts          []uint16
}

func (c *Config) Validate() {

	if len(c.ClusterName) == 0 {
		glog.Exit(errors.New("cluster name not specified"))
	}

	if c.K8sClusterInfo != nil {

		ci := c.ClusterInfo
		k8s := c.K8sClusterInfo

		ci.AlgVersion = 2
		ci.NumZones = k8s.NumZones
		ci.NumShards = k8s.NumShards
		ci.SSPorts = k8s.SSPorts

		if len(NumNodesByZone) != int(ci.NumZones) {
			glog.Exitf("[ERROR] Number of zones set by -scale is not equal to %d.",
				ci.NumZones)
		}

		// Set SSHosts
		ci.SSHosts = make([][]string, ci.NumZones)
		for i := 0; i < int(ci.NumZones); i++ {
			n, _ := strconv.Atoi(NumNodesByZone[i])
			if n <= 0 {
				glog.Exitf("[ERROR] Number of nodes set by -scale is not valid.")
			}

			for j := 0; j < n; j++ { // n nodes
				host := fmt.Sprintf(
					// "ss-%d-%d.ss-%d.ss",
					k8s.SSHostNameFormat,
					i, // zoneid
					j, // nodeid
					i) // zoneid
				ci.SSHosts[i] = append(ci.SSHosts[i], host)
			}
		}
	}

	if err := c.ClusterInfo.Validate(); err != nil {
		glog.Exit(err)
	}
}

var cfg = Config{
	ClusterInfo: &clusterInfo[0].Config,
	Etcd:        *etcd.NewConfig("127.0.0.1:2379"),
}

var newCfg = Config{
	ClusterInfo: &clusterInfo[1].Config,
	Etcd:        *etcd.NewConfig("127.0.0.1:2379"),
}

func LoadConfigOnly(file string) {
	if _, err := toml.DecodeFile(file, &cfg); err != nil {
		glog.Exitf("Failed to load config file %s. %s", file, err)
	}
}

func LoadConfig(file string) {
	if _, err := toml.DecodeFile(file, &cfg); err != nil {
		glog.Exitf("Failed to load config file %s. %s", file, err)
	}

	cfg.Validate()
	if cfg.ClusterInfo.AlgVersion == 0 {
		cfg.ClusterInfo.AlgVersion = 1
	}
}

func LoadNewConfig(file string) {
	if _, err := toml.DecodeFile(file, &newCfg); err != nil {
		glog.Exitf("Failed to load config file %s. %s", file, err)
	}

	newCfg.Validate()
	if newCfg.ClusterInfo.AlgVersion == 0 {
		newCfg.ClusterInfo.AlgVersion = 1
	}
}
