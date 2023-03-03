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
package config

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"juno/third_party/forked/golang/glog"

	"github.com/BurntSushi/toml"

	repconfig "juno/cmd/proxy/replication/config"
	"juno/pkg/cluster"
	"juno/pkg/etcd"
	"juno/pkg/initmgr"
	"juno/pkg/io"
	cal "juno/pkg/logging/cal/config"
	"juno/pkg/logging/sherlock"
	"juno/pkg/sec"
	"juno/pkg/service"
	"juno/pkg/util"
	"juno/pkg/version"
)

var (
	Initializer initmgr.IInitializer = initmgr.NewInitializer(initialize, finalize)

	Conf = Config{
		Config:      service.DefaultConfig,
		NumChildren: 1,

		MaxRecordVersion:   ^uint32(0),
		MaxKeyLength:       128,
		MaxNamespaceLength: 64,
		MaxPayloadLength:   204800,
		MaxTimeToLive:      3600 * 24 * 3,
		DefaultTimeToLive:  3600,

		StateLogEnabled:              true,
		TwoPhaseDestroyEnabled:       true,
		PayloadEncryptionEnabled:     false,
		ReplicationEncryptionEnabled: false,
		BypassLTMEnabled:             false,
		CloudEnabled:                 false,

		StateLogDir:          "./",
		PidFileName:          "junoserv.pid",
		LogLevel:             "INFO",
		ClusterName:          "cluster",
		ClusterInfo:          &cluster.ClusterInfo[0].Config,
		ClusterStats:         cluster.DefaultStatsConfig,
		ReqProcessorPoolSize: 5000,
		MaxNumReqProcessors:  20000,

		Outbound: io.DefaultOutboundConfig,
		ReqProc: ReqProcConfig{
			SSReqTimeout: util.Duration{100 * time.Millisecond},
		},
		Replication: repconfig.DefaultConfig,
		CAL: cal.Config{
			Host:             "127.0.0.1",
			Port:             1118,
			Environment:      "PayPal",
			Poolname:         "junoproxy",
			MessageQueueSize: 10000,
			CalType:          "socket",
		},
		Etcd: *etcd.NewConfig("127.0.0.1:2379"),
		Sec:  sec.DefaultConfig,
		Sherlock: sherlock.Config{
			Enabled: true,
		},
	}
)

type ReqProcConfig struct {
	SSReqTimeout util.Duration
}

type Config struct {
	service.Config

	RootDir     string
	StateLogDir string

	HttpMonAddr string
	PidFileName string

	NumChildren        int
	MaxRecordVersion   uint32
	MaxKeyLength       int
	MaxNamespaceLength int
	MaxPayloadLength   int
	MaxTimeToLive      int
	DefaultTimeToLive  int

	StateLogEnabled              bool
	EtcdEnabled                  bool
	TwoPhaseDestroyEnabled       bool
	PayloadEncryptionEnabled     bool
	ReplicationEncryptionEnabled bool
	BypassLTMEnabled             bool
	CloudEnabled                 bool

	ReqProcessorPoolSize int
	MaxNumReqProcessors  int

	LogLevel    string
	ClusterName string

	ClusterInfo  *cluster.Config
	ClusterStats cluster.StatsConfig
	Outbound     io.OutboundConfig
	ReqProc      ReqProcConfig
	Replication  repconfig.Config
	CAL          cal.Config
	Etcd         etcd.Config
	Sec          sec.Config
	Sherlock     sherlock.Config
}

func (c *Config) GetNumWrites() uint32 {
	return (c.ClusterInfo.NumZones + 1) / 2
}

func (c *Config) Dump() {
	var buf bytes.Buffer
	encoder := toml.NewEncoder(&buf)
	encoder.Encode(c)
	glog.Info(buf.String())
}

// set path to be under Config.RootDir if path is empty or not specified as absolute path
func (c *Config) validatePath(path *string) {
	if path != nil {
		if len(*path) == 0 {
			*path = filepath.Clean(c.RootDir + "/")
		} else if !filepath.IsAbs(*path) {
			*path = filepath.Clean(c.RootDir + "/" + *path)
		}
	}
}

func (c *Config) validatePathAndFileNames() (err error) {
	if len(c.RootDir) == 0 {
		c.RootDir = filepath.Dir(os.Args[0])
	}
	c.validatePath(&c.StateLogDir)
	c.validatePath(&c.Sec.CertPemFilePath)
	c.validatePath(&c.Sec.KeyPemFilePath)
	c.validatePath(&c.Sec.KeyStoreFilePath)
	c.validatePath(&c.Etcd.CacheDir)
	c.validatePath(&c.PidFileName)
	return
}

func (c *Config) Validate() (err error) {
	c.Config.SetDefaultIfNotDefined()
	c.Replication.Validate()
	err = c.Config.Validate()
	if err != nil {
		glog.Errorf("config error: %s", err)
	}
	return
}

func (c *Config) IsTLSEnabled(serverSide bool) (enabled bool) {
	if serverSide {
		for _, lsnr := range c.Listener {
			if lsnr.SSLEnabled {
				enabled = true
				break
			}
		}
	} else {
		for _, target := range c.Replication.Targets {
			if target.SSLEnabled {
				enabled = true
				break
			}
		}
	}
	return
}

func (c *Config) IsEncryptionEnabled() bool {
	return c.ReplicationEncryptionEnabled || c.PayloadEncryptionEnabled
}

///TODO find a better name
func (c *Config) GetSecFlag() (f sec.Flag) {
	if c.IsTLSEnabled(true) {
		f |= sec.KFlagServerTlsEnabled
	}
	if c.IsTLSEnabled(false) {
		f |= sec.KFlagClientTlsEnabled
	}
	if c.IsEncryptionEnabled() {
		f |= sec.KFlagEncryptionEnabled
	}
	return
}

func LoadConfig(file string) (err error) {
	if _, err = toml.DecodeFile(file, &Conf); err != nil {
		glog.Exitf("config error : %s", err)
		return
	}
	if err = Conf.validatePathAndFileNames(); err != nil {
		return
	}

	if Conf.EtcdEnabled {
		etcd.Connect(&Conf.Etcd, Conf.ClusterName)
		rw := etcd.GetClsReadWriter()
		cacheFileName := filepath.Join(Conf.Etcd.CacheDir, Conf.Etcd.CacheName)

		if rw != nil {
			cluster.Version, err = cluster.ClusterInfo[0].Read(rw)
		}
		if rw == nil || err != nil {
			if cluster.Version, err = cluster.ClusterInfo[0].ReadFromCache(cacheFileName); err == nil {
				glog.Infof("Read from etcd cache.")
			}
		}
	} else {
		err = cluster.ClusterInfo[0].PopulateFromConfig()
	}
	if err != nil {
		glog.Errorf("error: %s. \n", err)
		return
	}
	Conf.CAL.Label = version.OnelineVersionString()
	if err = Conf.Validate(); err == nil {
		resetDefaultLimitsConfig()
	}

	return
}

func initialize(args ...interface{}) (err error) {
	sz := len(args)
	if sz < 1 {
		err = fmt.Errorf("a string config file name argument expected")
		return
	}
	filename, ok := args[0].(string)

	if ok == false {
		err = fmt.Errorf("wrong argument type. a string config file name expected")
		return
	}
	err = LoadConfig(filename)
	return
}

func finalize() {
	if Conf.EtcdEnabled {
		etcd.Close()
	}
}
