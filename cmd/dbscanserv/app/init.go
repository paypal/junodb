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
package app

import (
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/BurntSushi/toml"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/clustermgr/cmd"
	"juno/cmd/dbscanserv/config"
	"juno/cmd/dbscanserv/prime"
	"juno/cmd/storageserv/storage/db"
	"juno/pkg/cluster"
	"juno/pkg/etcd"
	"juno/pkg/sec"
)

type CmdConfig cmd.Config

type ClusterConfig struct {
	CmdConfig
	DB             *db.Config
	DbScanLog      string
	DbWatchEnabled bool
	Sec            sec.Config
	DbScan         config.DbScan
	NumConns       int
}

type ClusterMap struct {
	ClusterConfig
	cluster.Cluster
	etcdcli *etcd.EtcdClient
}

var dbscanConfig config.DbScan

func stringToInt(val string) int {
	w, err := strconv.Atoi(val)
	if err != nil {
		glog.Errorf("[ERROR] strconv.Atoi failed, val=%s err=%s", val, err)
		return 0
	}
	return w
}

func InitScanners(file string, readOnly bool) *ClusterMap {

	c, rootpath := newClusterMap(file)
	dbscanConfig = c.ClusterConfig.DbScan
	prime.SetSecConfig(&c.ClusterConfig.Sec)
	prime.InitReplicator(c.ClusterConfig.DbScan.ReplicationAddr, c.ClusterConfig.NumConns)
	InitPatchDb(rootpath, &c.ClusterConfig.DbScan, readOnly)

	for i := range c.Zones {
		for j := range c.Zones[i].Nodes {
			ip := c.getIP(i, j)
			AddRpcClient(i, ip)
			if IsLocalAddress(ip, i) {
				// zoneid, nodeid
				if !prime.AddDbHandle(i, j) {
					glog.Errorf("[ERROR] Failed to open db.")
				}
			}

			shards, err := c.GetShards(uint32(i), uint32(j))
			if err != nil {
				glog.Errorf("[ERROR] zone=%d node=%d error=%s", i, j, err)
				continue
			}

			for _, shardid := range shards {
				// zoneid, shardid, nodeid
				AddScanner(i, int(shardid), j, ip)
				AddShardInfo(i, int(shardid), j, ip)
			}
		}
	}
	return c
}

func InitRemoteMap(file string) *ClusterMap {

	c, _ := newClusterMap(file)

	for i := range c.Zones {
		for j := range c.Zones[i].Nodes {
			ip := c.getIP(i, j)
			AddRpcClient(i, ip)

			shards, err := c.GetShards(uint32(i), uint32(j))
			if err != nil {
				glog.Errorf("[ERROR] zone=%d node=%d error=%s", i, j, err)
				continue
			}

			for _, shardid := range shards {
				// zoneid, shardid, nodeid
				AddShardInfo(i, int(shardid), j, ip)
			}
		}
	}
	return c
}

func newClusterMap(file string) (*ClusterMap, string) {

	c := &ClusterMap{
		ClusterConfig: ClusterConfig{
			CmdConfig: CmdConfig{
				Etcd: *etcd.NewConfig(),
			},
		},
	}

	if _, err := toml.DecodeFile(file, &c.ClusterConfig); err != nil {
		glog.Exitf("[ERROR] %s", err)
	}

	if len(c.Etcd.Endpoints) == 0 {
		glog.Exitf("[ERROR] etcd endpoints not set.")
	}

	c.etcdcli = etcd.NewEtcdClient(&c.Etcd, c.ClusterName)
	for {
		if c.etcdcli != nil {
			break
		}
		glog.Errorf("[WARN] Failed to start etcd client.")
		time.Sleep(30 * time.Second)
	}
	defer c.etcdcli.Close()

	var path string
	if c.ClusterConfig.DB != nil {
		dbPaths := c.ClusterConfig.DB.DbPaths
		if len(dbPaths) > 0 && len(dbPaths[0].Path) > 0 {
			path = dbPaths[0].Path
			prime.SetDBRoot(dbPaths[0].Path)
		}
	}

	// Load cluster config from etcd.
	rw := etcd.NewEtcdReadWriter(c.etcdcli)
	var err error
	if rw != nil {
		_, err = c.Read(rw)
	}
	if rw == nil || err != nil {
		cacheFile := filepath.Join(c.Etcd.CacheDir, c.Etcd.CacheName)
		_, err = c.ReadFromCache(cacheFile)
		if err != nil {
			prime.SetErrorStatus(err.Error())
			glog.Errorf("[ERROR] %s", err)
		}
	}
	glog.Infof("clusterName=%s numZones=%d", c.ClusterName, len(c.Zones))

	c.setOtherConfig()

	return c, path
}

func (c *ClusterMap) setOtherConfig() {

	if c.DbScan.ListenPort > 0 {
		SetListenPort(c.DbScan.ListenPort, c.DbWatchEnabled)
	}

	prime.InitFileWriter(c.DbScanLog)
}

func (c *ClusterMap) GetNumZones() int {
	return len(c.Zones)
}

func (c *ClusterMap) getIP(zoneid int, nodeid int) string {

	i := strings.Index(c.ConnInfo[zoneid][nodeid], ":")

	if i < 0 {
		return c.ConnInfo[zoneid][nodeid]
	}
	return c.ConnInfo[zoneid][nodeid][:i]
}

func (c *ClusterMap) GetLocalShardList(start int, stop int, skip int) (numZones int,
	numShards int, shardsByNode [][]uint32) {

	numShards = 0
	countSkip := 0
	done := false
	for i := 0; i < len(c.Zones); i++ {

		for j := range c.Zones[i].Nodes {
			ip := c.getIP(i, j)
			if !IsLocalAddress(ip, i) {
				continue
			}

			shards, _ := c.GetShards(uint32(i), uint32(j))
			prime.LogMsg(">> zoneid=%d nodeid=%d len=%d shards=%v",
				i, j, len(shards), shards)

			// Find all shards in range [start, stop)
			var list []uint32
			for _, s := range shards {
				if int(s) >= start && int(s) < stop {
					countSkip++
					if countSkip <= skip {
						continue
					}
					list = append(list, s)
				}
			}
			if len(list) > 0 && !done {
				shardsByNode = append(shardsByNode, list)
				numShards += len(list)
			}
		}
		if len(shardsByNode) > 0 {
			done = true
		}
	}

	return len(c.Zones), numShards, shardsByNode
}
