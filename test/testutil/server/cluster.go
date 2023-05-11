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

package server

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/proxy/handler"
	"juno/cmd/proxy/stats"
	"juno/pkg/cluster"
	"juno/pkg/etcd"
	"juno/pkg/net/netutil"
	"juno/pkg/util"
	"juno/test/testutil/log/frwk"
	"juno/test/testutil/mock"
)

type ICluster interface {
	ProxyServer() *IServer

	init()

	Start() error
	Stop() error
}

type Cluster struct {
	Proxy           IServer
	StorageServers  [][]IServer
	ssStartWaitTime time.Duration
	ssStopWaitTime  time.Duration
	numShards       uint32
	cluster_alg_ver uint32
	isInProcProxy   bool
	shardMap        *cluster.ShardMap
}

type ServerType uint

const (
	serverTypeInProcess = ServerType(iota)
	serverTypeStandard
	serverTypeGeneral
)

var (
	serverTypeNames = []string{
		"InProcess",
		"Standard",
		"General",
	}
)

func GetServerTypeText(t ServerType) string {
	if int(t) >= len(serverTypeNames) {
		return "Unsupported"
	}
	return serverTypeNames[t]
}

var (
	supportedProxyServerTypeMap = map[string]ServerType{
		strings.ToUpper(GetServerTypeText(serverTypeInProcess)): serverTypeInProcess,
	}
	supportedStorageServerTypeMap = map[string]ServerType{
		strings.ToUpper(GetServerTypeText(serverTypeInProcess)): serverTypeInProcess,
		strings.ToUpper(GetServerTypeText(serverTypeStandard)):  serverTypeStandard,
		strings.ToUpper(GetServerTypeText(serverTypeGeneral)):   serverTypeGeneral,
	}
)

var ssHostMap map[string]int

func (c *Cluster) GetSSNodes(key []byte) (ssNodes []SSNode) {
	if c.numShards == 0 {
		glog.Fatal("zero numShards")
	}
	if c.shardMap == nil {
		glog.Fatal("shard map is nil")
	}
	sid := uint32(util.GetPartitionId(key, c.numShards))
	numZone := len(c.StorageServers)

	if frwk.LOG_DEBUG {
		glog.DebugInfof("numshards, numzone, cluster_alg_vers are " + strconv.Itoa(int(c.numShards)) +
			" ," + strconv.Itoa(int(numZone)) + ", " + strconv.Itoa(int(c.cluster_alg_ver)))
	}
	_, start_zoneid := util.GetShardInfoByKey(key, uint32(c.numShards), uint32(numZone), c.cluster_alg_ver)
	zones, nodes, e := c.shardMap.GetNodes(sid, start_zoneid)
	if e != nil {
		glog.Fatal("Failed to get Shard Info")
		return
	}
	num := len(zones)
	if num != len(nodes) {
		glog.Fatal("Internal")
		return
	}
	ssNodes = make([]SSNode, num)
	for i := 0; i < num; i++ {
		ssNodes[i].Zone = int(zones[i])
		ssNodes[i].Node = int(nodes[i])
		ssNodes[i].NumShards = c.numShards
		ssNodes[i].Server = c.StorageServers[zones[i]][nodes[i]]
	}
	return
}

func (c *Cluster) getAllSSNodes() (ssNodes []SSNode) {
	num := 0
	for z := range c.StorageServers {
		num += len(c.StorageServers[z])
	}
	i := 0
	ssNodes = make([]SSNode, num)
	for z := range c.StorageServers {
		for n := range c.StorageServers[z] {
			ssNodes[i].Zone = z
			ssNodes[i].Node = n
			ssNodes[i].NumShards = c.numShards
			ssNodes[i].Server = c.StorageServers[z][n]
		}
	}
	return
}

func (c *Cluster) Start() (err error) {
	if frwk.LOG_DEBUG {
		glog.DebugInfof("Starting servers ...")
	}
	var srvs []IServer
	for z := range c.StorageServers {
		for n := range c.StorageServers[z] {
			srvs = append(srvs, c.StorageServers[z][n])
			if ssErr := c.StorageServers[z][n].Start(); ssErr != nil {
				glog.Fatalf("failed to start SS %d-%d %s", z, n, ssErr)
			}
		}
	}
	if WaitForUp(c.ssStartWaitTime, srvs...) == false {
		err = fmt.Errorf("not all SSs started in time")
		return
	}

	if c.Proxy != nil {
		if pErr := c.Proxy.Start(); pErr != nil {
			err = fmt.Errorf("proxy failed to start: %s", pErr)
			return
		}
		if waitForStateWithSSs(c.IsProxyConnectedWithSS, c.getAllSSNodes()...) == false {
			err = fmt.Errorf("proxy not connecting to all SS")
			return
		}
	} else {
		err = fmt.Errorf("nil proxy")
	}
	return
}

func (c *Cluster) Stop() {
	if frwk.LOG_DEBUG {
		glog.DebugInfof("Shutting down servers ...")
	}
	if cluster.GetShardMgr() != nil {
		cluster.GetShardMgr().Shutdown(nil)
	}
	if c.Proxy != nil {
		c.Proxy.Stop()
	}

	c.StopAllStorageServers()
	waitForStateWithSSs(c.IsProxyNotConnectedWithSS)
}

func (c *Cluster) StartAllStorageServers() {
	var srvs []IServer
	for z := range c.StorageServers {
		if frwk.LOG_DEBUG {
			glog.DebugInfof("Starting SS in zone %d", z)
		}
		for n := range c.StorageServers[z] {
			if frwk.LOG_DEBUG {
				glog.DebugInfof("Starting SS[%d, %d]", z, n)
			}
			srvs = append(srvs, c.StorageServers[z][n])
			if err := c.StorageServers[z][n].Start(); err != nil {

				glog.Fatal(err)
			}
		}
	}
	WaitForUp(c.ssStartWaitTime, srvs...)
	waitForStateWithSSs(c.IsProxyConnectedWithSS)
}

func (c *Cluster) StopAllStorageServers() {
	var srvs []IServer
	for z := range c.StorageServers {
		for n := range c.StorageServers[z] {
			if frwk.LOG_DEBUG {
				glog.DebugInfof("Stoping SS[%d, %d]", z, n)
			}
			srvs = append(srvs, c.StorageServers[z][n])
			go func(srv IServer) {
				if err := srv.Stop(); err != nil {
					glog.Error(err)
				}
			}(c.StorageServers[z][n])
		}
	}
	if WaitForDown(c.ssStopWaitTime, srvs...) == false {
		glog.Fatal("SSs not down in time")
	}
	if waitForStateWithSSs(c.IsProxyNotConnectedWithSS) == false {
		glog.Fatal("Proxy failed to establish required connections to SSs")
	}
}

func (c *Cluster) StartStorageServer(ssNodes []SSNode, indices ...int) (err error) {
	numSSs := len(indices)
	srvs := make([]IServer, 0, numSSs)
	numZone := len(c.StorageServers)
	for _, i := range indices {
		zone := ssNodes[i].Zone
		if zone >= 0 && zone < numZone {
			numNode := len(c.StorageServers[zone])
			node := ssNodes[i].Node
			if node >= 0 && node < numNode {
				s := c.StorageServers[zone][node]
				srvs = append(srvs, s)
				if frwk.LOG_DEBUG {
					glog.DebugInfof("Starting SS[%d, %d](%d) @%s:%s", zone, node, s.Id(), s.IPAddress(), s.Port())
				}
				if err = s.Start(); err != nil {
					glog.Error(err)
					return
				}
			}
		}
	}
	allUp := WaitForUp(c.ssStartWaitTime, srvs...)
	if allUp {
		if frwk.LOG_DEBUG {
			glog.DebugInfof("All Up")
		}
	} else {
		err = fmt.Errorf("Not all Up")
		glog.Error(err)
		return
	}

	if waitForStateWithSSs(c.IsProxyConnectedWithSS, getSsNodes(ssNodes, indices...)...) == false {
		err = fmt.Errorf("proxy not connected all the SSs in time")
		glog.Error(err)
	}
	return
}

func getSsNodes(ssNodes []SSNode, indices ...int) []SSNode {
	num := len(indices)

	nodes := make([]SSNode, num)
	for i := 0; i < num; i++ {
		nodes[i] = ssNodes[indices[i]]
	}
	return nodes
}

func (c *Cluster) StopStorageServer(ssNodes []SSNode, indices ...int) (err error) {
	numSSs := len(indices)
	srvs := make([]IServer, 0, numSSs)
	numZone := len(c.StorageServers)
	for _, i := range indices {
		zone := ssNodes[i].Zone
		if zone >= 0 && zone < numZone {
			numNode := len(c.StorageServers[zone])
			node := ssNodes[i].Node
			if node >= 0 && node < numNode {
				s := c.StorageServers[zone][node]
				srvs = append(srvs, s)
				if frwk.LOG_DEBUG {
					glog.DebugInfof("Stopping SS[%d, %d](%d) @%s:%s", zone, node, s.Id(), s.IPAddress(), s.Port())
				}
				if err = s.Stop(); err != nil {
					glog.Error(err)
					return
				}
			}
		}
	}
	allDown := WaitForDown(c.ssStopWaitTime, srvs...)
	if allDown {
		if frwk.LOG_DEBUG {
			glog.DebugInfof("All down")
		}
	} else {
		err = fmt.Errorf("Not all SSs down")
		glog.Error(err)
		return
	}
	if waitForStateWithSSs(c.IsProxyNotConnectedWithSS, getSsNodes(ssNodes, indices...)...) == false {
		err = fmt.Errorf("proxy not disconnected the given SSs in time")
	}
	return
}

func NewClusterWithConfig(conf *ClusterConfig) (c *Cluster) {
	c = &Cluster{isInProcProxy: conf.Proxy.IsInProcess()}

	c.numShards = uint32(conf.ProxyConfig.ClusterInfo.NumShards)
	c.cluster_alg_ver = uint32(conf.ProxyConfig.ClusterInfo.AlgVersion)
	var host, port string
	var err error
	var addresses []net.IP
	host, port, err = net.SplitHostPort(conf.ProxyAddress.Addr)
	if err != nil {
		glog.Fatal("Invalid proxy address ", conf.ProxyAddress.Addr, " error: ", err)
	}
	addresses, err = net.LookupIP(host)
	if err != nil || len(addresses) == 0 {
		glog.Fatal("cannot resolve proxy IP: ", err)
	}
	ipAddr := addresses[0].String() //take the first one

	var cls *cluster.Cluster

	if conf.Proxy.IsInProcess() {
		stats.Initialize(stats.KTypeStandAloneWorker)
		var chWatch chan int
		cls = &cluster.ClusterInfo[0]
		glog.Debug("ProxyAdd in func test1 is ", conf.ProxyAddress)
		if conf.ProxyConfig.EtcdEnabled {
			chWatch = etcd.WatchForProxy()
			etcd.Connect(&conf.ProxyConfig.Etcd, conf.ProxyConfig.ClusterName)
			rw := etcd.GetClsReadWriter()
			if rw == nil {
				glog.Exitf("no etcd setup")
			}
			cls.Read(rw)
		} else {

			cls.PopulateFromConfig()
		}
		cluster.Initialize(&cluster.ClusterInfo[0], &conf.ProxyConfig.Outbound, chWatch, etcd.GetClsReadWriter(), nil, nil)
		conf.ProxyConfig.SetListeners([]string{conf.ProxyAddress.Addr})
		if conf.ProxyAddress.SSLEnabled {
			conf.ProxyConfig.Listener[0].SSLEnabled = true
		}
		c.Proxy = NewInProcessServer("proxy", ipAddr, port, handler.NewProxyService(conf.ProxyConfig), conf.ProxyAddress.SSLEnabled, &conf.Proxy)
		c.shardMap = cluster.GetShardMgr().GetShardMap()
	} else {
		cls = &cluster.ClusterInfo[0]
		if conf.ProxyConfig.EtcdEnabled {
			etcd.Connect(&conf.ProxyConfig.Etcd, conf.ProxyConfig.ClusterName)
			rw := etcd.GetClsReadWriter()
			if rw == nil {
				glog.Exitf("no etcd setup")
			}
			cls.Read(rw)
		} else {

			cls.PopulateFromConfig()
		}
		srv := NewServer("proxy", 1, ipAddr, port, conf.ProxyAddress.SSLEnabled, conf.ProxyConfig.HttpMonAddr, &conf.Proxy)
		if conf.ProxyAddress.SSLEnabled {
			srv.AddStartCmdArg("ssl")
		}
		c.Proxy = srv
		c.shardMap = cls.CreateShardMap()
	}

	connInfo := cls.ConnInfo
	if len(connInfo) != int(conf.ProxyConfig.ClusterInfo.NumZones) {
		glog.Fatal("Invalid ConnInfo")
	}
	c.StorageServers = make([][]IServer, conf.ProxyConfig.ClusterInfo.NumZones)
	ssHostMap = make(map[string]int)

	ssDef := &conf.StorageServer
	if ssDef.StartWaitTime.Duration == 0 {
		ssDef.StartWaitTime.Duration = 1 * time.Second
	}
	c.ssStartWaitTime = ssDef.StartWaitTime.Duration
	if ssDef.StopWaitTime.Duration == 0 {
		ssDef.StopWaitTime.Duration = 1 * time.Second
	}
	c.ssStopWaitTime = ssDef.StopWaitTime.Duration

	for i := range connInfo {
		c.StorageServers[i] = make([]IServer, len(connInfo[i]))
		for ni := range connInfo[i] {
			host, port, err = net.SplitHostPort(connInfo[i][ni])
			addresses, err = net.LookupIP(host)
			if err != nil || len(addresses) == 0 {
				glog.Fatal("cannot get IP: ", err)
			}
			ipAddr := addresses[0].String() //take the first one
			if err != nil {
				glog.Fatalf("invalid ConnInfo[%d][%d]:%s error: %s", i, ni, connInfo[i][ni], err)
			}
			if netutil.IsLocalAddress(ipAddr) && strings.EqualFold(ssDef.Type, "mockss") {
				c.StorageServers[i][ni] = NewInProcessServer(
					"mockss", ipAddr, port, mock.NewMockStorageService(mock.DefaultSSConfig, connInfo[i][ni]), false, ssDef)

			} else {

				srvID := 1

				if v, ok := ssHostMap[ipAddr]; ok == true {
					srvID = v + 1
				}
				ssHostMap[ipAddr] = srvID
				c.StorageServers[i][ni] = NewServer("ss", uint(srvID), ipAddr, port, false, "", ssDef)
			}
		}
	}

	return
}

func (c *Cluster) IsProxyConnectedWithSS(nodes ...SSNode) bool {
	if c.isInProcProxy {
		if cluster.GetShardMgr() != nil {
			for _, node := range nodes {
				if !cluster.GetShardMgr().IsConnected(node.Zone, node.Node) {
					return false
				}
			}
			return true
		} else {
			glog.Fatal("nil ShardMgr")
		}
	} else {
		query := fmt.Sprintf("http://%s/stats?info=ss_connected", c.Proxy.GetHttpMonAddr())
		for _, ss := range nodes {
			query += fmt.Sprintf("&node=%d,%d", ss.Zone, ss.Node)
		}
		if resp, err := http.Get(query); err == nil {
			if body, err := io.ReadAll(resp.Body); err == nil {
				if strings.EqualFold(string(body), "true") {
					return true
				}
			}
		}
		glog.Verboseln(query)
	}
	return false
}

func isConnRefused(err error) bool {
	if opErr, ok := err.(*net.OpError); ok {
		if sErr, ok := opErr.Err.(*os.SyscallError); ok {
			if sErr.Err == syscall.ECONNREFUSED {
				return true
			}
		}
	}
	return false
}

func (c *Cluster) IsProxyNotConnectedWithSS(nodes ...SSNode) bool {
	if c.isInProcProxy {
		if cluster.GetShardMgr() != nil {
			for _, node := range nodes {
				if cluster.GetShardMgr().IsConnected(node.Zone, node.Node) {
					return false
				}
			}
			return true
		} else {
			glog.Fatal("nil ShardMgr")
		}
	} else {
		query := fmt.Sprintf("http://%s/stats?info=ss_not_connected", c.Proxy.GetHttpMonAddr())
		for _, ss := range nodes {
			query += fmt.Sprintf("&node=%d,%d", ss.Zone, ss.Node)
		}
		if resp, err := http.Get(query); err == nil {
			if body, err := io.ReadAll(resp.Body); err == nil {
				if strings.EqualFold(string(body), "true") {
					return true
				}
			}
		} else {
			if urlerr, ok := err.(*url.Error); ok {
				if isConnRefused(urlerr.Err) {
					return true
				}
			}
		}

		glog.Verboseln(query)
	}
	return false
}

func waitForStateWithSSs(fn func(ss ...SSNode) bool, nodes ...SSNode) bool {
	timer := time.NewTimer(20 * time.Second)
	ticker := time.NewTicker(100 * time.Millisecond)

	defer func() {
		timer.Stop()
		ticker.Stop()
	}()
	for {
		if fn(nodes...) {
			return true
		}
		select {
		case <-timer.C:
			fmt.Println("TIMEOUT......")
			return false
		case <-ticker.C:
			//fmt.Println("...tick......")
			continue
		}
	}

	return false
}
