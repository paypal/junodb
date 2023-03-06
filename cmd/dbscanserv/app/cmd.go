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
  
package app

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"juno/third_party/forked/golang/glog"
	"juno/third_party/forked/tecbot/gorocksdb"

	"juno/cmd/dbscanserv/prime"
	"juno/cmd/storageserv/compact"
	"juno/cmd/storageserv/storage/db"
)

type CmdLine struct {
	cfgFile    string
	cmd        string
	zoneid     int
	serverAddr string

	startid int
	stopid  int
	nsFile  string
	nsNames string
	trace   bool
}

func NewCmdLine(cfgFile, cmd string, zoneid int, serverAddr string,
	startid, stopid int, nsNames string, trace bool) *CmdLine {
	return &CmdLine{
		cfgFile:    cfgFile,
		cmd:        cmd,
		zoneid:     zoneid,
		serverAddr: serverAddr,
		startid:    startid,
		stopid:     stopid,
		nsNames:    nsNames,
		trace:      trace,
	}
}

func NewCmdLine2(cfgFile, cmd string, zoneid int, nsFile string) *CmdLine {
	return &CmdLine{
		cfgFile: cfgFile,
		cmd:     cmd,
		zoneid:  zoneid,
		nsFile:  nsFile,
	}
}

func (c *CmdLine) HandleCommand() {

	prime.SetCommandMode()

	if len(c.serverAddr) > 0 && len(c.serverAddr) < 6 {
		glog.Errorf("[ERROR] Parameter -s "+
			"has invalid address: %s", c.serverAddr)
	}

	if c.cmd != "copy_ns" && c.cmd != "delete_ns" && c.cmd != "readpatch" &&
		len(c.nsNames) > 0 {
		glog.Errorf("[ERROR] Parameter -ns is not allowed.")
		return
	}

	switch c.cmd {
	case "copy_ns":
		if len(c.nsNames) == 0 {
			glog.Errorf("[ERROR] Parameter -ns is not specified.")
			return
		}
		prime.SetNamespaceNames(c.nsNames, c.trace)
		fallthrough
	case "run":
		if c.zoneid < 0 {
			glog.Errorf("[ERROR] Parameter -zone cannot be negative.")
			return
		}

		if len(c.serverAddr) > 0 {
			if !IsValidAddr(c.serverAddr) {
				glog.Errorf("[ERROR] Scan failed to start.")
				return
			}
			prime.SetProxyAddr(c.serverAddr)
		}
		c.runCollect()

	case "status":
		c.pollDrivers()

	case "ping":
		c.pollServers()

	case "stop":
		c.stopServers()

	case "patch":
		c.doPatch()

	case "readpatch":
		InitScanners(c.cfgFile, true)
		DoPatch(c.nsNames)

	case "get", "getone", "fix":
		c.testGetKey()

	case "delete_ns":
		c.deleteNamespace()

	default:
		glog.Exitf("[ERROR] Invalid command: %s", c.cmd)
	}
}

func (c *CmdLine) runCollect() {

	InitRemoteMap(c.cfgFile)

	glog.Infof("proxy_addr='%s' shard_range=[%d:%d]",
		prime.GetProxyAddr(), c.startid, c.stopid)

	if !PingServers(cmdInit, c.startid, c.stopid) {
		glog.Errorf("[ERROR] Scan failed to start. Not all servers are available.")
		return
	}

	time.Sleep(1 * time.Second)
	PingDrivers(cmdRun, c.zoneid, c.startid, c.stopid)
	glog.Infof("Starts scanning ...")
	time.Sleep(2 * time.Second)
	CloseConnect()
}

func (c *CmdLine) pollDrivers() {
	InitRemoteMap(c.cfgFile)
	PingDrivers(cmdStatus, c.zoneid, 0, 0)
	CloseConnect()
}

func (c *CmdLine) pollServers() {
	InitRemoteMap(c.cfgFile)
	PingServers(cmdStatus, c.startid, c.stopid, c.serverAddr)
	CloseConnect()
}

func (c *CmdLine) stopServers() {
	InitRemoteMap(c.cfgFile)
	PingServers(cmdStop, c.startid, c.stopid, c.serverAddr)
	CloseConnect()
}

func (c *CmdLine) doPatch() {
	InitRemoteMap(c.cfgFile)
	PingServers(cmdPatch, c.startid, c.stopid, c.serverAddr)
	CloseConnect()
}

func (c *CmdLine) deleteNamespace() {
	clusterMap := InitRemoteMap(c.cfgFile)

	event := compact.NewEventConfig(c.nsFile)
	if event == nil {
		return
	}
	buf := new(bytes.Buffer)
	if err := compact.EncodeEventConfig(buf, event); err != nil {
		glog.Exitf("[ERROR] Bad format in %s: %s", c.nsFile, err)
	}
	glog.Infof("Delete list: %v", event.Delete)

	if c.zoneid < 0 {
		glog.Exitf("zone=%d is not allowed.", c.zoneid)
	}

	ok, addr := LockSession()
	if !ok {
		CloseConnect()
		return
	}

	for i := c.zoneid; i < clusterMap.GetNumZones(); i++ {
		PingServersByZone(cmdClearNamespace, i, addr, buf.Bytes())
		if i == clusterMap.GetNumZones()-1 {
			break
		}
		for j := 0; j < 12; j++ {
			RefreshLock(addr)
			time.Sleep(2 * time.Second)
		}
	}

	UnlockSession(addr)
	CloseConnect()
}

func (c *CmdLine) testGetKey() {

	var key []byte
	s := bufio.NewScanner(os.Stdin)
	if s.Scan() {
		line := s.Text()
		line = strings.TrimSpace(line)
		buf := strings.ReplaceAll(line, " ", ",")
		json.Unmarshal([]byte(buf), &key)
	}

	if len(key) == 0 {
		return
	}

	switch c.cmd {
	case "get":
		clusterMap := InitRemoteMap(c.cfgFile)
		GetVersionFromServers(key, c.serverAddr, clusterMap.GetNumZones())
		CloseConnect()

	case "getone":
		InitScanners(c.cfgFile, true)
		val, size, err := GetVersionFromDb(key, c.zoneid)
		mt, ct, ver, et, md := prime.DecodeVal(val)

		E9 := uint64(time.Second)
		glog.Infof("Get: key=%v len=%d", key, size)
		glog.Infof("     zoneid=%d error=%s"+
			"     md=%v mt=%d.%d ct=%d ver=%d et=%d et-ct=%d",
			c.zoneid, err,
			md, mt/E9, mt%E9, ct, ver, et, et-ct)

	case "fix":
		if len(c.serverAddr) == 0 {
			glog.Errorf("Proxy addr not specified.")
			return
		}
		if !IsValidAddr(c.serverAddr) {
			return
		}

		prime.SetProxyAddr(c.serverAddr)
		prime.RepairKey(key, true)
		prime.CloseProxyConnect()
	}
}

func GetVersionFromDb(key []byte, zoneid int) (data []byte, valLen int,
	msg string) {

	data = prime.NewValBuffer()
	valLen = 0

	shardid := int(binary.BigEndian.Uint16(key[0:2]))
	info, found := GetShardInfo(zoneid, shardid)
	if !found {
		msg = "Shardid not found."
		return
	}

	handle := prime.GetDbHandle(zoneid, info.nodeid)
	var ro *gorocksdb.ReadOptions = gorocksdb.NewDefaultReadOptions()
	value, err := handle.Get(ro, key)

	if err != nil {
		msg = fmt.Sprintf("%s", err)
		return
	}

	if len(value.Data()) == 0 {
		msg = "no key"
		return
	}

	rec := new(db.Record)
	if err = rec.Decode(value.Data()); err == nil {
		prime.EncodeVal(data, rec)
	}

	if err != nil {
		msg = fmt.Sprintf("%s", err)
		return
	}

	valLen = len(value.Data())
	msg = "ok"

	return
}
