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
	"encoding/binary"
	"errors"
	"fmt"
	"net/rpc"
	"strings"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/dbscanserv/config"
	"juno/cmd/dbscanserv/prime"
)

type RpcClient struct {
	zoneidHash uint32
	timeout    int
	iterations int
	serverIP   string // ip:port
	client     *rpc.Client
}

// map from server ip to rpc client.
type RemoteMap map[string]*RpcClient

var (
	remoteMap = make(RemoteMap)
)

func GetRpcClient(addr string) *RpcClient {
	return remoteMap[addr]
}

func AddRpcClient(zoneid int, ip string) {

	addr := GenRemoteAddr(ip, zoneid)
	client, found := remoteMap[addr]
	if !found {
		client = &RpcClient{
			serverIP: addr,
		}
		remoteMap[addr] = client
	}
	client.zoneidHash |= (1 << uint32(zoneid))
	client.timeout = 20
	client.iterations = 3
}

func NewRpcClient(port int) *RpcClient {
	client := &RpcClient{
		serverIP: fmt.Sprintf(":%d", port),
	}
	client.timeout = 20
	client.iterations = 3
	return client
}

func CloseConnect() {
	for _, client := range remoteMap {
		if client.client == nil {
			continue
		}
		client.client.Close()
		client.client = nil
	}
}

func (c *RpcClient) Timeout(t int, i int) *RpcClient {
	c.timeout = t
	c.iterations = i
	return c
}

func (c *RpcClient) Invoke(methodName string, req interface{},
	reply interface{}) (err error) {

	for i := 0; i < c.iterations; i++ {

		if c.client == nil {
			for j := 0; j < 3; j++ {
				if err = c.connect(); err == nil {
					break
				}
				if j == 2 {
					return err
				}
			}
			glog.Infof("Connected")
		}

		step := c.timeout
		call := c.client.Go(methodName, req, reply, make(chan *rpc.Call, 1))
		ticker := time.NewTicker(time.Duration(step) * time.Second)
		select {
		case <-call.Done:

			err = call.Error
			if err != nil {
				glog.Errorf("[WARN] Retry")
				time.Sleep(time.Duration(1) * time.Second)
				break
			}
			ticker.Stop()
			return nil

		case <-ticker.C:
			ticker.Stop()
			glog.Info("Continue ...")
			continue
		}

		ticker.Stop()
		// Close connection and retry
		c.client.Close()
		c.client = nil
	}

	return err
}

func (c *RpcClient) connect() (err error) {

	if c.client != nil {
		return nil
	}

	for i := 0; i < 5; i++ {

		c.client, err = rpc.Dial("tcp", c.serverIP)
		if err == nil {
			return nil
		}

		if IsCancel() {
			return errors.New("Shutdown started.")
		}

		if prime.IsCommandMode() {
			return err
		}
		if i < 4 {
			time.Sleep(time.Duration((1<<i)*1000) * time.Millisecond)
		}
	}
	return err
}

func PingServers(cmd int, startid int, stopid int, addr ...string) bool {

	count := 0
	serverReady := true
	msg := make([]string, 0, 10)

	repKeys := int64(0)
	repErrors := int64(0)
	var cfg config.DbScan
	for ip, client := range remoteMap {

		if len(addr) > 0 && strings.Index(ip, addr[0]) != 0 {
			continue // prefix not matched
		}

		req := CmdRequest{
			Cmd:           cmd,
			IncExpireKeys: prime.IncludeExpireKeys(),
			ModTimeBegin:  prime.GetModTimeBegin(),
			RangeidBitlen: int(prime.GetRangeidBits()),
		}

		if cmd == cmdInit { // before run command
			req.StartShardid = startid
			req.StopShardid = stopid
			if prime.IsCopyNamespace() {
				req.NamespaceNames = prime.GetNamespaceNames()
				req.Trace = prime.GetTrace()
			}
		}

		var reply CmdReply

		err := client.Invoke("Remote.Ping", req, &reply)
		if err != nil {
			glog.Errorf("[ERROR] Ping server=%s err=%s", client.serverIP, err)
			if cmd == cmdInit {
				serverReady = false
			}
			continue
		}

		if cmd == cmdInit {
			if len(reply.Error) > 0 {
				glog.Errorf("[ERROR] %s is not idle.", client.serverIP)
				serverReady = false
			}
			if count == 0 {
				cfg = reply.Config
			} else {
				if cfg != reply.Config {
					glog.Errorf("[ERROR] DbScan config is out of sync at %s: cfg=%v",
						client.serverIP, reply.Config)
					serverReady = false
				}
			}
		}

		count++
		header := "Ping:"
		if cmd == cmdStop {
			header = "Stop:"
		}
		msg = append(msg, fmt.Sprintf("%s server=%s %s NumOpenDbs=%d replicateKeys=%d replicateErrors=%d\n",
			header, client.serverIP,
			reply.DbRange(), reply.NumOpenDbs, reply.ReplicateKeys, reply.ReplicateErrors))
		repKeys += reply.ReplicateKeys
		repErrors += reply.ReplicateErrors
	}

	for _, str := range msg {
		prime.LogMsg(str)
	}
	if repKeys > 0 {
		prime.LogMsg(fmt.Sprintf("Total: replicateKeys=%d replicateErrors=%d", repKeys, repErrors))
	}

	if cmd == cmdStatus || cmd == cmdStop {
		glog.Infof("Server ok Count=%d", count)
	}

	return serverReady
}

func GetVersionFromServers(key []byte, addr string, numZones int) {

	shardid := int(binary.BigEndian.Uint16(key[0:2]))

	for zoneid := 0; zoneid < numZones; zoneid++ {
		info, found := GetShardInfo(zoneid, shardid)

		if !found {
			continue
		}
		if strings.Index(info.serverAddr, addr) != 0 {
			continue // prefix not matched
		}

		req := CmdRequest{
			Cmd:          cmdGet,
			ServerZoneid: zoneid,
			Key:          key,
		}
		var reply CmdReply

		client := GetRpcClient(info.serverAddr)
		err := client.Invoke("Remote.Ping", req, &reply)
		if err != nil {
			glog.Errorf("[ERROR] Get: server=%s err=%s", client.serverIP, err)
			continue
		}

		if reply.Error == "no key" {
			glog.Infof("Get: server=%s error=no key", client.serverIP)
			continue
		}

		mt, ct, ver, et, md := prime.DecodeVal(reply.Val)
		E9 := uint64(time.Second)
		glog.Infof("Get: server=%s key=%v", client.serverIP, key)
		glog.Infof("     zoneid=%d error=%s"+
			"     md=%v mt=%d.%d ct=%d ver=%d et=%d et-ct=%d",
			zoneid, reply.Error,
			md, mt/E9, mt%E9, ct, ver, et, et-ct)
	}
}

func PingDrivers(cmd int, zoneid, startid, stopid int) {

	for _, client := range remoteMap {

		if (client.zoneidHash>>uint32(zoneid))&0x1 == 0 {
			continue
		}

		req := CmdRequest{
			Cmd:          cmd,
			ServerZoneid: zoneid,

			IncExpireKeys: prime.IncludeExpireKeys(),
			ProxyAddr:     prime.GetProxyAddr(),
			RangeidBitlen: int(prime.GetRangeidBits()),
			StartShardid:  startid,
			StopShardid:   stopid,
		}
		var reply CmdReply

		err := client.Invoke("Remote.Ping", req, &reply)
		if err != nil {
			glog.Errorf("[ERROR] cmd=%d server=%s err=%s",
				cmd, client.serverIP, err)
			continue
		}

		switch cmd {
		case cmdStatus:
			glog.Infof("Status: server=%s zoneid=%d status=%s",
				client.serverIP, zoneid, reply.Status)
		case cmdRun:
			glog.Infof("Run: server=%s zoneid=%d %s NumOpenDbs=%d",
				client.serverIP, zoneid,
				reply.DbRange(), reply.NumOpenDbs)
		}
	}
}

func IsValidAddr(addr string) bool {
	_, err := rpc.Dial("tcp", addr)
	if err != nil {
		glog.Errorf("[ERROR] Invalid addr: %s", err)
		return false
	}

	return true
}

func (c *RpcClient) callServer(req CmdNamespaceRequest, result chan bool) {

	var reply CmdNamespaceReply

	err := c.Timeout(600, 15).Invoke("Remote.NamespaceService", req, &reply)
	if err != nil {
		glog.Errorf("[ERROR] Server=%s err=%s", c.serverIP, err)
		result <- false
		return
	}

	if reply.Failed {
		glog.Errorf("[ERROR] Server=%s failed.", c.serverIP)
		result <- false
		return
	}

	result <- true
}

func LockSession() (bool, string) {
	for _, client := range remoteMap {

		if (client.zoneidHash & 0x1) == 0 {
			continue
		}

		req := CmdNamespaceRequest{
			Cmd:          cmdAcquireLock,
			ServerZoneid: 0,
		}

		var reply CmdNamespaceReply
		err := client.Invoke("Remote.NamespaceService", req, &reply)
		if err != nil {
			glog.Errorf("[ERROR] Server=%s err=%s", client.serverIP, err)
			return false, ""
		}

		if reply.Locked {
			glog.Errorf("[ERROR] Namespace deletion is still running. " +
				"No new session can be started.")
			return false, ""
		}
		if reply.Disabled {
			glog.Errorf("[ERROR] Namespace deletion is not enabled.")
			return false, ""
		}

		return true, client.serverIP
	}

	glog.Errorf("[ERROR] Namespace deletion failed to start.")
	return false, ""
}

func UnlockSession(addr string) {
	req := CmdNamespaceRequest{
		Cmd:          cmdReleaseLock,
		ServerZoneid: 0,
	}

	var reply CmdNamespaceReply
	c := GetRpcClient(addr)
	c.Invoke("Remote.NamespaceService", req, &reply)
}

func RefreshLock(addr string) {
	req := CmdNamespaceRequest{
		Cmd:          cmdRefreshLock,
		ServerZoneid: 0,
	}

	var reply CmdNamespaceReply
	c := GetRpcClient(addr)
	c.Invoke("Remote.NamespaceService", req, &reply)
}

func PingServersByZone(cmd int, zoneid int, addr string, ns []byte) bool {

	count := 0

	result := make(chan bool, 200)
	for _, client := range remoteMap {

		if ((client.zoneidHash >> zoneid) & 0x1) == 0 {
			continue // zoneid not matched
		}
		req := CmdNamespaceRequest{
			Cmd:             cmd,
			ServerZoneid:    zoneid,
			LockServerAddr:  addr,
			NamespaceConfig: ns,
		}

		go client.callServer(req, result)
		count++
	}

	ok := 0
	for i := 0; i < count; i++ {
		w := <-result
		if w {
			ok++
		}
	}

	glog.Infof("zone=%d success_count=%d failure_count=%d", zoneid, ok, count-ok)

	return (count == ok)
}
