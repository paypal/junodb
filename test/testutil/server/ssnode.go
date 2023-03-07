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
	"net"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/client"
	"juno/test/testutil/ssclient"
)

type SSNode struct {
	Server    IServer
	Zone      int
	Node      int
	cluster   *Cluster
	NumShards uint32
}

func (n *SSNode) Get(namespace string, key []byte) (value []byte, recInfo client.IContext, err error) {
	addr := net.JoinHostPort(n.Server.IPAddress(), n.Server.Port())
	c := ssclient.NewSSClientWithNumShards(addr, namespace, n.NumShards)
	return c.Get(key)
}

func (n *SSNode) Read(namespace string, key []byte) (rec *ssclient.Record, err error) {
	addr := net.JoinHostPort(n.Server.IPAddress(), n.Server.Port())
	c := ssclient.NewSSClientWithNumShards(addr, namespace, n.NumShards)
	return c.Read(key)
}

func (n *SSNode) Store(namespace string, key []byte, rec *ssclient.Record) (err error) {
	addr := net.JoinHostPort(n.Server.IPAddress(), n.Server.Port())
	c := ssclient.NewSSClientWithNumShards(addr, namespace, n.NumShards)
	return c.Store(key, rec)
}

func (n *SSNode) MarkDelete(namespace string, key []byte, rec *ssclient.Record) (err error) {
	addr := net.JoinHostPort(n.Server.IPAddress(), n.Server.Port())
	c := ssclient.NewSSClientWithNumShards(addr, namespace, n.NumShards)
	return c.MarkDelete(key, rec)
}

func (n *SSNode) Delete(namespace string, key []byte) (err error) {
	addr := net.JoinHostPort(n.Server.IPAddress(), n.Server.Port())
	c := ssclient.NewSSClientWithNumShards(addr, namespace, n.NumShards)
	return c.Delete(key)
}

//func (n *SSNode) Start() {
//	if n.Server != nil {
//		n.Server.Start()
//	}
//}
//
//func (n *SSNode) Stop() {
//	if n.Server != nil {
//		n.Server.Stop()
//	}
//}
//
//func (n *SSNode) Restart() {
//	if n.Server != nil {
//		n.Server.Stop()
//		n.Server.Start()
//	}
//}

func (n *SSNode) String() string {
	if n.Server == nil {
		glog.Error("SSNode.Server is null")
		return fmt.Sprintf("SS[%d][%d]", n.Zone, n.Node)
	}
	return fmt.Sprintf("ss[%d][%d] %s", n.Zone, n.Node, n.Server.String())
}
