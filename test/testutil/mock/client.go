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
package mock

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"net"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/client"
	"juno/pkg/cluster"
	"juno/pkg/proto"
)

type MockClient struct {
	proxy     string
	namespace string
	client    client.IClient
	connInfo  [][]string
	conns     [][]net.Conn
	config    client.Config
}

func NewMockClient(ssconnInfo [][]string, conf client.Config) *MockClient {
	cli, err := client.New(conf)
	if err != nil {
		glog.Error(err)
		return nil
	}
	c := &MockClient{
		proxy:     conf.Server.Addr,
		namespace: conf.Namespace,
		client:    cli,
		connInfo:  ssconnInfo,
		conns:     nil,
		config:    conf,
	}
	c.conns = make([][]net.Conn, len(ssconnInfo))
	for i := range c.conns {
		c.conns[i] = make([]net.Conn, len(ssconnInfo[0]))
		for j := range c.conns[i] {
			c.conns[i][j] = nil
		}
	}
	return c
}

func (c *MockClient) ResetClient(ns string) {
	cli, err := client.New(c.config)
	if err == nil {
		c.client = cli
	} else {
		glog.Error(err)
	}
}

func (c *MockClient) Close() {
	for i := range c.conns {
		for j := range c.conns[i] {
			if c.conns[i][j] != nil {
				c.conns[i][j].Close()
			}
		}
	}
}

func (c *MockClient) ReSetMockInfo() {
	for i := range c.conns {
		for j := range c.conns[i] {
			if c.conns[i][j] != nil {
				c.reSetMockInfo(c.conns[i][j])
			}
		}
	}
}

func (c *MockClient) checkSSConnection() bool {

	// starting the connections to the downstream storage servers
	for i := range c.conns {
		for j := range c.conns[i] {
			if c.conns[i][j] == nil {
				var err error
				c.conns[i][j], err = net.Dial("tcp", c.connInfo[i][j])
				if err != nil {
					glog.Fatal("cannot connect: ", err)
					return false
				}
			}
		}
	}

	return true
}

func (c *MockClient) setMockParams(mp *MockParams, key []byte) {
	if mp != nil {
		c.checkSSConnection()
		// todo check mp.NumSS
		zones, nodes, _ := cluster.GetShardMgr().GetShardInfoByKey(key)
		glog.Info("zones are ", zones)
		glog.Info("nodes are ", nodes)

		for i := range zones {
			glog.Infof("SetMockParam : %s\n", c.connInfo[zones[i]][nodes[i]])
			glog.Info(mp.MockInfoList[i])
			conn := c.conns[zones[i]][nodes[i]]
			c.SetMockInfo(conn, &mp.MockInfoList[i])
		}
	}
}

func (c *MockClient) Create(key []byte, value []byte, ttl uint32, mp *MockParams) (context client.IContext, err error) {
	c.setMockParams(mp, key)

	return c.client.Create(key, value, client.WithTTL(ttl))
}

func (c *MockClient) Get(key []byte, mp *MockParams) (value []byte, context client.IContext, err error) {
	c.setMockParams(mp, key)
	return c.client.Get(key)
}

func (c *MockClient) Update(key []byte, value []byte, ttl uint32, mp *MockParams) (client.IContext, error) {
	c.setMockParams(mp, key)
	return c.client.Update(key, value, client.WithTTL(ttl))
}

func (c *MockClient) Set(key []byte, value []byte, ttl uint32, mp *MockParams) (client.IContext, error) {
	c.setMockParams(mp, key)
	return c.client.Set(key, value, client.WithTTL(ttl))
}

func (c *MockClient) Destroy(key []byte, mp *MockParams) error {
	c.setMockParams(mp, key)
	return c.client.Destroy(key)
}

func (c *MockClient) SetMockInfo(conn net.Conn, m *MockInfo) bool {

	// send OpCodeMockSetParam msg
	var value bytes.Buffer
	enc := gob.NewEncoder(&value) // Will write to value.
	if len(m.Namespace) == 0 {
		m.Namespace = c.namespace
	}
	err := enc.Encode(m)
	if err != nil {
		fmt.Println(err)
		return false
	}

	op := new(proto.OperationalMessage)
	var payload proto.Payload
	payload.SetWithClearValue(value.Bytes())
	op.SetRequest(proto.OpCodeCreate, []byte("fake"), []byte("fake"), &payload, 100)
	op.SetOpCode(proto.OpCodeMockSetParam)

	var raw proto.RawMessage

	err2 := op.Encode(&raw)

	if err2 != nil {
		glog.Infoln("Error: ", err2.Error())
		return false
	}
	raw.Write(conn)
	raw.ReleaseBuffer()
	decoder := proto.NewDecoder(conn)

	response := &proto.OperationalMessage{}

	err = decoder.Decode(response)

	if err != nil {
		glog.Infoln("error: ", err)
		return false
	}

	if response == nil {
		glog.Infoln("bad response")
		return false
	}

	//fmt.Println("Response:")
	//response.PrettyPrint()
	return true
}

func (c *MockClient) reSetMockInfo(conn net.Conn) bool {

	op := new(proto.OperationalMessage)
	var payload proto.Payload
	payload.SetWithClearValue([]byte("fake"))
	op.SetRequest(proto.OpCodeCreate, []byte("fake"), []byte("fake"), &payload, 100)
	op.SetOpCode(proto.OpCodeMockReSet)

	var raw proto.RawMessage
	err2 := op.Encode(&raw)

	if err2 != nil {
		glog.Infoln("Error: ", err2.Error())
		return false
	}
	raw.Write(conn)
	raw.ReleaseBuffer()
	decoder := proto.NewDecoder(conn)

	response := &proto.OperationalMessage{}

	err := decoder.Decode(response)

	if err != nil {
		glog.Infoln("error: ", err)
		return false
	}

	if response == nil {
		glog.Infoln("bad response")
		return false
	}

	//fmt.Println("Response:")
	//response.PrettyPrint()
	return true
}

func (c *MockClient) BatchCreate(keys [][]byte, value []byte, ttl uint32) []*proto.OperationalMessage {

	len := len(keys)
	msgs := make([]*proto.OperationalMessage, len)

	i := 0
	for _, key := range keys {
		request := c.NewRequest(proto.OpCodeCreate, key, value, ttl)
		request.SetCreationTime(uint32(time.Now().Unix()))
		msgs[i] = request
		i++
		//c.setMockParams(mp, key) // just once?
	}
	return msgs
}

func (c *MockClient) Batch(msgs []*proto.OperationalMessage, mp []*MockParams) (responses []*proto.OperationalMessage, err error) {
	glog.Verbosef("BatchSend")

	for i, msg := range msgs {
		c.setMockParams(mp[i], msgs[i].GetKey())
		var raw proto.RawMessage
		err = msg.Encode(&raw)
		if err != nil {
			return
		}
	}
	responses, err = c.Batch(msgs, mp)
	return
}

func (c *MockClient) NewRequest(op proto.OpCode, key []byte, value []byte, ttl uint32) (request *proto.OperationalMessage) {
	///TODO: validate op
	request = &proto.OperationalMessage{}
	var payload proto.Payload
	payload.SetWithClearValue(value)
	request.SetRequest(op, key, []byte(c.namespace), &payload, ttl)
	request.SetNewRequestID()
	return
}
