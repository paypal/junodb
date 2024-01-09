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

package cli

import (
	"fmt"
	"math/rand"
	"os"
	"time"

	//	"juno/third_party/forked/golang/glog"
	uuid "github.com/satori/go.uuid"

	"juno/internal/cli"
	"juno/pkg/client"
	"juno/pkg/cluster"
	"juno/pkg/cmd"
	"juno/pkg/proto"
	"juno/pkg/util"
)

const (
	connectTimeout  = 100 * time.Millisecond
	responseTimeout = 1000 * time.Millisecond
)

type (
	requestIdOptionT           string
	originatorRequestIdOptionT string

	shardOptionsT struct {
		numShards  uint
		shardId    int
		algVersion uint
	}

	ssClientCommandT struct {
		clientCommandT
		shardOptionsT
	}

	ssClientCommandWithValueT struct {
		clientCommandWithValueT
		shardOptionsT
	}

	cmdPrepareCreateT struct {
		ssClientCommandWithValueT
		requestIdOptionT
	}

	cmdReadT struct {
		ssClientCommandT
		ttl uint
	}

	cmdPrepareUpdateT struct {
		ssClientCommandWithValueT
		requestIdOptionT
	}

	cmdPrepareSetT struct {
		ssClientCommandWithValueT
		requestIdOptionT
	}

	cmdPrepareDeleteT struct {
		ssClientCommandT
	}

	cmdDeleteT struct {
		ssClientCommandT
	}

	cmdCommitT struct {
		ssClientCommandT
		requestIdOptionT
		ttl     uint
		version uint
	}

	cmdAbortT struct {
		ssClientCommandT
		requestIdOptionT
	}

	cmdRepairT struct {
		ssClientCommandWithValueT
		version uint
	}

	cmdMarkDeleteT struct {
		ssClientCommandT
		requestIdOptionT
		originatorRequestIdOptionT
		version uint
		ttl     uint
	}

	cmdPopulateSST struct {
		cmd.Command
		client.Config
		shardOptionsT
		numRecords uint
		lenValue   uint
		ttl        uint
	}
)

func (s *requestIdOptionT) init(cmd *cmd.Command) {
	cmd.StringOption((*string)(s), "rid|request-id", "", "specify request id")
}

func (s *requestIdOptionT) setRequestId(req *proto.OperationalMessage) {
	str := *((*string)(s))
	if len(str) != 0 {
		var prid proto.RequestId
		if err := prid.SetFromString(str); err == nil {
			req.SetRequestID(prid)
			return
		}
	}
	req.SetNewRequestID()
}

func (s *originatorRequestIdOptionT) init(cmd *cmd.Command) {
	cmd.StringOption((*string)(s), "oid|originator-rid", "", "specify originator request id")
}

func (s *originatorRequestIdOptionT) setOriginatorRequestId(req *proto.OperationalMessage) {
	str := *((*string)(s))
	if len(str) != 0 {
		var prid proto.RequestId
		if err := prid.SetFromString(str); err == nil {
			req.SetOriginatorRequestID(prid)
			return
		}
	}
}

func (s *shardOptionsT) init(cmd *cmd.Command) {
	cmd.UintOption(&s.numShards, "num-shards", 1024, "specify number of shards")
	cmd.IntOption(&s.shardId, "id|shard-id", -1, "specify shard id")
	cmd.UintOption(&s.algVersion, "shard-algorithm-verion", 2, "specify sharding manager algorithm version")
}

func (s *shardOptionsT) getShardId(key []byte) uint16 {
	if s.shardId != -1 {
		return uint16(s.shardId)
	}
	if s.numShards == 0 {
		return 0
	}
	return util.GetPartitionId(key, uint32(s.numShards))
}

func newProcessor(cfg *client.Config) *cli.Processor {
	processor := cli.NewProcessor(cfg.Server, cfg.Appname,
		1, // connPoolSize
		cfg.ConnectTimeout.Duration,
		cfg.ResponseTimeout.Duration,
		nil) // GetTLSConfig
	processor.Start()
	return processor
}

func (c *ssClientCommandT) Init(name string, desc string) {
	c.clientCommandT.Init(name, desc)
	c.shardOptionsT.init(&c.Command)
}

func (c *ssClientCommandT) Parse(args []string) (err error) {
	err = c.clientCommandT.Parse(args)
	cluster.SetMappingAlg(uint32(c.algVersion))
	return
}

func (c *ssClientCommandWithValueT) Init(name string, desc string) {
	c.clientCommandWithValueT.Init(name, desc)
	c.shardOptionsT.init(&c.Command)
}

func (c *ssClientCommandWithValueT) Parse(args []string) (err error) {
	err = c.clientCommandWithValueT.Parse(args)
	cluster.SetMappingAlg(uint32(c.algVersion))
	return
}

func (c *cmdPrepareCreateT) Init(name string, desc string) {
	c.ssClientCommandWithValueT.Init(name, desc)
	c.requestIdOptionT.init(&c.Command)
}

func (c *cmdPrepareCreateT) Exec() {
	processor := newProcessor(&c.Config)

	req := newSSRequest(proto.OpCodePrepareCreate, []byte(c.Namespace), c.key, c.value, uint32(c.ttl))
	c.setRequestId(req)
	req.SetShardId(c.getShardId(c.key))
	if resp, err := processor.ProcessRequest(req); err == nil {
		resp.PrettyPrint(os.Stdout)
	} else {
	}
}

func (c *cmdReadT) Init(name string, desc string) {
	c.ssClientCommandT.Init(name, desc)
	c.UintOption(&c.ttl, "ttl", kDefaultTimeToLive, "specify TTL in second")
}

func (c *cmdReadT) Exec() {
	processor := newProcessor(&c.Config)
	req := newSSRequest(proto.OpCodeRead, []byte(c.Namespace), c.key, nil, uint32(c.ttl))
	req.SetNewRequestID()
	req.SetShardId(c.getShardId(c.key))
	if resp, err := processor.ProcessRequest(req); err == nil {
		resp.PrettyPrint(os.Stdout)
	} else {
	}
}

func (c *cmdPrepareUpdateT) Init(name string, desc string) {
	c.ssClientCommandWithValueT.Init(name, desc)
	c.requestIdOptionT.init(&c.Command)
}

func (c *cmdPrepareUpdateT) Exec() {
	processor := newProcessor(&c.Config)
	req := newSSRequest(proto.OpCodePrepareUpdate, []byte(c.Namespace), c.key, c.value, uint32(c.ttl))
	req.SetShardId(c.getShardId(c.key))
	c.setRequestId(req)
	if resp, err := processor.ProcessRequest(req); err == nil {
		resp.PrettyPrint(os.Stdout)
	} else {
	}
}

func (c *cmdPrepareSetT) Init(name string, desc string) {
	c.ssClientCommandWithValueT.Init(name, desc)
	c.requestIdOptionT.init(&c.Command)
}

func (c *cmdPrepareSetT) Exec() {
	processor := newProcessor(&c.Config)
	req := newSSRequest(proto.OpCodePrepareUpdate, []byte(c.Namespace), c.key, c.value, uint32(c.ttl))
	req.SetShardId(c.getShardId(c.key))
	c.setRequestId(req)
	if resp, err := processor.ProcessRequest(req); err == nil {
		resp.PrettyPrint(os.Stdout)
	} else {
	}
}

func (c *cmdPrepareDeleteT) Init(name string, desc string) {
	c.ssClientCommandT.Init(name, desc)
}

func (c *cmdPrepareDeleteT) Exec() {
	processor := newProcessor(&c.Config)
	req := newSSRequest(proto.OpCodePrepareDelete, []byte(c.Namespace), c.key, nil, 0)
	req.SetShardId(c.getShardId(c.key))
	req.SetNewRequestID()
	if resp, err := processor.ProcessRequest(req); err == nil {
		resp.PrettyPrint(os.Stdout)
	} else {
	}
}

func (c *cmdDeleteT) Init(name string, desc string) {
	c.ssClientCommandT.Init(name, desc)
}

func (c *cmdDeleteT) Exec() {
	processor := newProcessor(&c.Config)
	req := newSSRequest(proto.OpCodeDelete, []byte(c.Namespace), c.key, nil, 0)
	req.SetShardId(c.getShardId(c.key))
	req.SetNewRequestID()
	if resp, err := processor.ProcessRequest(req); err == nil {
		resp.PrettyPrint(os.Stdout)
	} else {
	}
}

func (c *cmdCommitT) Init(name string, desc string) {
	c.ssClientCommandT.Init(name, desc)
	c.UintOption(&c.ttl, "ttl", kDefaultTimeToLive, "specify TTL in second")
	c.UintOption(&c.version, "v|version", 1, "specify the record version")
	c.requestIdOptionT.init(&c.Command)
}

func (c *cmdCommitT) Exec() {
	processor := newProcessor(&c.Config)
	req := newSSRequest(proto.OpCodeCommit, []byte(c.Namespace), c.key, nil, uint32(c.ttl))
	req.SetVersion(uint32(c.version))
	req.SetCreationTime(uint32(time.Now().Unix() - 5))
	req.SetShardId(c.getShardId(c.key))
	c.setRequestId(req)
	if resp, err := processor.ProcessRequest(req); err == nil {
		resp.PrettyPrint(os.Stdout)
	} else {
	}
}

func (c *cmdAbortT) Init(name string, desc string) {
	c.ssClientCommandT.Init(name, desc)
	c.requestIdOptionT.init(&c.Command)
}

func (c *cmdAbortT) Exec() {
	processor := newProcessor(&c.Config)
	req := newSSRequest(proto.OpCodeCommit, []byte(c.Namespace), c.key, nil, 0)
	req.SetShardId(c.getShardId(c.key))
	c.setRequestId(req)
	if resp, err := processor.ProcessRequest(req); err == nil {
		resp.PrettyPrint(os.Stdout)
	} else {
	}
}

func (c *cmdRepairT) Init(name string, desc string) {
	c.ssClientCommandWithValueT.Init(name, desc)
	c.UintOption(&c.version, "v|version", 1, "specify the record version")
}

func (c *cmdRepairT) Exec() {
	processor := newProcessor(&c.Config)
	req := newSSRequest(proto.OpCodeRepair, []byte(c.Namespace), c.key, nil, uint32(c.ttl))
	req.SetVersion(uint32(c.version))
	req.SetCreationTime(uint32(time.Now().Unix() - 5))
	req.SetShardId(c.getShardId(c.key))
	req.SetNewRequestID()
	if len(c.value) != 0 {
		var payload proto.Payload
		payload.SetWithClearValue(c.value)
		req.SetPayload(&payload)
	}
	if resp, err := processor.ProcessRequest(req); err == nil {
		resp.PrettyPrint(os.Stdout)
	} else {
	}
}

func (c *cmdMarkDeleteT) Init(name string, desc string) {
	c.ssClientCommandT.Init(name, desc)
	c.UintOption(&c.version, "v|version", 1, "specify the record version")
	c.UintOption(&c.ttl, "ttl", 0, "specify TTL in second")
	c.requestIdOptionT.init(&c.Command)
	c.originatorRequestIdOptionT.init(&c.Command)
}

func (c *cmdMarkDeleteT) Exec() {
	processor := newProcessor(&c.Config)
	req := newSSRequest(proto.OpCodeMarkDelete, []byte(c.Namespace), c.key, nil, uint32(c.ttl))
	req.SetVersion(uint32(c.version))
	req.SetCreationTime(uint32(time.Now().Unix() - 5))
	req.SetShardId(c.getShardId(c.key))
	req.SetNewRequestID()
	c.setOriginatorRequestId(req)
	if resp, err := processor.ProcessRequest(req); err == nil {
		resp.PrettyPrint(os.Stdout)
	} else {
	}
}

func (c *cmdPopulateSST) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.Config = defaultConfig
	c.StringOption(&c.Server.Addr, "s|server", "127.0.0.1:8080", "specify server address")
	c.UintOption(&c.numRecords, "n|num-record", 100, "specify the number of records to be created")
	c.UintOption(&c.lenValue, "l|value-len", 1024, "specify the value length")
	c.StringOption(&c.Namespace, "ns|namespace", "namespace", "specify namespace")
	c.UintOption(&c.ttl, "ttl", kDefaultTimeToLive, "specify TTL in second")
	c.shardOptionsT.init(&c.Command)
}

func (c *cmdPopulateSST) Exec() {
	if c.numRecords != 0 {
		cluster.SetMappingAlg(uint32(c.algVersion))
		processor := newProcessor(&c.Config)
		fmt.Printf("  populating %d record(s)...\n", c.numRecords)
		var numFailed uint
		rand.Seed(time.Now().Unix())
		value := make([]byte, c.lenValue, c.lenValue)
		for i := 0; i < int(c.lenValue); i++ {
			value[i] = byte(rand.Intn(255))
		}
		for i := uint(0); i < c.numRecords; i++ {
			key := uuid.NewV4().Bytes()
			req := newSSRequest(proto.OpCodeRepair, []byte(c.Namespace), key, value, uint32(c.ttl))
			req.SetVersion(1)
			req.SetCreationTime(uint32(time.Now().Unix() - 5))
			req.SetShardId(c.getShardId(key))
			req.SetNewRequestID()
			if _, err := processor.ProcessRequest(req); err == nil {
				//		resp.PrettyPrint(os.Stdout)
			} else {
				numFailed++
			}

		}
		fmt.Printf("  * %d successful, %d failed\n", c.numRecords-numFailed, numFailed)
	}

}
func newSSRequest(op proto.OpCode, namespace []byte, key []byte, value []byte, ttl uint32) (request *proto.OperationalMessage) {
	request = &proto.OperationalMessage{}
	var payload proto.Payload
	payload.SetWithClearValue(value)
	request.SetRequest(op, key, namespace, &payload, ttl)
	return
}
