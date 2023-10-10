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
	"encoding/binary"
	"errors"
	"time"

	"juno/pkg/client"
	"juno/pkg/util"
	"juno/third_party/forked/golang/glog"
)

type Cmd struct {
	addr     string
	client   client.IClient
	payload  []byte
	ErrCount int
}

func createPayload() []byte {
	k := 1024
	p := make([]byte, k)

	for i := 0; i < k/2; i++ {
		val := byte((uint(i) * 12345678) & 0xff)
		p[i] = val
		p[i+k/2] = val
	}
	return p
}

func verifyPayload(p []byte) bool {
	off := len(p) / 2
	for k := 0; k < off; k++ {
		if p[k] != p[k+off] {
			return false
		}
	}
	return true
}

func showMetaInfo(ctx client.IContext) {
	glog.Infof("v=%d ct=%d ttl=%d", ctx.GetVersion(), ctx.GetCreationTime(), ctx.GetTimeToLive())
}

func NewCmdWithConfig(addr string, poolSize int) (*Cmd, error) {
	cfg := client.Config{
		Appname:           "testcli",
		Namespace:         "juno_cli_qa",
		DefaultTimeToLive: 60,
		ConnPoolSize:      poolSize,
		ResponseTimeout:   util.Duration{1000 * time.Millisecond},
	}

	cfg.Server.Addr = addr
	if addr == serverTls {
		cfg.Server.SSLEnabled = true
	}
	client, err := client.NewWithTLS(cfg, GetTLSConfig)
	if err != nil {
		glog.Errorf("%s", err)
		return nil, err
	}

	cmd := &Cmd{
		addr:    addr,
		client:  client,
		payload: createPayload(),
	}
	if !verifyPayload(cmd.payload) {
		cmd = nil
	}
	return cmd, err
}

func (c *Cmd) newRandomKey(s int) []byte {
	off := 2
	key := make([]byte, 16+off)
	r := uint32(((int64(s+1)*25214903917 + 11) >> 5) & 0x7fffffff)
	binary.BigEndian.PutUint32(key[0+off:], r)
	binary.BigEndian.PutUint32(key[4+off:], uint32(s))
	binary.BigEndian.PutUint32(key[12+off:], 0xff)

	return key
}

func (c *Cmd) createKey(ix int) (ctx client.IContext, err error) {
	key := c.newRandomKey(ix)

	for i := 0; i < 2; i++ {
		ctx, err = c.client.Create(key, c.payload, client.WithTTL(uint32(30)))
		if err == nil {
			break
		}
	}

	return ctx, err
}

func (c *Cmd) getKey(ix int) error {
	key := c.newRandomKey(ix)

	val, _, err := c.client.Get(key, client.WithTTL(uint32(30)))
	if err == nil && !verifyPayload(val) {
		glog.Errorf("validate failed")
		err = errors.New("Bad payload")
	}
	return err
}

func (c *Cmd) setKey(ix int) error {
	key := c.newRandomKey(ix)

	var err error
	var ctx client.IContext
	stop := 2
	for i := 0; i < stop; i++ {
		ctx, err = c.client.Set(key, c.payload, client.WithTTL(uint32(60)))
		if err == nil {
			showMetaInfo(ctx)
			break
		}
	}
	return err
}

func (c *Cmd) updateKey(ix int) (ctx client.IContext, err error) {
	key := c.newRandomKey(ix)

	for i := 0; i < 2; i++ {
		ctx, err = c.client.Update(key, c.payload, client.WithTTL(uint32(60)))
		if err == nil {
			break
		}
	}
	return ctx, err
}

func (c *Cmd) updateKeyWithCond(ix int, ctxIn client.IContext) (ctx client.IContext, err error) {
	key := c.newRandomKey(ix)

	for i := 0; i < 2; i++ {
		ctx, err = c.client.Update(key, c.payload, client.WithCond(ctxIn))
		if err == nil {
			break
		}
	}
	return ctx, err
}

func (c *Cmd) deleteKey(ix int) error {
	key := c.newRandomKey(ix)

	var err error
	for i := 0; i < 2; i++ {
		err = c.client.Destroy(key)
		if err == nil {
			break
		}
	}
	return err
}
