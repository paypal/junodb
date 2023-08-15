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

// bulkload
//
// Tool to create a set of random keys, which can be accessed by
// [-get|-update|-set|-delete] in a subsequent command.
// =================================================================
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/paypal/junodb/pkg/client"
	"github.com/paypal/junodb/pkg/util"
)

type CmdLine struct {
	client    client.IClient
	clientCfg client.Config

	prefix     []byte
	payload    []byte
	payloadLen int
	ttl        int

	ErrCount   int
	DupCount   int
	NoKeyCount int
}

type Duration = util.Duration

func (c *CmdLine) newRandomKey(s int) []byte {
	off := len(c.prefix)

	key := make([]byte, 16+off)
	copy(key, c.prefix)
	r := uint32(((int64(s+1)*25214903917 + 11) >> 5) & 0x7fffffff)
	binary.BigEndian.PutUint32(key[0+off:], r)
	binary.BigEndian.PutUint32(key[4+off:], uint32(s))
	binary.BigEndian.PutUint32(key[12+off:], 0xff)

	return key
}

func (c *CmdLine) Init(server string, ns string, prefix string, payloadLen int, ttl int) {

	var err error
	c.clientCfg = client.Config{
		RetryCount:         1,
		DefaultTimeToLive:  ttl,
		ConnectTimeout:     Duration{500 * time.Millisecond},
		ReadTimeout:        Duration{500 * time.Millisecond},
		WriteTimeout:       Duration{500 * time.Millisecond},
		RequestTimeout:     Duration{500 * time.Millisecond},
		ConnRecycleTimeout: Duration{300 * time.Second},
	}
	c.clientCfg.Server.Addr = server
	c.clientCfg.Appname = "bulkload"
	c.clientCfg.Namespace = ns
	c.prefix = []byte(prefix)

	c.client, err = client.New(c.clientCfg)

	if err != nil {
		glog.Exitf("%s", err)
	}

	if payloadLen <= 0 {
		glog.Exitf("-len value must be greater than 0.")
	}

	if ttl <= 0 {
		glog.Exitf("-ttl value must be greater than 0.")
	}

	k := payloadLen * 15 / 10
	if k == 0 {
		k = 1
	}

	c.payloadLen = k
	c.payload = make([]byte, k)
	for i := 0; i < k; i++ {
		val := byte((int64(i)*1103515245 + 12345) & 0xff)
		c.payload[i] = val
	}
	c.ttl = ttl
}

func (c *CmdLine) Close() {
}

func (c *CmdLine) randLen() int {
	min := c.payloadLen / 3
	if min == 0 {
		return 1
	}
	len := min + rand.Intn(2*min)
	return len
}

func (c *CmdLine) addKey(ix int) bool {
	key := c.newRandomKey(ix)

	var err error
	len := c.randLen()
	for i := 0; i < 3; i++ {
		_, err = c.client.Create(key, c.payload[0:len], client.WithTTL(uint32(c.ttl)))
		if err == nil {
			return true
		}
		if strings.Contains(err.Error(), "unique key violation") {
			c.DupCount++
			break
		}
	}

	c.ErrCount++
	if c.ErrCount <= 5 {
		glog.Errorf("Add entry %d failed with %s", ix, err)
	}
	return false
}

func (c *CmdLine) getKey(ix int) bool {
	key := c.newRandomKey(ix)
	var err error

	for i := 0; i < 3; i++ {
		_, _, err = c.client.Get(key)
		if err == nil {
			return true
		}
		if strings.Contains(err.Error(), " no key") {
			c.NoKeyCount++
			break
		}
	}

	c.ErrCount++
	if c.ErrCount <= 5 {
		glog.Errorf("Get entry %d failed with %s", ix, err)
	}
	return false
}

func (c *CmdLine) delKey(ix int) bool {
	key := c.newRandomKey(ix)
	err := c.client.Destroy(key)
	if err == nil {
		return true
	}

	c.ErrCount++
	if c.ErrCount <= 100 {
		glog.Errorf("Delete entry %d failed with %s", ix, err)
	}
	return false
}

func (c *CmdLine) updateKey(ix int) bool {
	key := c.newRandomKey(ix)
	len := c.randLen()

	_, err := c.client.Update(key, c.payload[0:len], client.WithTTL(uint32(c.ttl)))
	if err == nil {
		return true
	}

	if strings.Contains(err.Error(), " no key") {
		c.NoKeyCount++
	}
	c.ErrCount++
	if c.ErrCount <= 100 {
		glog.Errorf("Update entry %d failed with %s", ix, err)
	}
	return false
}

func (c *CmdLine) setKey(ix int) bool {
	key := c.newRandomKey(ix)
	len := c.randLen()

	_, err := c.client.Set(key, c.payload[0:len], client.WithTTL(uint32(c.ttl)))
	if err == nil {
		return true
	}

	c.ErrCount++
	if c.ErrCount <= 100 {
		glog.Errorf("Set entry %d failed with %s", ix, err)
	}
	return false
}

func parseKeys(key string) (start int, last int) {
	var err error
	list := strings.Split(key, ",")
	start, err = strconv.Atoi(list[0])
	if err != nil {
		glog.Exitf("%s", err)
	}

	if len(list) < 2 {
		last = start + 1
	} else {
		last, err = strconv.Atoi(list[1])
		if err != nil {
			glog.Exitf("%s", err)
		}
	}
	if start < 0 || last < 0 {
		glog.Exitf("Negative range params are not allowed.")
	}
	return
}

func main() {
	defer glog.Finalize()

	var server string
	var payloadLen int
	var ttl int
	var namespace, prefix string
	var keyAdd, keyGet, keyDel, keyUpd, keySet string

	flag.StringVar(&server, "s", "", "proxy ip:port")
	flag.IntVar(&payloadLen, "len", 2000, "payload length")
	flag.IntVar(&ttl, "ttl", 1800, "record time to live")
	flag.StringVar(&namespace, "ns", "bulkload", "namespace")
	flag.StringVar(&prefix, "prefix", "", "prefix")
	flag.StringVar(&keyAdd, "create", "", "create keys for range: begin,end")
	flag.StringVar(&keyGet, "get", "", "get keys for range: begin,end")
	flag.StringVar(&keyDel, "delete", "", "delete keys for range: begin,end")
	flag.StringVar(&keyUpd, "update", "", "update keys for range: begin,end")
	flag.StringVar(&keySet, "set", "", "set keys for range: begin,end")

	flag.Parse()

	if len(server) == 0 {
		glog.Exitf("[ERROR] proxy ip:port is not set.")
	}

	var op, start, last int
	const (
		opCreate = 0
		opGet    = 1
		opDelete = 2
		opUpdate = 3
		opSet    = 4
	)
	op = -1
	if len(keyAdd) > 0 {
		op = opCreate
		start, last = parseKeys(keyAdd)
	}

	if len(keyGet) > 0 {
		if op >= 0 {
			glog.Exitf("Only one access operation is allowed.")
		}
		op = opGet
		start, last = parseKeys(keyGet)
	}

	if len(keyDel) > 0 {
		if op >= 0 {
			glog.Exitf("Only one access operation is allowed.")
		}
		op = opDelete
		start, last = parseKeys(keyDel)
	}

	if len(keyUpd) > 0 {
		if op >= 0 {
			glog.Exitf("Only one access operation is allowed.")
		}
		op = opUpdate
		start, last = parseKeys(keyUpd)
	}

	if len(keySet) > 0 {
		if op >= 0 {
			glog.Exitf("Only one access operation is allowed.")
		}
		op = opSet
		start, last = parseKeys(keySet)
	}

	if op < 0 {
		printUsage()
		return
	}

	var cmdline CmdLine
	cmdline.Init(server, namespace, prefix, payloadLen, ttl)

	switch op {
	case opCreate: // create
		count := 0
		for i := start; i < last; i++ {
			if cmdline.addKey(i) {
				count++
				cycle := 1000
				if count >= 10000 {
					cycle = 10000
				}
				if (count % cycle) == 0 {
					glog.Infof("create: okCount=%d, errCount=%d, dupKeys=%d.  Continue ...",
						count, cmdline.ErrCount, cmdline.DupCount)
				}
			}
			if (count % 1000) == 0 {
				time.Sleep(10 * time.Millisecond)
			}
		}
		glog.Infof("Total create: okCount=%d, errCount=%d, dupKeys=%d",
			count, cmdline.ErrCount, cmdline.DupCount)

	case opGet: // get
		count := 0
		for i := start; i < last; i++ {
			if !cmdline.getKey(i) {
				continue
			}

			count++
			cycle := 10000
			if (count % cycle) == 0 {
				glog.Infof("get: okCount=%d, errCount=%d, keyNotFound=%d.  Continue ...",
					count, cmdline.ErrCount, cmdline.NoKeyCount)
			}
		}
		glog.Infof("Total get: okCount=%d, errCount=%d, keyNotFound=%d",
			count, cmdline.ErrCount, cmdline.NoKeyCount)

	case opDelete: // delete
		count := 0
		for i := start; i < last; i++ {
			if !cmdline.delKey(i) {
				continue
			}

			count++
			cycle := 10000
			if (count % cycle) == 0 {
				glog.Infof("delete: okCount=%d, errCount=%d.  Continue ...",
					count, cmdline.ErrCount)
			}
		}
		glog.Infof("Total delete: okCount=%d, errCount=%d", count, cmdline.ErrCount)

	case opUpdate: // Update
		count := 0
		for i := start; i < last; i++ {
			if !cmdline.updateKey(i) {
				continue
			}

			count++
			cycle := 10000
			if (count % cycle) == 0 {
				glog.Infof("update: okCount=%d, errCount=%d, keyNotFound=%d.  Continue ...",
					count, cmdline.ErrCount, cmdline.NoKeyCount)
			}
		}
		glog.Infof("Total update: okCount=%d, errCount=%d, keyNotFound=%d",
			count, cmdline.ErrCount, cmdline.NoKeyCount)

	case opSet: // Set
		count := 0
		for i := start; i < last; i++ {
			if !cmdline.setKey(i) {
				continue
			}

			count++
			cycle := 10000
			if (count % cycle) == 0 {
				glog.Infof("set: okCount=%d, errCount=%d.  Continue ...",
					count, cmdline.ErrCount)
			}
		}
		glog.Infof("Total set: okCount=%d, errCount=%d", count, cmdline.ErrCount)

	default:
	}
	cmdline.Close()
}

func printUsage() {
	progName := filepath.Base(os.Args[0])
	fmt.Printf("\nExample 1: Use junoserver at <ip:port>.  Create 10000 random keys, which can be accessed by [-get|-update|-set|-delete] in a subsequent command.\n\n")
	fmt.Printf("./%s -s <ip:port> -ttl <ttl> -len <payload-len> -create 0,10000\n", progName)
	fmt.Printf("./%s -s <ip:port> -get    0,10000\n", progName)
	fmt.Printf("./%s -s <ip:port> -ttl <ttl> -len <payload-len> -update 0,10000\n", progName)
	fmt.Printf("./%s -s <ip:port> -ttl <ttl> -len <payload-len> -set    0,10000\n", progName)
	fmt.Printf("./%s -s <ip:port> -delete 0,10000\n", progName)

	fmt.Printf("\nExample 2: Create multiple sets of unique random keys, which can be accessed by [-get|-update|-set|-delete] in a subsequent command.\n\n")
	fmt.Printf("./%s -s <ip:port> -ttl <ttl> -len <payload-len> -create 0,10000\n", progName)
	fmt.Printf("./%s -s <ip:port> -ttl <ttl> -len <payload-len> -create 10000,15000\n", progName)
	fmt.Printf("./%s -s <ip:port> -ttl <ttl> -len <payload-len> -create 20000,30000\n", progName)
}
