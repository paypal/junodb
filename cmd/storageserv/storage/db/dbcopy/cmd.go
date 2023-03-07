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
  
package main

import (
	"time"

	"juno/third_party/forked/golang/glog"
)

type CmdLine struct {
	dbclient  *DbClient
	tgtclient *DbClient

	prefixLen int
}

func (c *CmdLine) NewDbClient(dbpath string, prefixLen int, compact bool) error {

	instance, err := NewDbInstance(dbpath, prefixLen, compact)
	if err != nil {
		return err
	}

	c.dbclient = &DbClient{
		path:      dbpath,
		prefixLen: prefixLen,
		db:        instance,
	}

	return nil
}

func (c *CmdLine) Init(dbpath string, prefixLen int, compact bool) (err error) {

	c.prefixLen = prefixLen
	err = c.NewDbClient(dbpath, prefixLen, compact)

	return err
}

func (c *CmdLine) Close() {

	if c.dbclient == nil {
		return
	}
	c.dbclient.DisplayStats()
	c.dbclient.Close()
}

func (c *CmdLine) ScanShards(start int, stop int, tgtPath string) (err error) {

	err = c.dbclient.ScanByShard(start, stop, tgtPath)

	return err
}

func (c *CmdLine) CompactDb(start int, stop int) error {

	glog.Infof("Compact started.")
	c.dbclient.DisplayStats()
	return c.dbclient.CompactRange(start, stop)
}

func (c *CmdLine) Work(start int, stop int, tgtPath string, compact bool) (err error) {

	if compact {
		err = c.CompactDb(start, stop)
	} else {
		err = c.ScanShards(start, stop, tgtPath)
	}
	time.Sleep(1 * time.Second)

	c.Close()

	return err
}
