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
  
package insp

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"

	"juno/pkg/cmd"
	"juno/pkg/proto"
)

type cmdInspMsgT struct {
	cmd.Command
	msg []byte
}

func (c *cmdInspMsgT) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.SetSynopsis("<hex-string>")
}

func (c *cmdInspMsgT) Parse(args []string) (err error) {
	if err = c.FlagSet.Parse(args); err != nil {
		return
	}
	n := c.NArg()
	if n < 1 {
		err = fmt.Errorf("missing hex msg")
		return
	}
	if c.msg, err = hex.DecodeString(c.Arg(0)); err != nil {
		return
	}
	return
}

func (c *cmdInspMsgT) Exec() {
	c.Validate()

	buf := bytes.NewBuffer(c.msg)

	var raw proto.RawMessage
	if _, err := raw.Read(buf); err != nil {
		fmt.Println(err)
		return
	}
	raw.HexDump()
	var opMsg proto.OperationalMessage
	opMsg.Decode(&raw)
	opMsg.PrettyPrint(os.Stdout)
}

func init() {
	c := &cmdInspMsgT{}
	c.Init("inspect", "check juno binary message, ...")

	cmd.Register(c)
}
