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
	"fmt"
	"io"
	"os"

	uuid "github.com/satori/go.uuid"

	"juno/pkg/cmd"
	"juno/pkg/proto"
	"juno/pkg/proto/mayfly"
	"juno/pkg/util"
)

type (
	cmdInspUUIDT struct {
		cmd.Command
		rid proto.IRequestId
	}
	requestIdWrapperT struct {
		proto.IRequestId
	}
	mayflyRequestIdWrapperT struct {
		proto.IRequestId
	}
)

func (c *cmdInspUUIDT) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.SetSynopsis("<xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx>\n  ridinsp <<IP>-<PID>-<Timestamp>-<Sequence>>")
}

func (c *cmdInspUUIDT) Parse(args []string) (err error) {
	if err = c.FlagSet.Parse(args); err != nil {
		return
	}
	n := c.NArg()
	if n < 1 {
		err = fmt.Errorf("missing request ID string")
		return
	}
	if c.rid, err = newRequestIdFromString(c.Arg(0)); err != nil {
		fmt.Println(err)
		return
	}
	return
}

func (c *cmdInspUUIDT) Exec() {
	c.Validate()

	if c.rid != nil {
		c.rid.PrettyPrint(os.Stdout)
	}
}

func init() {
	c := &cmdInspUUIDT{}
	c.Init("ridinsp", "check RequestID, ...")

	cmd.Register(c)
}

func newRequestIdFromString(str string) (rid proto.IRequestId, err error) {
	var jrid proto.RequestId
	if err = jrid.SetFromString(str); err == nil {
		var uid uuid.UUID
		copy(uid[:], jrid.Bytes())
		if uid.Version() != 0 {
			rid = &requestIdWrapperT{
				&jrid,
			}
		} else {
			if mfrid, e := mayfly.NewRequestIdFromBytes(uid.Bytes()); e == nil {
				rid = &mayflyRequestIdWrapperT{
					mfrid,
				}
			}

		}
	} else {
		if mfrid, e := mayfly.NewRequestIdFromString(str); e == nil {
			rid = &mayflyRequestIdWrapperT{
				mfrid,
			}
			err = nil
		} else {
			err = fmt.Errorf("%s %s", err.Error(), e.Error())
		}
	}

	return
}

func (r *requestIdWrapperT) PrettyPrint(w io.Writer) {
	if r.IRequestId != nil {
		fmt.Fprintf(w, "* Type\n  UUID\n")
		var uid uuid.UUID
		copy(uid[:], r.IRequestId.Bytes())
		if ver := uid.Version(); ver == 0 {
		} else {
			fmt.Fprintf(w, "* Info\n")
			fmt.Fprintf(w, "  Verion   : %d\n", ver)
			fmt.Fprintf(w, "  Variant  : %d\n", uid.Variant())
			if ver == 1 {
				if tm, err := util.GetTimeFromUUIDv1(uid); err == nil {
					fmt.Fprintf(w, "  Timestamp: %d ns (%s)\n", tm.UnixNano(), tm.String())
				}
			}
		}
	}
}

func (r *mayflyRequestIdWrapperT) PrettyPrint(w io.Writer) {
	if r.IRequestId != nil {
		fmt.Fprintf(w, "* Type\n  Mayfly Request ID\n")
		fmt.Fprintf(w, "* String\n")
		var rid proto.RequestId
		if rid.SetFromBytes(r.IRequestId.Bytes()) == nil {
			fmt.Fprintf(w, "  UUID format  : %s\n", rid.String())
		}
		fmt.Fprintf(w, "  Mayfly format: %s\n", r.IRequestId.String())
		fmt.Fprintf(w, "* Info\n")
		r.IRequestId.PrettyPrint(w)
	}
}
