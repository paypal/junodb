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

package ioutil

import (
	"io"
	"net"
	"os"
	"syscall"

	"juno/third_party/forked/golang/glog"
)

func LogError(err error) {
	if err == nil {
		return
	}

	if nerr, ok := err.(net.Error); ok {
		if nerr.Timeout() {
			glog.WarningDepth(1, err)
			return
		}
	}

	if opErr, ok := err.(*net.OpError); ok {
		if sErr, ok := opErr.Err.(*os.SyscallError); ok {
			if sErr.Err == syscall.ECONNRESET {
				glog.DebugDepth(1, err)
				return
			}
		}
		if opErr.Err.Error() == "use of closed network connection" {
			glog.DebugDepth(1, err)
			return
		}
	}

	if err == io.EOF {
		glog.DebugDepth(1, err)
	} else {
		glog.WarningDepth(1, err)
	}
}
