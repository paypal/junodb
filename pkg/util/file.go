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
  
package util

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"text/tabwriter"
)

func GetNumOpenFDs() (n int) {
	// alternatives on Unix/Linux:
	// * /proc/<pid>/fd
	// * lsof
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err == nil {
		for i := 0; i < int(rlim.Cur); i++ {
			var stat syscall.Stat_t
			if e := syscall.Fstat(i, &stat); e == nil {
				n++
			}
		}
	}
	return
}

func IsSocketFD(fd int) bool {
	if fd != -1 {
		var stat syscall.Stat_t
		if e := syscall.Fstat(fd, &stat); e == nil {
			if stat.Mode&syscall.S_IFSOCK != 0 {
				return true
			}
		}
	}
	return false
}

func IsSocket(f *os.File) bool {
	if f != nil {
		if st, err := f.Stat(); err == nil {
			if st.Mode()&os.ModeSocket != 0 {
				return true
			}
		}
	}
	return false
}

func WriteFileInfo(files []*os.File, w io.Writer) {
	wo := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	for _, f := range files {
		if st, err := f.Stat(); err == nil {
			fmt.Fprintf(w, "\t%s\t%s\n", f.Name(), st.Mode().String())
		}
	}
	wo.Flush()
}

func Lsof(w io.Writer) {
	if lsof, err := exec.Command("lsof", "-b", "-n", "-p", strconv.Itoa(os.Getpid())).Output(); err == nil {
		fmt.Fprintf(w, string(lsof))
	}
}
