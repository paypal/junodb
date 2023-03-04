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
// +build darwin freebsd

package shm

import (
	"os"
	"syscall"
	"unsafe"
)

func Open(name string, flag int, mode os.FileMode) (file *os.File, err error) {
	var bname *byte
	if bname, err = syscall.BytePtrFromString(name); err != nil {
		return
	}
	fd, _, errno := syscall.Syscall(syscall.SYS_SHM_OPEN,
		uintptr(unsafe.Pointer(bname)),
		uintptr(flag), uintptr(mode),
	)
	if errno != 0 {
		err = errno
		return
	}
	file = os.NewFile(fd, name)
	return
}

func Close(name string) (err error) {
	var bname *byte
	if bname, err = syscall.BytePtrFromString(name); err != nil {
		return
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_SHM_UNLINK,
		uintptr(unsafe.Pointer(bname)), 0, 0,
	); errno != 0 {
		err = errno
		return
	}
	return
}
