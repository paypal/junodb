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
//Package shm implements functions for using shared memory
package shm

import (
	"fmt"
	"os"
	"syscall"
)

func Ftruncate(file *os.File, size int) (err error) {
	if file == nil {
		err = fmt.Errorf("nil file")
		return
	}
	err = syscall.Ftruncate(int(file.Fd()), int64(size))
	return
}

func MmapForReadWrite(file *os.File, offset int64, length int) (data []byte, err error) {
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flag := syscall.MAP_SHARED
	return Mmap(file, offset, length, prot, flag)
}

func MmapForRead(file *os.File, offset int64, length int) (data []byte, err error) {
	prot := syscall.PROT_READ
	flag := syscall.MAP_PRIVATE
	return Mmap(file, offset, length, prot, flag)
}

func Mmap(file *os.File, offset int64, length int, prot int, flags int) (data []byte, err error) {
	if file == nil {
		err = fmt.Errorf("nil file")
		return
	}
	return syscall.Mmap(int(file.Fd()), offset, length, prot, flags)
}

func Munmap(data []byte) error {
	return syscall.Munmap(data)
}
