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
// +build linux

package shm

import (
	"fmt"
	"os"
	"path/filepath"
)

func shmName(name string) string {
	return filepath.Join("/dev/shm", name)
}

func Open(name string, flag int, mode os.FileMode) (file *os.File, err error) {
	file, err = os.OpenFile(shmName(name), flag, mode)
	return
}

func Close(name string) error {
	if err := os.Remove(shmName(name)); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
