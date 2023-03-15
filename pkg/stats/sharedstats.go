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

package stats

import (
	"fmt"
	"os"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/shm"
)

type (
	sharedRegionT struct {
		name   string
		offset int
		size   int
	}

	SharedStats struct {
		file        *os.File
		shmName     string
		size        int
		btypes      []byte
		reservation map[string]sharedRegionT
		initialized bool
		isOwner     bool
	}
)

func (s *SharedStats) Size() int {
	return s.size
}

func (s *SharedStats) initialize() {
	s.reservation = make(map[string]sharedRegionT)
	s.size = 0
	s.btypes = nil
	s.file = nil
	s.initialized = true
}

func (s *SharedStats) Reserve(name string, size int) error {
	if glog.LOG_DEBUG {
		glog.Debugf("reserve %d bytes for %s", size, name)
	}
	if !s.initialized {
		s.initialize()
	}
	if _, ok := s.reservation[name]; ok {
		return fmt.Errorf("%s already reserved", name)
	}
	s.reservation[name] = sharedRegionT{
		name: name, offset: s.size, size: size}
	size += (8 - size%8) % 8
	s.size += size
	return nil
}

func (s *SharedStats) GetMappedData() []byte {
	return s.btypes
}

func (s *SharedStats) Get(name string) ([]byte, error) {
	if name == "" {
		return s.btypes, nil
	}
	r, ok := s.reservation[name]
	if !ok {
		return nil, fmt.Errorf("%s region not found", name)
	}
	return s.btypes[r.offset : r.offset+r.size], nil
}

func (s *SharedStats) Create(name string, size int) error {
	s.initialize()
	flag := os.O_APPEND | os.O_CREATE | os.O_RDWR
	mode := os.FileMode(0644)
	if f, err := shm.Open(name, flag, mode); err == nil {
		s.file = f
		s.shmName = name
	} else {
		return err
	}
	s.initialized = true
	s.isOwner = true

	if err := shm.Ftruncate(s.file, size); err != nil {
		return err
	}
	s.size = size

	if b, err := shm.MmapForReadWrite(s.file, 0, int(s.size)); err == nil {
		s.btypes = b
	} else {
		return err
	}

	for i := 0; i < len(s.btypes); i++ {
		s.btypes[i] = 0
	}
	return nil
}

func (s *SharedStats) Open(name string) error {
	s.initialize()
	flag := os.O_RDWR
	mode := os.FileMode(0644)
	if f, err := shm.Open(name, flag, mode); err == nil {
		s.file = f
		s.shmName = name
	} else {
		return err
	}
	if fi, err := s.file.Stat(); err == nil {
		s.size = int(fi.Size())
	} else {
		return err
	}
	s.initialized = true
	if b, err := shm.MmapForReadWrite(s.file, 0, int(s.size)); err == nil {
		s.btypes = b
	} else {
		return err
	}
	return nil
}

func (s *SharedStats) OpenForRead(name string) error {
	s.initialize()
	if f, err := shm.Open(name, os.O_RDONLY, 0); err == nil {
		s.file = f
		s.shmName = name
	} else {
		return err
	}
	if fi, err := s.file.Stat(); err == nil {
		s.size = int(fi.Size())
	} else {
		return err
	}
	s.initialized = true
	if b, err := shm.MmapForRead(s.file, 0, int(s.size)); err == nil {
		s.btypes = b
	} else {
		return err
	}
	return nil
}

func (s *SharedStats) Finalize() {
	if s.file != nil {
		s.file.Close()
		s.file = nil
	}
	if s.btypes != nil {
		shm.Munmap(s.btypes)
		s.btypes = nil
	}
	if s.isOwner {
		shm.Close(s.shmName)
	}
}
