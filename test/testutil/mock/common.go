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

package mock

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"testing"
	"time"

	. "github.com/paypal/junodb/pkg/proto"
)

const (
	DEF_OPCODE     = OpCodeNop
	DEF_STATUS     = uint8(OpStatusNoError)
	DEF_DELAY      = 0
	DEF_VER        = 1
	DEF_NORESPONSE = false
	DEF_SAVE       = false
	DEF_NS         = "ns"
)

// Mock specification for one storage server
// if opcode is set to OpCodeNop, it will apply to all opcode;
// otherwise apply to specific opcode
type MockInfo struct {
	Namespace    string
	Opcode       OpCode
	Status       uint8
	Delay        uint32
	Version      uint32
	NoResponse   bool
	Save         bool
	Value        []byte // expected value if set
	CreationTime uint32
}

func NewMockInfo() *MockInfo {
	info := &MockInfo{DEF_NS, DEF_OPCODE, DEF_STATUS, DEF_DELAY, DEF_VER, DEF_NORESPONSE, DEF_SAVE, nil, uint32(time.Now().Unix() - 10)}
	return info
}

func (m *MockInfo) GetStatusText() string {
	return OpStatus(m.Status).String()
}

func (m *MockInfo) ToString() string {
	noRespStr := ""
	if m.NoResponse {
		noRespStr = "no response"
	}
	return fmt.Sprintf("ns=%s,op=%s,st=%s,del=%d,ver=%d,ct=%d %s",
		m.Namespace, m.Opcode.String(), OpStatus(m.Status).String(), m.Delay, m.Version, m.CreationTime, noRespStr)
}

func (m *MockInfo) Encode() (*bytes.Buffer, error) {
	var value bytes.Buffer
	enc := gob.NewEncoder(&value) // Will write to value.
	err := enc.Encode(m)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return &value, nil
}

func (m *MockInfo) Decode(value *bytes.Buffer) error {
	*m = MockInfo{}

	decoder := gob.NewDecoder(value)
	err := decoder.Decode(m)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

// Mock spec for a group of storage servers (in different rack)
type MockParams struct {
	NumSS        uint8
	MockInfoList []MockInfo
}

func NewMockParams(numss uint8) *MockParams {
	p := &MockParams{numss, make([]MockInfo, numss)}
	ct := uint32(time.Now().Unix() - 10)
	for i := 0; i < int(numss); i++ {
		p.MockInfoList[i] = MockInfo{DEF_NS, DEF_OPCODE, DEF_STATUS, DEF_DELAY, DEF_VER, DEF_NORESPONSE, DEF_SAVE, nil, ct}
	}

	return p
}

func (m *MockParams) Log(t *testing.T) {
	t.Helper()
	info := "MockParams being set {\n"
	for i, _ := range m.MockInfoList {
		info += fmt.Sprintf("SS[%d] %s\n", i, m.MockInfoList[i].ToString())
	}
	info += "}\n"
	t.Log(info)
}

func (m *MockParams) Encode() (*bytes.Buffer, error) {
	var value bytes.Buffer
	enc := gob.NewEncoder(&value) // Will write to value.
	err := enc.Encode(m)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	return &value, nil
}

func (m *MockParams) Decode(value *bytes.Buffer) error {

	decoder := gob.NewDecoder(value)
	err := decoder.Decode(m)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}

func (m *MockParams) Reset(numss uint8) {
	m.NumSS = numss
	sz := len(m.MockInfoList)
	if sz < int(numss) {
		m.MockInfoList = make([]MockInfo, numss)
	} else if sz > int(numss) {
		m.MockInfoList = m.MockInfoList[0:numss]
	}
	ct := uint32(time.Now().Unix() - 10)
	for i := 0; i < int(numss); i++ {
		m.MockInfoList[i] = MockInfo{DEF_NS, DEF_OPCODE, DEF_STATUS, DEF_DELAY, DEF_VER, DEF_NORESPONSE, DEF_SAVE, nil, ct}
	}
}

func (m *MockParams) SetStatusForAll(status uint8) {
	for i := 0; i < int(m.NumSS); i++ {
		m.MockInfoList[i].Status = status
	}
}

func (m *MockParams) SetOpCodeForAll(code OpCode) {
	for i := 0; i < int(m.NumSS); i++ {
		m.MockInfoList[i].Opcode = code
	}
}

func (m *MockParams) SetValueForAll(value []byte) {
	for i := 0; i < int(m.NumSS); i++ {
		m.MockInfoList[i].Value = value
	}
}

func (m *MockParams) SetVersionForAll(version uint32) {
	for i := 0; i < int(m.NumSS); i++ {
		m.MockInfoList[i].Version = version
	}
}

func (m *MockParams) SetStatus(status ...uint8) {
	for i, st := range status {
		if i >= int(m.NumSS) {
			break
		}
		m.MockInfoList[i].Status = st
	}
}

func (m *MockParams) SetVersion(versions ...uint32) {
	for i, v := range versions {
		if i >= int(m.NumSS) {
			break
		}
		m.MockInfoList[i].Version = v
	}
}

func (m *MockParams) SetValue(values ...[]byte) {
	for i, _ := range values {
		if i >= int(m.NumSS) {
			break
		}
		m.MockInfoList[i].Value = values[i]
	}
}
