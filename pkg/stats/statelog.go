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
	"math"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/proto"
)

const (
	KWriteIntervalSecond = 1
)

type (
	IState interface {
		Header() string
		FullHeader() string
		State() string
		CollectData()
		Width() int
	}

	StateBase struct {
		header     string
		fullHeader string
	}

	Uint16State struct {
		StateBase
		addr  *uint16
		value uint16
	}

	Uint32State struct {
		StateBase
		addr  *uint32
		value uint32
	}

	Uint64State struct {
		StateBase
		addr  *uint64
		value uint64
	}

	Uint64DeltaState struct {
		Uint64State
		lastValue  uint64
		resolution uint16
		count      uint16
	}

	Float32State struct {
		StateBase
		addr      *float32
		value     float32
		precision uint8
	}

	GenState struct {
		StateBase
		Value func() string
		width int
	}

	ProcStat struct {
		ProcTime          uint32 // in Microsecond
		RequestPayloadLen uint32
		RequestTimeToLive uint32
		Opcode            proto.OpCode
		ResponseStatus    proto.OpStatus

		// for now, we only need one category: <appName> + <namespace>
		// group key format: <appName> '.' <namespace>
		GroupKey  []byte
		appName   []byte
		namespace []byte
	}

	IStateHandler interface {
		ProcessStateChange(stat ProcStat)
		ProcessWrite(cnt int)
	}

	IStatesWriter interface {
		Write(now time.Time) error
		Close() error
	}

	StateLog struct {
		id         string
		logfile    string
		states     []IState
		writers    []IStatesWriter
		quitOnce   sync.Once
		chQuit     chan bool
		chProcStat chan ProcStat
		isLeader   bool
		handler    IStateHandler
	}
)

func (s *StateBase) FullHeader() string {
	return s.fullHeader
}

func (s *StateBase) Header() string {
	return s.header
}

func NewUint16State(addr *uint16, header string, fullHeader string) *Uint16State {
	return &Uint16State{
		StateBase: StateBase{
			header:     header,
			fullHeader: fullHeader,
		},
		addr: addr,
	}
}

func (s *Uint16State) State() string {
	//value := atomic.LoadUint16(s.addr)
	return fmt.Sprintf("%v", *s.addr)
}

func (s *Uint16State) CollectData() {
	s.value = *s.addr
}

func (s *Uint16State) Width() int {
	if len(s.header) > 5 {
		return len(s.header)
	} else {
		return 5
	}
}

func NewUint32State(addr *uint32, header string, fullHeader string) *Uint32State {
	return &Uint32State{
		StateBase: StateBase{
			header:     header,
			fullHeader: fullHeader,
		},
		addr: addr,
	}
}

func (s *Uint32State) State() string {
	value := atomic.LoadUint32(s.addr)
	return fmt.Sprintf("%v", value)
}

func (s *Uint32State) CollectData() {
	s.value = atomic.LoadUint32(s.addr)
}

func (s *Uint32State) Width() int {
	if len(s.header) > 8 {
		return len(s.header)
	} else {
		return 8
	}
}

func NewUint64DeltaState(addr *uint64, header string, fullHeader string,
	resolution uint16) *Uint64DeltaState {
	state := &Uint64DeltaState{
		Uint64State: Uint64State{
			StateBase: StateBase{
				header:     header,
				fullHeader: fullHeader,
			},
			addr: addr,
		},
		resolution: resolution,
		count:      0,
		lastValue:  0,
	}

	if resolution == 0 {
		state.resolution = 1
	}
	return state
}

func (s *Uint64DeltaState) State() string {
	curValue := atomic.LoadUint64(s.addr)
	value := curValue - s.lastValue
	if s.count%s.resolution == 0 {
		s.lastValue = curValue
	}
	s.count++
	return fmt.Sprintf("%v", value)
}

func (s *Uint64DeltaState) CollectData() {
	s.value = atomic.LoadUint64(s.addr)
}

func (s *Uint64DeltaState) Width() int {
	if len(s.header) > 5 {
		return len(s.header)
	} else {
		return 5
	}
}

func NewUint64State(addr *uint64, header string, fullHeader string) *Uint64State {
	return &Uint64State{
		StateBase: StateBase{
			header:     header,
			fullHeader: fullHeader,
		},
		addr: addr,
	}
}

func (s *Uint64State) State() string {
	return fmt.Sprintf("%v", *s.addr)
}

func (s *Uint64State) CollectData() {
	s.value = atomic.LoadUint64(s.addr)
}

func (s *Uint64State) Width() int {
	if len(s.header) > 8 {
		return len(s.header)
	} else {
		return 8
	}
}

func NewFloat32State(addr *float32, header string, fullHeader string, precision uint8) *Float32State {
	return &Float32State{
		StateBase: StateBase{
			header:     header,
			fullHeader: fullHeader,
		},
		addr:      addr,
		precision: precision,
	}
}

func (s *Float32State) State() string {
	return fmt.Sprintf(fmt.Sprintf("%%.%df", s.precision), *s.addr)
}

func (s *Float32State) CollectData() {
	s.value = math.Float32frombits(atomic.LoadUint32((*uint32)(unsafe.Pointer(s.addr))))
}

func (s *Float32State) Width() int {
	if len(s.header) > 8 {
		return len(s.header)
	} else {
		return 8
	}
}

func NewGenState(header string, fullHeader string, v func() string, width int) *GenState {
	st := &GenState{
		StateBase: StateBase{
			header:     header,
			fullHeader: fullHeader,
		},
		Value: v,
		width: width,
	}

	if len(st.header) > st.width {
		st.width = len(st.header)
	}

	return st
}

func (s *GenState) State() string {
	return s.Value()
}

func (s *GenState) CollectData() {
	// do nothing
}

func (s *GenState) Width() int {
	return s.width
}

func (l *StateLog) Init(id string, logfilepath string, isLeader bool,
	handler IStateHandler, states []IState) {
	l.id = id
	l.logfile = filepath.Join(logfilepath, "state.log")
	l.states = states
	l.isLeader = isLeader
	l.handler = handler
	l.chQuit = make(chan bool)
	l.chProcStat = make(chan ProcStat, 1000)
}

func (l *StateLog) AddStateWriter(w IStatesWriter) {
	l.writers = append(l.writers, w)
}

func (l *StateLog) GetStates() []IState {
	return l.states
}

func (l *StateLog) AddState(st IState) {
	l.states = append(l.states, st)
}

func (l *StateLog) AddStatePrepend(st IState) {
	l.states = append([]IState{st}, l.states...)
}

func (l *StateLog) SendProcState(st ProcStat) {
	select {
	case l.chProcStat <- st:
	default:
		// drop if the buffer full
	}
}

func (l *StateLog) IsLeader() bool {
	return l.isLeader
}

func (l *StateLog) Run() {
	go l.collect()
	go l.write()
}

func (l *StateLog) collect() {
	for {
		select {
		case <-l.chQuit:
			glog.Verbosef("statelog collector quite")
			return

		case stat := <-l.chProcStat:
			l.handler.ProcessStateChange(stat)
		}
	}
}

func (l *StateLog) write() {
	ticker := time.NewTicker(KWriteIntervalSecond * time.Second)
	defer func() {
		ticker.Stop()
		for _, w := range l.writers {
			w.Close()
		}
	}()

	cnt := 0
	for {
		select {
		case <-l.chQuit:
			glog.Verbosef("statelog writer quit")
			return

		case now := <-ticker.C:
			for _, i := range l.states {
				i.CollectData()
			}
			l.handler.ProcessWrite(cnt)
			//			l.WriteStates(now, file)
			for _, w := range l.writers {
				w.Write(now)
			}
			cnt++
		}
	}
}

func (l *StateLog) GetProcStatCH() chan ProcStat {
	return l.chProcStat
}

func (l *StateLog) Quit() {
	l.quitOnce.Do(func() {
		close(l.chQuit)
	})
}

func newGroupKey(appName []byte, namespace []byte) (key []byte, app []byte, ns []byte) {
	szNs := len(namespace)
	szApp := len(appName)
	szKey := szNs + szApp + 1
	key = make([]byte, szKey)
	copy(key[:szApp], appName)
	key[szApp] = '.'
	copy(key[szApp+1:], namespace)
	app = key[:szApp]
	ns = key[szApp+1:]
	return
}

func (st *ProcStat) Init(req *proto.OperationalMessage) {
	st.Opcode = req.GetOpCode()
	st.ProcTime = 0
	st.RequestPayloadLen = req.GetPayloadValueLength()
	st.RequestTimeToLive = req.GetTimeToLive()
	st.GroupKey, st.appName, st.namespace = newGroupKey(req.GetAppName(), req.GetNamespace())
}

func (st *ProcStat) OnComplete(rht uint32, respStatus proto.OpStatus) {
	st.ProcTime = rht
	st.ResponseStatus = respStatus
}
