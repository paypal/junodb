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
  
package prime

import (
	"bytes"
	"encoding/binary"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/storage/db"
)

type Meta struct {
	Key []byte

	// Val is packed with
	// LastModificationTime uint64
	// CreationTime         uint32
	// Version              uint32
	// ExpirationTime       uint32
	// MarkedDelete         byte
	Val []byte
}

type MessageBlock struct {
	Shardid int
	Rangeid int
	// total keys in this shard
	TotalKeys int

	Data []Meta
	// unexported field.  Not over network.
	opData OpData
}

const (
	mtOff  = 0
	ctOff  = 8
	verOff = 12
	etOff  = 16
	mdOff  = 20
	vallen = 21
)

var (
	rangeidBitlen uint = 6
)

var (
	modTimeBegin     int64  = 0
	modTimeEnd       int64  = 0
	expireBeforeTime uint32 = 0
)

var (
	pool = sync.Pool{
		New: func() interface{} {
			return &MessageBlock{}
		},
	}
)

func NewValBuffer() []byte {
	return make([]byte, vallen)
}

// Get one from pool.
func NewMessageBlock(shardid, rangeid, totalKeys int) (mb *MessageBlock) {

	if rangeid < 0 || rangeid >= GetRangeCount() {
		mb = &MessageBlock{} // for tagging only
	} else {
		mb = pool.Get().(*MessageBlock)
	}

	mb.Shardid = shardid
	mb.Rangeid = rangeid
	mb.TotalKeys = totalKeys
	mb.opData.Rangeid = rangeid
	mb.opData.recordList = nil

	return mb
}

func (m *MessageBlock) NumKeysThisBlock() int {
	return len(m.Data)
}

func (m *MessageBlock) IsEmpty() bool {
	return len(m.Data) == 0
}

func (m *MessageBlock) NotEmpty() bool {
	return len(m.Data) > 0
}

// Put back to pool.
func (m *MessageBlock) Release() {

	if m.Rangeid < 0 || m.Rangeid >= GetRangeCount() {
		return
	}

	// Clear fields
	m.Shardid = 0
	m.Rangeid = 0
	m.TotalKeys = 0

	for _, meta := range m.Data {
		// Reset the len and keep the capacity
		meta.Key = meta.Key[:0]
	}

	// Reset the len and keep the capacity
	m.Data = m.Data[:0]

	m.opData.Rangeid = 0
	m.opData.recordList = nil
	pool.Put(m)
}

func (m *MessageBlock) AppendData(key []byte, rec *db.Record, nsCopy bool) {

	if rec.ExpirationTime < expireBeforeTime ||
		rec.LastModificationTime < uint64(GetModTimeBegin()) {
		// skip
		return
	}

	size := len(m.Data)
	if size == cap(m.Data) {
		// Add a new entry
		meta := &Meta{
			Key: make([]byte, len(key), len(key)+100),
			Val: NewValBuffer(),
		}
		m.Data = append(m.Data, *meta)
	} else { // resize by adding one
		m.Data = m.Data[:size+1]
	}

	k := len(m.Data) - 1
	p := &m.Data[k]
	if cap(p.Key) < len(key) { // not big enough
		p.Key = make([]byte, len(key), len(key)+100)
	} else { // reset the len
		p.Key = p.Key[:len(key)]
	}

	if cap(p.Val) < vallen {
		p.Val = NewValBuffer()
	} else {
		p.Val = p.Val[:vallen]
	}

	copy(m.Data[k].Key, key)
	EncodeVal(m.Data[k].Val, rec)

	// Build operatioal message for namespace migration
	if nsCopy {
		m.opData.Insert(key, rec)
	}
}

func EncodeVal(val []byte, rec *db.Record) {
	binary.BigEndian.PutUint64(val[mtOff:ctOff], rec.LastModificationTime)
	binary.BigEndian.PutUint32(val[ctOff:verOff], rec.CreationTime)
	binary.BigEndian.PutUint32(val[verOff:etOff], rec.Version)
	binary.BigEndian.PutUint32(val[etOff:mdOff], rec.ExpirationTime)
	val[mdOff] = 0
	if rec.IsMarkedDelete() {
		val[mdOff] = 1
	}
}

func DecodeVal(val []byte) (mt uint64, ct, ver, et, md uint32) {
	mt = binary.BigEndian.Uint64(val[mtOff:ctOff])
	ct = binary.BigEndian.Uint32(val[ctOff:verOff])
	ver = binary.BigEndian.Uint32(val[verOff:etOff])
	et = binary.BigEndian.Uint32(val[etOff:mdOff])
	md = uint32(val[mdOff])
	return
}

func FuzzyCompare(x []byte, y []byte) int {
	rt := bytes.Compare(x, y)

	if rt > 0 && bytes.Compare(x[:etOff], y[:etOff]) == 0 {
		et1 := binary.BigEndian.Uint32(x[etOff:mdOff])
		et2 := binary.BigEndian.Uint32(y[etOff:mdOff])
		if et1 == et2+1 {
			// Expiration times differ by one sec.
			return 0
		}
	}

	return rt
}

func SetRangeidBits(n int) {
	rangeidBitlen = uint(n)
}

func GetRangeidBits() uint {
	return rangeidBitlen
}

func GetRangeCount() int {
	return 1 << rangeidBitlen
}

func GetRangeidFromKey(key []byte) int {
	return int(key[2]) >> (8 - rangeidBitlen)
}

func SetBeginEndTimes(incExpireKeys bool, t int64, nsCopy bool) {
	if nsCopy {
		if incExpireKeys {
			atomic.StoreUint32(&expireBeforeTime, 0)
		} else {
			t := uint32(time.Now().Unix()) + 3600*3
			atomic.StoreUint32(&expireBeforeTime, t)
		}
		LogMsg("expireBeforeTime=%d\n", expireBeforeTime)
		return
	}
	if incExpireKeys {
		atomic.StoreUint32(&expireBeforeTime, 0)
		atomic.StoreInt64(&modTimeEnd, 0)
	} else {
		SetExpireBeforeTime(false, false)
		SetModTimeEnd()
	}

	atomic.StoreInt64(&modTimeBegin, t)

	LogMsg("modTimeBegin=%d modTimeEnd=%d expireBeforeTime=%d\n",
		modTimeBegin, modTimeEnd, expireBeforeTime)
}

func GetModTimeBegin() int64 {
	return atomic.LoadInt64(&modTimeBegin)
}

func AddModTimeBegin(priorDate string) {
	list := strings.Split(priorDate, "-")
	msg := "[ERROR] Parameter -pd has invalid format. " +
		"YYYY-MM-DD is expected."
	if len(list) < 3 {
		glog.Exitf(msg)
	}

	var y, m, d int
	var mo time.Month
	var err error
	if y, err = strconv.Atoi(list[0]); err != nil {
		glog.Exitf(msg)
	}
	if m, err = strconv.Atoi(list[1]); err != nil {
		glog.Exitf(msg)
	}
	if d, err = strconv.Atoi(list[2]); err != nil {
		glog.Exitf(msg)
	}
	t := time.Date(y, time.Month(m), d, 0, 0, 0, 0, time.Local)

	// Midnight 3 days ago
	y, mo, d = t.AddDate(0, 0, -3).Date()
	t = time.Date(y, mo, d, 0, 0, 0, 0, time.Local)

	atomic.StoreInt64(&modTimeBegin, t.Unix())
}

func GetModTimeEnd() int64 {
	return atomic.LoadInt64(&modTimeEnd)
}

// Modified after this are skipped.
func SetModTimeEnd() {
	// Midnight 2 days ago.
	y, m, d := time.Now().AddDate(0, 0, -2).Date()
	t := time.Date(y, m, d, 0, 0, 0, 0, time.Local)
	atomic.StoreInt64(&modTimeEnd, t.Unix())
}

func IsModTimeInRange(val []byte) bool {
	if modTimeEnd <= 0 {
		return true
	}

	t := binary.BigEndian.Uint64(val[mtOff:ctOff]) / uint64(time.Second)
	return int64(t) <= modTimeEnd
}

// Keys expired before this are skipped.
// Keys modified after modTimeEnd are skipped.
func SetExpireBeforeTime(incExpired, nsCopy bool) {
	if nsCopy {
		if incExpired {
			atomic.StoreUint32(&expireBeforeTime, 0)
		} else {
			t := uint32(time.Now().Unix()) + 3600*3
			atomic.StoreUint32(&expireBeforeTime, t)
		}
		return
	}
	// Midnight in 2 days.
	y, m, d := time.Now().AddDate(0, 0, 2).Date()
	t := time.Date(y, m, d, 0, 0, 0, 0, time.Local)
	atomic.StoreUint32(&expireBeforeTime, uint32(t.Unix()))

	SetModTimeEnd()
}

func IncludeExpireKeys() bool {
	return expireBeforeTime == 0
}

func IsMarkDelete(val []byte) bool {
	return val[mdOff]&0x1 != 0
}
