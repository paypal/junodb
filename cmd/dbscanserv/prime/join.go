package prime

import (
	"bytes"
	"math/rand"
	"sort"
	"sync/atomic"
	"time"
)

type Entry struct {
	zoneid int

	// val is packed with
	// LastModificationTime uint64
	// CreationTime         uint32
	// Version              uint32
	// ExpirationTime       uint32
	// MarkedDelete         byte
	val []byte
}

// Ordered list of entries for all zones
type OrderedList []Entry

// Map from key to OrderedList
type JoinMap struct {
	numDisplays int
	lookup      map[string]OrderedList
}

var (
	minJoinMapSize int32 = 1000
	displayCount         = 0
)

func NewJoinMap() *JoinMap {
	minSize := atomic.LoadInt32(&minJoinMapSize)
	m := &JoinMap{
		lookup: make(map[string]OrderedList, minSize*12/10),
	}
	return m
}

// Defined for sorting.
func (s OrderedList) Len() int {
	return len(s)
}

func (s OrderedList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

// Sort in decreasing order
func (s OrderedList) Less(i, j int) bool {
	r := bytes.Compare(s[i].val, s[j].val)
	if r == 0 {
		return rand.Intn(100) > 45
	}
	return r > 0
}

func (m *JoinMap) IsEmpty() bool {
	return len(m.lookup) == 0
}

// Add an entry to the map.
func (m *JoinMap) Insert(zoneid int, key, val []byte) {

	k := string(key)
	list, found := m.lookup[k]

	if !found {
		list = make([]Entry, 0, 5)
		m.lookup[k] = list
	}

	entry := Entry{
		zoneid: zoneid,
		val:    NewValBuffer(),
	}
	copy(entry.val, val)

	list = append(list, entry)
	m.lookup[k] = list
}

func (m *JoinMap) Filter(numZones, rangeid int, result *Result) {

	last := (numZones >> 1)

	modTimeEnd = GetModTimeEnd()
	if IsCopyNamespace() {
		if result.KeysByZone == nil {
			result.KeysByZone = make([]KeyList, numZones)
		}
		for i := 0; i < numZones; i++ {
			result.KeysByZone[i].Rangeid = rangeid
		}
	}

	for key, list := range m.lookup {

		sort.Sort(OrderedList(list))
		if IsCopyNamespace() {
			if !IsMarkDelete(list[0].val) {
				result.AppendKey(key, list[0].zoneid)
			} // exclude markdelete
			continue
		}
		if len(list) > last &&
			IsMarkDelete(list[0].val) &&
			IsMarkDelete(list[last].val) &&
			(last <= 1 || IsMarkDelete(list[1].val)) {
			continue
		}

		if len(list) <= last ||
			FuzzyCompare(list[0].val, list[last].val) != 0 {
			// Less than majority copies are present or consistent

			if IsModTimeInRange(list[0].val) {
				m.displayEntry(key, list)
				result.AppendData([]byte(key), list[0].val)
			}
		}
	}

	result.TotalKeys = len(m.lookup)
	if IsCopyNamespace() {
		result.TotalKeys = result.CountKeys()
	}
	minSize := atomic.LoadInt32(&minJoinMapSize)

	if len(m.lookup) > int(minSize) {
		atomic.StoreInt32(&minJoinMapSize, int32(len(m.lookup)))
	}
}

func (m *JoinMap) displayEntry(key string, list []Entry) {

	if m.numDisplays >= 100 {
		return
	}
	m.numDisplays++

	const maxCount = 10000
	if displayCount >= maxCount {
		if displayCount == maxCount {
			LogMsg(">> ... truncated ...")
			displayCount++
		}
		return
	}
	displayCount++

	LogMsg(">> fail key=%v", []byte(key))

	const E9 = uint64(time.Second)
	for i := range list {
		mt, ct, ver, et, md := DecodeVal(list[i].val)

		LogMsg("        i=%d zoneid=%d md=%d mt=%d.%d ct=%d ver=%d et=%d et-ct=%d",
			i, list[i].zoneid, md, mt/E9, mt%E9, ct, ver, et, et-ct)
	}
}

func ResetDisplayCount() {
	displayCount = 0
}
