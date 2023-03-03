// +build debug

package db

import (
	"fmt"
	"io"
	"sync"
	"unsafe"

	"juno/third_party/forked/tecbot/gorocksdb"
)

var (
	gorocksdbSliceTracker sync.Map
)

func onAllocValue(i interface{}) {
	t := unsafe.Pointer((i.(*gorocksdb.Slice)))
	if _, loaded := gorocksdbSliceTracker.LoadOrStore(t, true); loaded {
		panic("")
	}
}

func onFreeValue(i interface{}) {
	t := unsafe.Pointer((i.(*gorocksdb.Slice)))
	gorocksdbSliceTracker.Delete(t)
}

func WriteSliceTrackerStats(w io.Writer) {
	num := 0

	gorocksdbSliceTracker.Range(func(key, value interface{}) bool {
		num++
		return true
	})
	fmt.Fprintf(w, "number of gorocksdb.Slice not being freed yet: %d", num)
}
