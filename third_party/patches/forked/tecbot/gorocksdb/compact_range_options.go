package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import "errors"
import "unsafe"

type CompactOptions struct {
	c *C.rocksdb_compactoptions_t
}

// NewDefaultCompactOptions creates a default CompactOptions object.
func NewDefaultCompactOptions() *CompactOptions {
	return &CompactOptions{C.rocksdb_compactoptions_create()}
}

func (opts *CompactOptions) SetExclusiveManual(value bool) {
	C.rocksdb_compactoptions_set_exclusive_manual_compaction(opts.c, boolToChar(value))
}

// CompactRangeOptions runs a manual compaction on a Range of keys with options.
func (db *DB) CompactRangeOptions(opts *CompactOptions, r Range) error {
	var (
		cErr *C.char
	)
	cStart := byteToChar(r.Start)
	cLimit := byteToChar(r.Limit)
	cErr = C.rocksdb_compact_range_opt(db.c, opts.c, cStart, C.size_t(len(r.Start)), cLimit, C.size_t(len(r.Limit)))
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}
