package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"unsafe"
)

// Set option for opened db.
func (db *DB) SetOption(key string, value string) error {

	cKeys := make([]*C.char, 1)
	cVals := make([]*C.char, 1)
	cErrs := make([]*C.char, 1)

	defer func() {
		C.free(unsafe.Pointer(cKeys[0]))
		C.free(unsafe.Pointer(cVals[0]))
		C.free(unsafe.Pointer(cErrs[0]))
	}()

	cKeys[0] = C.CString(key)
	cVals[0] = C.CString(value)

	C.rocksdb_set_options(db.c, C.int(1), &cKeys[0], &cVals[0], &cErrs[0])

	if cErrs[0] != nil {
		return errors.New(C.GoString(cErrs[0]))
	}

	return nil
}
