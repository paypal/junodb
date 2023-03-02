package gorocksdb

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"errors"
	"unsafe"
)

// Delete files on a Range of keys.
func (db *DB) DeleteFilesInRange(r Range) error {
	cStart := byteToChar(r.Start)
	cLimit := byteToChar(r.Limit)
	var cErr *C.char
	C.rocksdb_delete_file_in_range(db.c, cStart, C.size_t(len(r.Start)), cLimit, C.size_t(len(r.Limit)), &cErr)
	if cErr != nil {
		defer C.free(unsafe.Pointer(cErr))
		return errors.New(C.GoString(cErr))
	}
	return nil
}
