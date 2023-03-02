// +build !debug

package db

import (
	"io"
)

func onAllocValue(i interface{}) {
}

func onFreeValue(i interface{}) {
}

func WriteSliceTrackerStats(w io.Writer) {
}
