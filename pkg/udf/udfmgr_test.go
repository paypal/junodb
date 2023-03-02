package udf

import (
	"encoding/binary"
	"juno/third_party/forked/golang/glog"

	"fmt"
	"testing"
)

func TestBuiltinCounter(t *testing.T) {

	mgr, _ := NewUDFManager("")
	udf := mgr.GetUDF("sc")
	if udf != nil {
		key1 := "k1"
		value1 := make([]byte, 4)
		delta := make([]byte, 4)
		binary.BigEndian.PutUint32(value1, 5)
		binary.BigEndian.PutUint32(delta, 4)

		r, _ := udf.Call([]byte(key1), value1, delta)
		newcount := binary.BigEndian.Uint32(r)
		if newcount != 9 {
			t.Errorf("wrong count")
		}
	} else {
		t.Errorf("can't find the counter udf plugin")
	}
	glog.Flush()
}
func TestCounterPlugin(t *testing.T) {

	mgr, _ := NewUDFManager("./example_plugins/counter")
	udf := mgr.GetUDF("counter")
	if udf != nil {
		key1 := "k1"
		value1 := make([]byte, 4)
		delta := make([]byte, 4)
		binary.BigEndian.PutUint32(value1, 5)
		binary.BigEndian.PutUint32(delta, 1)

		r, _ := udf.Call([]byte(key1), value1, delta)
		newcount := binary.BigEndian.Uint32(r)
		if newcount != 6 {
			t.Errorf("wrong count")
		}
	} else {
		t.Errorf("can't find the counter udf plugin")
	}
	glog.Flush()
}

func TestBadCounterPlugin(t *testing.T) {
	mgr, _ := NewUDFManager("./example_plugins/bad_plugin")
	udf := mgr.GetUDF("bad_plugin")
	if udf != nil {
		t.Error("should not get here")
	} else {
		fmt.Printf("can't find the udf plugin \n")
	}
	glog.Flush()
}
