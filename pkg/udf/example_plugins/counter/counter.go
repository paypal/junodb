package main

import (
	"encoding/binary"
	"errors"
)

type CounterUDF struct{}

func (u *CounterUDF) Call(key []byte, value []byte, params []byte) (res []byte, err error) {
	if len(value) != 4 || len(params) != 4 {
		return nil, errors.New("Bad Param")
	}
	var counter uint32 = binary.BigEndian.Uint32(value)
	var delta uint32 = binary.BigEndian.Uint32(params)
	counter += delta
	res = make([]byte, 4)
	binary.BigEndian.PutUint32(res, counter)
	return res, nil
}

func (u *CounterUDF) GetVersion() uint32 {
	return 1
}

func (u *CounterUDF) GetName() string {
	return "counter"
}

func GetUDFInterface() (f interface{}, err error) {
	f = &CounterUDF{}
	return
}
