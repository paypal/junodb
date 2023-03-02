package udf

// dummy udfs for testing

// Hello udf
type HelloUDF struct{}

func (u *HelloUDF) Call(key []byte, value []byte, params []byte) (res []byte, err error) {
	res = make([]byte, len("hello world"))
	copy(res, "hello world")
	return res, nil
}

func (u *HelloUDF) GetVersion() uint32 {
	return 1
}

func (u *HelloUDF) GetName() string {
	return "hello"
}

// Echo udf
type EchoUDF struct{}

func (u *EchoUDF) Call(key []byte, value []byte, params []byte) (res []byte, err error) {
	res = make([]byte, len(value))
	copy(res, value)
	return res, nil
}

func (u *EchoUDF) GetVersion() uint32 {
	return 1
}

func (u *EchoUDF) GetName() string {
	return "echo"
}

// Register built-in UDFs
func registerDummyUDFs(um *UDFMap) {
	(*um)["hello"] = &HelloUDF{}
	(*um)["echo"] = &EchoUDF{}
}
