package udf

import (
	"sync/atomic"
)

type IUDF interface {
	Call(key []byte, value []byte, params []byte) (res []byte, err error)
	GetVersion() uint32
	GetName() string
}

type UDFMap map[string]IUDF

func newUDFMap() (m *UDFMap) {
	um := make(map[string]IUDF)
	return (*UDFMap)(&um)
}

type UDFMgr struct {
	index  int32
	udfs   [2]*UDFMap
	udfDir string
}

var theMgr *UDFMgr

func Init(udfDir string) {
	if theMgr == nil {
		theMgr, _ = NewUDFManager(udfDir)
	}
}

func GetUDFManager() *UDFMgr {
	return theMgr
}

func NewUDFManager(udfDir string) (m *UDFMgr, err error) {
	mgr := &UDFMgr{
		index:  0,
		udfDir: udfDir,
	}
	mgr.Init()
	return mgr, nil
}

// thread safe
func (m *UDFMgr) Init() (err error) {
	var next int32 = (m.index + 1) % 2
	mp := newUDFMap()

	registerBuiltinUDFs(mp)
	registerDummyUDFs(mp)
	loadUDFPlugins(m.udfDir, mp)

	m.udfs[next] = mp
	atomic.StoreInt32(&m.index, next)
	return nil
}

// thread safe
func (m *UDFMgr) GetUDF(name string) IUDF {
	var ix int32 = atomic.LoadInt32(&m.index)
	umap := m.udfs[ix]
	if umap != nil {
		return (*umap)[name]
	} else {
		return nil
	}
}
