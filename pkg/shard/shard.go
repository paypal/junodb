package shard

import ()

type ID uint16

//Map by ID
type Map map[ID]struct{}

func (id ID) Uint16() uint16 {
	return uint16(id)
}

func NewMap() Map {
	return make(Map)
}

func NewMapWithSize(sz int) Map {
	return make(Map, sz)
}

func (m Map) Keys() []ID {
	i := 0
	keys := make([]ID, len(m), len(m))
	for k, _ := range m {
		keys[i] = k
		i++
	}

	return keys
}
