package db

import (
	"bytes"
	"fmt"
	"juno/pkg/shard"
	"testing"
)

func TestNoMicroShard(t *testing.T) {
	ns := "namespace"
	key := "testkey1"

	shardid := 129    // fake
	microshardid := 6 // fake

	// encode
	var buf bytes.Buffer
	recordid := NewRecordIDWithBuffer(&buf, shard.ID(shardid), uint8(microshardid), []byte(ns), []byte(key))

	// decode
	ns_d, key_d, _ := DecodeRecordKey([]byte(recordid))
	fmt.Printf("ns_d=%s, key_d=%s, len=%d", ns_d, key_d, len([]byte(recordid)))
	if string(ns_d) != ns {
		t.Error(fmt.Sprintf("passed wrong ns: %s", ns_d))
	}

	if string(key_d) != key {
		t.Error(fmt.Sprintf("passed wrong key: %s", key_d))
	}

	size := len(ns) + len(key) + 3
	if size != len([]byte(recordid)) {
		t.Error(fmt.Sprintf("wrong size %d", len([]byte(recordid))))
	}
}

func TestEnableMicroShard(t *testing.T) {
	SetEnableMircoShardId(true)
	ns := "namespace"
	key := "testkey1"

	shardid := 129    // fake
	microshardid := 6 // fake

	// encode
	var buf bytes.Buffer
	recordid := NewRecordIDWithBuffer(&buf, shard.ID(shardid), uint8(microshardid), []byte(ns), []byte(key))

	// decode
	ns_d, key_d, _ := DecodeRecordKey([]byte(recordid))
	fmt.Printf("ns_d=%s, key_d=%s, len=%d", ns_d, key_d, len([]byte(recordid)))
	if string(ns_d) != ns {
		t.Error(fmt.Sprintf("passed wrong ns: %s", ns_d))
	}

	if string(key_d) != key {
		t.Error(fmt.Sprintf("passed wrong key: %s", key_d))
	}

	size := len(ns) + len(key) + 4
	if size != len([]byte(recordid)) {
		t.Error(fmt.Sprintf("wrong size %d", len([]byte(recordid))))
	}
}
