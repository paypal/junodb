package proto

import (
	"crypto/rand"
	"net"
	"testing"
)

var (
	gOpMsg OperationalMessage
	gRaw   RawMessage
)

func BenchmarkEncode(b *testing.B) {
	raw := &RawMessage{}
	for i := 0; i < b.N; i++ {
		if gOpMsg.Encode(raw) != nil {
			b.Fail()
		}
	}
	raw.ReleaseBuffer()
}

func BenchmarkDecode(b *testing.B) {
	opMsg := OperationalMessage{}
	for i := 0; i < b.N; i++ {
		if opMsg.Decode(&gRaw) != nil {
			b.FailNow()
		}
	}
}

func BenchmarkCopyDataDecode(b *testing.B) {
	opMsg := OperationalMessage{}
	for i := 0; i < b.N; i++ {
		if opMsg.decode(gRaw.body, &gRaw.messageHeaderT, true) != nil {
			b.FailNow()
		}
	}
}

func BenchmarkWithoutBufferPoolEncode(b *testing.B) {
	for i := 0; i < b.N; i++ {
		raw := &RawMessage{}
		if gOpMsg.Encode(raw) != nil {
			b.Fail()
		}
	}
}

func init() {
	key := make([]byte, 128)
	_, err := rand.Read(key)
	namespace := []byte("the namespace")
	appName := []byte("appname")
	value := make([]byte, 2048)
	_, err = rand.Read(value)
	if err != nil {

	}
	corrId := []byte("11e8c63b89ba0")
	ttl := uint32(1800)
	payload := &Payload{}
	payload.SetWithClearValue(value)
	gOpMsg.SetMessage(OpCodeCreate, key, namespace, payload, ttl)
	gOpMsg.SetNewRequestID()
	gOpMsg.SetCorrelationID(corrId)
	gOpMsg.SetSource(net.ParseIP("127.0.0.1"), 1234, appName)

	gOpMsg.Encode(&gRaw)
}
