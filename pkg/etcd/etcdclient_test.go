package etcd

//import (
//	"bytes"
//	"fmt"
//	"github.com/coreos/etcd/clientv3"
//	"testing"
//	"time"
//)
//
//func TestPut(t *testing.T) {
//	cfg := NewConfig("127.0.0.1:2379")
//	cli := NewEtcdClient(cfg, "mycluster")
//	err := cli.PutValue("key1", "v1")
//	if err != nil {
//		t.Error("failed to put value:", err)
//	}
//}
//
//func TestGet(t *testing.T) {
//	cfg := NewConfig("127.0.0.1:2379")
//	cli := NewEtcdClient(cfg, "mycluster")
//	err := cli.PutValue("key2", "v2")
//	if err != nil {
//		t.Error("failed to put value:", err)
//	}
//
//	v, err := cli.GetValue("key2")
//	if err != nil {
//		t.Error("failed to get value:", err)
//	}
//
//	if bytes.Compare([]byte(v), []byte("v2")) != 0 {
//		t.Error("data value != inputData")
//	}
//}
//
//func TestTimeout(t *testing.T) {
//	cfg := NewConfig("127.0.0.1:2379")
//	cli := NewEtcdClient(cfg, "mycluster")
//	if cli == nil {
//		return
//	}
//
//	fmt.Printf("connected")
//	time.Sleep(5 * time.Second)
//
//	err := cli.PutValue("key1", "v1")
//	if err != nil {
//		fmt.Println("failed to put value:", err)
//	}
//
//	time.Sleep(5 * time.Second)
//	err = cli.PutValue("key1", "v1")
//	if err != nil {
//		fmt.Println("failed to put value:", err)
//	}
//}
//
//type TestWatchHandler struct {
//}
//
//func (h *TestWatchHandler) OnEvent(e ...*clientv3.Event) {
//	for i, ev := range e {
//		fmt.Printf("on Event: %d\t, %s\n", i, ev.Kv.Value)
//	}
//}
//
//func TestWatcher(t *testing.T) {
//	cfg := NewConfig("127.0.0.1:2379")
//	cli := NewEtcdClient(cfg, "mycluster")
//	if cli == nil {
//		return
//	}
//
//	fmt.Println("connected")
//	th := &TestWatchHandler{}
//	cancel, err := cli.Watch("mytest", th)
//
//	//	err = cli.PutValue("mytest", "v3")
//	//	if err != nil {
//	//		fmt.Println("failed to put value:", err)
//	//	}
//
//	v, err := cli.GetValue("mytest")
//	if err != nil {
//		t.Error("failed to get value:", err)
//	}
//	fmt.Printf("v=%s\n", v)
//
//	time.Sleep(5 * time.Second)
//	cancel()
//
//	key := fmt.Sprintf("%s%sredist%senable%s%d",
//		"payments",
//		kKeyComponentDelimiter,
//		kKeyComponentDelimiter,
//		kKeyComponentDelimiter,
//		3)
//	fmt.Printf("key=%s\n", key)
//}
