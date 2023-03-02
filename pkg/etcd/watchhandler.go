package etcd

import (
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type IWatchHandler interface {
	OnEvent(e ...*clientv3.Event)
}

type EchoWatchHandler struct{}

func (h *EchoWatchHandler) OnEvent(e ...*clientv3.Event) {
	for i, ev := range e {
		fmt.Printf("%d\t%v\n", i, ev)
	}
}
