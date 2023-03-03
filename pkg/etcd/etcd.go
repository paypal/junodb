package etcd

import (
	"errors"
	"juno/third_party/forked/golang/glog"
	"sync"
)

var (
	cli  *EtcdClient
	rw   *EtcdReadWriter
	once sync.Once
)

func Connect(cfg *Config, clsName string) (err error) {
	glog.Infof("Setting up etcd.")
	once.Do(func() {
		cli = NewEtcdClient(cfg, clsName)
		if cli != nil {
			rw = NewEtcdReadWriter(cli)
		}
	})

	if cli == nil {
		return errors.New("Failed to initialize etcd")
	}

	return nil
}

func Close() {
	glog.Infof("Closing etcd.")
}

func GetClsReadWriter() *EtcdReadWriter {
	return rw
}

func GetEtcdCli() *EtcdClient {
	return cli
}
