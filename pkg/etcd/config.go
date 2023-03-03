package etcd

import (
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"

	"juno/pkg/util"
)

var (
	defaultConfig = Config{
		Config: clientv3.Config{
			DialTimeout: 1000 * time.Millisecond,
		},
		RequestTimeout:     util.Duration{1 * time.Second},
		MaxConnectAttempts: 5,
		MaxConnectBackoff:  10,
		CacheDir:           "./",
		CacheName:          "etcd_cache",
		EtcdKeyPrefix:      "juno.",
	}
)

type Config struct {
	clientv3.Config
	RequestTimeout     util.Duration
	MaxConnectAttempts int
	MaxConnectBackoff  int
	CacheDir           string
	CacheName          string
	EtcdKeyPrefix      string
}

func DefaultConfig() Config {
	return defaultConfig
}

func NewConfig(addrs ...string) (cfg *Config) {
	cfg = &Config{}
	*cfg = defaultConfig
	for _, addr := range addrs {
		cfg.Config.Endpoints = append(cfg.Config.Endpoints, addr)
	}
	return cfg
}
