package sec

import (
	"fmt"
	"sync/atomic"

	"juno/third_party/forked/golang/glog"
)

var (
	DefaultConfig = Config{
		AppName:    "junoserv",
		ClientAuth: true,
	}
	config    = DefaultConfig // xuli: to revisit
	secInited uint32          // xuli: to revisit
)

type Config struct {
	AppName          string
	CertPem          string
	KeyPem           string
	ClientAuth       bool
	KeyStoreFilePath string
	CertPemFilePath  string
	KeyPemFilePath   string
	CAFilePath       string
}

func InitSecConfig(conf *Config) error {
	if atomic.CompareAndSwapUint32(&secInited, 0, 1) {
		if conf != nil {
			config = *conf
		}
		config.Default()
		//TODO validate
	} else {
		return fmt.Errorf("sec config had been initialized before")
	}
	return nil
}

func (c *Config) Default() {
	c.Validate()
}

func (c *Config) Validate() {
	if len(c.AppName) <= 0 { ///TODO
		glog.Fatal("Error: AppName is required for KMS.")
	}
}

func (c *Config) Dump() {
	glog.Infof("KMS AppName : %s\n", c.AppName)
}
