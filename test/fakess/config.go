// +build

package main

import (
	"math"
	"time"

	"juno/third_party/forked/golang/glog"

	"github.com/BurntSushi/toml"

	"juno/pkg/io"
	cal "juno/pkg/logging/cal/config"
	"juno/pkg/util"
)

var Conf = Config{

	Inbound: io.InboundConfig{
		IdleTimeout:          util.Duration{math.MaxUint32 * time.Second},
		ReadTimeout:          util.Duration{math.MaxUint32 * time.Millisecond},
		WriteTimeout:         util.Duration{math.MaxUint32 * time.Millisecond},
		RequestTimeout:       util.Duration{600 * time.Millisecond},
		ReqProcessorPoolSize: 5000,
		MaxNumReqProcessors:  20000,
	},
	CAL: cal.Config{
		Host:             "127.0.0.1",
		Port:             1118,
		Environment:      "PayPal",
		Poolname:         "fakess",
		MessageQueueSize: 10000,
		Enabled:          false,
		CalType:          "FILE",
		CalLogFile:       "cal.log",
	},
	LogLevel: "warning",
}

type Config struct {
	Inbound            io.InboundConfig
	MaxKeyLength       int
	MaxNamespaceLength int
	MaxPayloadLength   int
	MaxTimeToLive      int
	LogLevel           string
	CAL                cal.Config
}

func (c *Config) Dump() {

	glog.Infof("inbound: %%v", c.Inbound)
}

func LoadConfig(file string) error {
	if _, err := toml.DecodeFile(file, &Conf); err != nil {
		return err
	}

	// TODO: Config validation

	return nil
}
