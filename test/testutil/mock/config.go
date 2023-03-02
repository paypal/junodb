package mock

import (
	"math"
	"time"

	"juno/pkg/io"
	"juno/pkg/service"
	"juno/pkg/util"
)

type SSConfig struct {
	service.Config
	MeanDelay   int
	StdDevDelay int
	ValueSize   int
	StdDevSize  int
	Inbound     io.InboundConfig
	LogLevel    string
}

var (
	DefaultSSConfig SSConfig = SSConfig{
		MeanDelay:   0,
		StdDevDelay: 0,
		ValueSize:   1024,
		StdDevSize:  100,
		Inbound: io.InboundConfig{
			IdleTimeout:    util.Duration{math.MaxUint32 * time.Second},
			ReadTimeout:    util.Duration{math.MaxUint32 * time.Millisecond},
			WriteTimeout:   util.Duration{math.MaxUint32 * time.Millisecond},
			RequestTimeout: util.Duration{600 * time.Millisecond},
		},
		LogLevel: "warning",
	}
)
