package client

import (
	"fmt"
	"time"

	"juno/pkg/io"
	"juno/pkg/util"
)

type Duration = util.Duration

type Config struct {
	Server             io.ServiceEndpoint
	Appname            string
	Namespace          string
	RetryCount         int
	DefaultTimeToLive  int
	ConnectTimeout     Duration
	ReadTimeout        Duration
	WriteTimeout       Duration
	RequestTimeout     Duration
	ConnRecycleTimeout Duration
}

var defaultConfig = Config{
	RetryCount:         1,
	DefaultTimeToLive:  1800,
	ConnectTimeout:     Duration{100 * time.Millisecond},
	ReadTimeout:        Duration{500 * time.Millisecond},
	WriteTimeout:       Duration{500 * time.Millisecond},
	RequestTimeout:     Duration{1000 * time.Millisecond},
	ConnRecycleTimeout: Duration{9 * time.Second},
}

func SetDefaultTimeToLive(ttl int) {
	defaultConfig.DefaultTimeToLive = ttl
}

func SetDefaultTimeout(connect, read, write, request, connRecycle time.Duration) {
	defaultConfig.ConnectTimeout.Duration = connect
	defaultConfig.ReadTimeout.Duration = read
	defaultConfig.WriteTimeout.Duration = write
	defaultConfig.RequestTimeout.Duration = request
	defaultConfig.ConnRecycleTimeout.Duration = connRecycle
}

func (c *Config) SetDefault() {
	*c = defaultConfig
}

func (c *Config) validate() error {
	if err := c.Server.Validate(); err != nil {
		return err
	}
	if len(c.Appname) == 0 {
		return fmt.Errorf("Config.AppName not specified.")
	}
	if len(c.Namespace) == 0 {
		return fmt.Errorf("Config.Namespace not specified.")
	}
	/// TODO to validate others
	return nil
}
