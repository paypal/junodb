package gld

import (
	"fmt"
	"os"
	"strings"
	"time"

	"juno/pkg/client"
	"juno/pkg/util"

	"juno/third_party/forked/golang/glog"
)

type Duration = util.Duration

// Juno Client Wrapper
type JunoClient struct {
	client    client.IClient
	clientCfg client.Config

	ttl        int
	ErrCount   int
	DupCount   int
	NoKeyCount int
}

func (c *JunoClient) Init(server string, ns string, default_ttl int) {

	var err error
	c.clientCfg = client.Config{
		RetryCount:         1,
		DefaultTimeToLive:  default_ttl,
		ConnectTimeout:     Duration{1000 * time.Millisecond},
		ReadTimeout:        Duration{1000 * time.Millisecond},
		WriteTimeout:       Duration{1000 * time.Millisecond},
		RequestTimeout:     Duration{1000 * time.Millisecond},
		ConnRecycleTimeout: Duration{1000 * time.Second},
	}
	c.clientCfg.Server.Addr = server
	c.clientCfg.Appname = ns
	c.clientCfg.Namespace = ns

	c.client, err = client.New(c.clientCfg)

	if err != nil {
		glog.Exitf("%s", err)
	}
	c.ttl = default_ttl
}

func (c *JunoClient) AddKey(shardid int, key []byte, payload []byte) bool {

	//c.client.Destroy(key)

	var err error
	for i := 0; i < 3; i++ {
		_, err = c.client.Create(key, payload, client.WithTTL(uint32(c.ttl)))
		if err == nil {
			return true
		}
		if strings.Contains(err.Error(), "unique key violation") {
			c.DupCount++
			break
		}
	}

	c.ErrCount++
	//if c.ErrCount <= 100 {
		glog.Errorf("Add entry %d failed with %s", shardid, err)
	//}
	return false
}

func (c *JunoClient) GetKey(shardid int, key []byte) (res bool, value []byte) {
	var err error
	var rec client.IContext
	for i := 0; i < 3; i++ {
		value, rec, err = c.client.Get(key)
		if err == nil {
			rec.PrettyPrint(os.Stdout)
			fmt.Printf("key: %#x, sharid: %d\n", key, shardid)
			fmt.Printf("Value: {\n  %s\n}\n", util.ToPrintableAndHexString(value))
			res = true
			return
		}
		if strings.Contains(err.Error(), " no key") {
			c.NoKeyCount++
			break
		}
	}

	c.ErrCount++
	if c.ErrCount <= 100 {
		glog.Errorf("Get shardid %d key failed with %s", shardid, err)
	}
	res = false
	return
}

func (c *JunoClient) DelKey(shardid int, key []byte) bool {
	err := c.client.Destroy(key)
	if err == nil {
		return true
	}

	c.ErrCount++
	if c.ErrCount <= 100 {
		glog.Errorf("Delete shard %d key failed with %s", shardid, err)
	}
	return false
}
