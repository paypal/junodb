//  
//  Copyright 2023 PayPal Inc.
//  
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//  
//     http://www.apache.org/licenses/LICENSE-2.0
//  
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//  
  
package cli

import (
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"time"

	"juno/pkg/logging/cal"
	"juno/pkg/logging/cal/config"
	"juno/third_party/forked/golang/glog"

	"github.com/BurntSushi/toml"
	uuid "github.com/satori/go.uuid"

	"juno/pkg/client"
	"juno/pkg/cmd"
	"juno/pkg/sec"
	"juno/pkg/util"
)

const (
	kClientAppName        = "junocli"
	kDefaultTimeToLive    = uint(1800)
	kDefaultServerAddress = "127.0.0.1:8080"
	kDefaultNamespace     = "namespace"
)

var defaultConfig = client.Config{
	RetryCount:         1,
	DefaultTimeToLive:  1800,
	ConnectTimeout:     util.Duration{100 * time.Millisecond},
	ReadTimeout:        util.Duration{500 * time.Millisecond},
	WriteTimeout:       util.Duration{500 * time.Millisecond},
	RequestTimeout:     util.Duration{1000 * time.Millisecond},
	ConnRecycleTimeout: util.Duration{9 * time.Second},
}

type (
	clientCommandT struct {
		cmd.Command
		client.Config

		key []byte

		optKeyType          uint
		optLogLevel         string
		optCfgFile          string
		optServerAddr       string
		optServerSSLEnabled bool
		optNamespace        string
		optAppName          string
	}
	clientCommandWithValueT struct {
		clientCommandT
		ttl       uint
		value     []byte
		valueType uint
		valueLen  uint
	}
	clientUDFCommandT struct {
		clientCommandWithValueT
		optUDFName string
	}

	cmdCreateT struct {
		clientCommandWithValueT
	}

	cmdGetT struct {
		clientCommandT
		ttl uint
	}

	cmdUpdateT struct {
		clientCommandWithValueT
	}

	cmdSetT struct {
		clientCommandWithValueT
	}

	cmdUDFGetT struct {
		clientUDFCommandT
	}

	cmdUDFSetT struct {
		clientUDFCommandT
	}
	cmdDestroyT struct {
		clientCommandT
	}
	cmdPopulateT struct {
		cmd.Command
		client.Config

		optLogLevel         string
		optCfgFile          string
		optServerAddr       string
		optServerSSLEnabled bool
		optNamespace        string
		optAppName          string
		optUDFName          string

		optNumRecords uint
		optLenValue   uint
		optTTL        uint
	}
)

func (c *clientCommandT) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.Config = defaultConfig

	c.StringOption(&c.optServerAddr, "s|server", kDefaultServerAddress, "specify server address")
	c.UintOption(&c.optKeyType, "kt|key-type", 0, "specify the type of the key. \n   \t0 - string key\n   \t1 - hex key\n   \t2 - generated key")
	c.StringOption(&c.optNamespace, "ns|namespace", kDefaultNamespace, "specify namespace")
	c.StringOption(&c.optAppName, "appname", kClientAppName, "specify appname")
	c.StringOption(&c.optLogLevel, "log-level", "info", "specify log level")
	c.StringOption(&c.optCfgFile, "c|config", "", "specify toml configuration file name")
	c.BoolOption(&c.optServerSSLEnabled, "ssl", false, "SSL")
	c.SetSynopsis("[option] <key>")
}

func (c *clientCommandT) Parse(args []string) (err error) {
	if err = c.Option.Parse(args); err != nil {
		return
	}
	glog.InitLogging(c.optLogLevel, " [cli] ")
	tmp := &struct {
		Config *client.Config
		Sec    sec.Config
		Cal    config.Config
	}{Config: &c.Config}

	if len(c.optCfgFile) != 0 {
		if _, err := toml.DecodeFile(c.optCfgFile, &tmp); err != nil {
			glog.Exitf("failed to load config file %s. %s", c.optCfgFile, err.Error())
		}
		//initialize CAL from Config file if present
		if tmp.Cal.Enabled == true {
			if tmp.Cal.Poolname == "" {
				tmp.Cal.Poolname = "junocli"
			}
			cal.InitWithConfig(&tmp.Cal)
		}
	}
	if c.Server.Addr == "" || c.optServerAddr != kDefaultServerAddress {
		c.Server.Addr = c.optServerAddr
	}
	if c.optServerSSLEnabled {
		c.Server.SSLEnabled = true
	}
	if c.Namespace == "" || c.optNamespace != kDefaultNamespace {
		c.Namespace = c.optNamespace
	}
	if c.Appname == "" || c.optAppName != kClientAppName {
		c.Appname = c.optAppName
	}

	if c.Server.SSLEnabled {
		if err := sec.Initialize(&tmp.Sec, sec.KFlagClientTlsEnabled); err != nil {
			glog.Exitf("failed to initialize sec. error: %s", err.Error())
		}
	}
	n := c.NArg()
	switch c.optKeyType {
	case 0:
		if n < 1 {
			err = fmt.Errorf("missing key")
			return
		}
		c.key = []byte(c.Arg(0))
	case 1:
		if n < 1 {
			err = fmt.Errorf("missing key")
			return
		}
		if c.key, err = hex.DecodeString(c.Arg(0)); err != nil {
			return
		}
	case 2:
		c.key = uuid.NewV4().Bytes()
	default:
	}
	return
}

func (c *clientCommandWithValueT) Init(name string, desc string) {
	c.clientCommandT.Init(name, desc)
	c.UintOption(&c.valueType, "vt|value-type", 0, "specify the type of the value. \n   \t0 - string value\n   \t1 - hex value\n   \t2 - generated value")
	c.UintOption(&c.valueLen, "vl|value-len", 1024, "specify the length of the value if value-type is 2")
	c.UintOption(&c.ttl, "ttl", kDefaultTimeToLive, "specify TTL in second")
	c.SetSynopsis("[option] <key> <value>")
}

func (c *clientCommandWithValueT) Parse(args []string) (err error) {
	if err = c.clientCommandT.Parse(args); err != nil {
		return
	}
	idxValue := 0
	if c.optKeyType != 2 {
		idxValue++
	}
	n := c.NArg() - idxValue

	switch c.valueType {
	case 0:
		if n < 1 {
			err = fmt.Errorf("missing value")
			return
		}
		c.value = []byte(c.Arg(idxValue))
	case 1:
		if n < 1 {
			err = fmt.Errorf("missing value")
			return
		}
		if c.value, err = hex.DecodeString(c.Arg(idxValue)); err != nil {
			return
		}
	case 2:
		if c.valueLen != 0 {
			c.value = make([]byte, c.valueLen, c.valueLen)
			rand.Seed(time.Now().Unix())
			for i := 0; i < int(c.valueLen); i++ {
				c.value[i] = byte(rand.Intn(255))
			}
		}
	default:
		err = fmt.Errorf("not supported")
	}
	return
}

func (c *clientUDFCommandT) Init(name string, desc string) {
	c.clientCommandWithValueT.Init(name, desc)
	c.StringOption(&c.optUDFName, "udfname", "", "specify udf name")
	c.SetSynopsis("[option] <key> <value/param>")
}

func (c *cmdCreateT) Exec() {
	c.Validate()

	if cli, err := client.New(c.Config); err == nil {
		rec, err := cli.Create(c.key, c.value, client.WithTTL(uint32(c.ttl)))
		if c.isOk(err) {
			rec.PrettyPrint(os.Stdout)
		}
	} else {
		fmt.Println(err)
	}
}

func (c *cmdGetT) Init(name string, desc string) {
	c.clientCommandT.Init(name, desc)
	c.UintOption(&c.ttl, "ttl", kDefaultTimeToLive, "specify TTL in second")
}

func (c *cmdGetT) Exec() {
	c.Validate()

	if client, err := client.New(c.Config); err == nil {
		value, rec, err := client.Get(c.key)
		if c.isOk(err) {
			rec.PrettyPrint(os.Stdout)
			fmt.Printf("Value: {\n  %s\n}\n", util.ToPrintableAndHexString(value))
		}
	} else {
		fmt.Println(err)
	}
}

func (c *cmdUpdateT) Exec() {
	c.Validate()

	if cli, err := client.New(c.Config); err == nil {
		rec, err := cli.Update(c.key, c.value, client.WithTTL(uint32(c.ttl)))
		if c.isOk(err) {
			rec.PrettyPrint(os.Stdout)
		}
	} else {
		fmt.Println(err)
	}
}

func (c *cmdSetT) Exec() {
	c.Validate()

	if cli, err := client.New(c.Config); err == nil {
		rec, err := cli.Set(c.key, c.value, client.WithTTL(uint32(c.ttl)))
		if c.isOk(err) {
			rec.PrettyPrint(os.Stdout)
		}
	} else {
		fmt.Println(err)
	}
}

func (c *cmdDestroyT) Exec() {
	c.Validate()

	if cli, err := client.New(c.Config); err == nil {
		err := cli.Destroy(c.key)
		c.isOk(err)
		//fmt.Println(err)
	} else {
		fmt.Println(err)
	}
}

func (c *cmdUDFGetT) Exec() {
	c.Validate()

	if cli, err := client.New(c.Config); err == nil {
		value, rec, err := cli.UDFGet(c.key, []byte(c.optUDFName), c.value, client.WithTTL(uint32(c.ttl)))
		if c.isOk(err) {
			rec.PrettyPrint(os.Stdout)
			fmt.Printf("Value: {\n  %s\n}\n", util.ToPrintableAndHexString(value))
		}
	} else {
		fmt.Println(err)
	}
}

func (c *cmdUDFSetT) Exec() {
	c.Validate()

	if cli, err := client.New(c.Config); err == nil {
		rec, err := cli.UDFSet(c.key, []byte(c.optUDFName), c.value, client.WithTTL(uint32(c.ttl)))
		if c.isOk(err) {
			rec.PrettyPrint(os.Stdout)
		}
	} else {
		fmt.Println(err)
	}
}

func (c *clientCommandT) isOk(err error) bool {
	if err == nil {
		fmt.Printf("* command '%s' successful\n", c.GetName())
		return true
	} else {
		fmt.Printf("* command '%s' failed: %s\n", c.GetName(), err)
	}
	return false
}

func (c *cmdPopulateT) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.Config = defaultConfig

	c.StringOption(&c.optServerAddr, "s|server", kDefaultServerAddress, "specify server address")
	c.StringOption(&c.optNamespace, "ns|namespace", kDefaultNamespace, "specify namespace")
	c.StringOption(&c.optAppName, "appname", kClientAppName, "specify appname")
	c.StringOption(&c.optLogLevel, "log-level", "info", "specify log level")
	c.StringOption(&c.optCfgFile, "c|config", "", "specify toml configuration file name")
	c.StringOption(&c.optUDFName, "udfname", "", "specify udf name")

	c.BoolOption(&c.optServerSSLEnabled, "ssl", false, "SSL")
	c.UintOption(&c.optNumRecords, "n|num-record", 100, "specify the number of records to be created")
	c.UintOption(&c.optLenValue, "l|value-len", 1024, "specify the value length")
	c.UintOption(&c.optTTL, "ttl", kDefaultTimeToLive, "specify TTL in second")

	//	c.SetSynopsis("[option] <key>")

}

func (c *cmdPopulateT) Parse(args []string) (err error) {
	if err = c.Option.Parse(args); err != nil {
		return
	}
	glog.InitLogging(c.optLogLevel, " [cli] ")
	tmp := &struct {
		Config *client.Config
		Sec    sec.Config
	}{Config: &c.Config, Sec: sec.DefaultConfig}

	if len(c.optCfgFile) != 0 {
		if _, err := toml.DecodeFile(c.optCfgFile, &tmp); err != nil {
			glog.Exitf("failed to local config file %s. %s", c.optCfgFile, err)
		}
	}
	if c.Server.Addr == "" || c.optServerAddr != kDefaultServerAddress {
		c.Server.Addr = c.optServerAddr
	}
	if c.optServerSSLEnabled {
		c.Server.SSLEnabled = true
	}
	if c.Namespace == "" || c.optNamespace != kDefaultNamespace {
		c.Namespace = c.optNamespace
	}
	if c.Appname == "" || c.optAppName != kClientAppName {
		c.Appname = c.optAppName
	}
	if c.Server.SSLEnabled {
		if err := sec.Initialize(&tmp.Sec, sec.KFlagClientTlsEnabled); err != nil {
			glog.Exitf("failed to initialize sec. error: %s", err.Error())
		}
	}
	return
}

func (c *cmdPopulateT) Exec() {
	if c.optNumRecords != 0 {
		fmt.Printf("  populating %d record(s)...\n", c.optNumRecords)

		if cli, err := client.New(c.Config); err == nil {
			var numFailed uint
			rand.Seed(time.Now().Unix())
			value := make([]byte, c.optLenValue, c.optLenValue)
			for i := 0; i < int(c.optLenValue); i++ {
				value[i] = byte(rand.Intn(255))
			}
			for i := uint(0); i < c.optNumRecords; i++ {
				key := uuid.NewV4().Bytes()
				if _, err := cli.Set(key, value, client.WithTTL(uint32(c.optTTL))); err != nil {
					numFailed++
				}
			}
			fmt.Printf("  * %d successful, %d failed\n", c.optNumRecords-numFailed, numFailed)
		} else {
			fmt.Println(err)

		}
	}

}

func init() {
	create := &cmdCreateT{}
	create.Init("create", "create a record")

	get := &cmdGetT{}
	get.Init("get", "get the value of a given key")

	update := &cmdUpdateT{}
	update.Init("update", "update a record")

	set := &cmdSetT{}
	set.Init("set", "create or update a record if exists")

	destroy := &cmdDestroyT{}
	destroy.Init("destroy", "destroy a record")

	udfget := &cmdUDFGetT{}
	udfget.Init("udfget", "udf get")

	udfset := &cmdUDFSetT{}
	udfset.Init("udfset", "udf set")

	populate := &cmdPopulateT{}
	populate.Init("populate", "populate a set of records with set commands")

	cmd.RegisterNewGroup("proxy commands", create, get, update, set, destroy, udfget, udfset, populate)

	pCreate := &cmdPrepareCreateT{}
	pCreate.Init("pcreate", "PrepareCreate to storage server")

	read := &cmdReadT{}
	read.Init("read", "Read to storage server")

	pUpdate := &cmdPrepareUpdateT{}
	pUpdate.Init("pupdate", "PrepareUpdate to storage server")

	pSet := &cmdPrepareSetT{}
	pSet.Init("pset", "PrepareSet to storage server")

	twoPhaseDel := &cmdPrepareDeleteT{}
	twoPhaseDel.Init("pdelete", "PrepareDelete to storage server")

	del := &cmdDeleteT{}
	del.Init("delete", "Delete to storage server")

	commit := &cmdCommitT{}
	commit.Init("commit", "Commit the record having been prepared successfully")

	abort := &cmdAbortT{}
	abort.Init("abort", "Abort the record having been prepared successfully")

	repair := &cmdRepairT{}
	repair.Init("repair", "Repair record")

	markDelete := &cmdMarkDeleteT{}
	markDelete.Init("mdelete", "mark a record as deleted")

	populatess := &cmdPopulateSST{}
	populatess.Init("populatess", "populate storage with a set of repair commands")

	cmd.RegisterNewGroup("storage commands", pCreate, read, pUpdate, pSet, del, twoPhaseDel, commit, abort, repair, markDelete, populatess)
}
