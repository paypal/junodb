package cfg

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"time"

	"juno/cmd/proxy/config"
	"juno/pkg/cfg"
	"juno/pkg/client"
	"juno/pkg/cmd"
	"juno/pkg/etcd"
)

const (
	defaultJunoServerAddr  = "127.0.0.1:8080"
	defaultEtcdServerAddr  = "127.0.0.1:2379"
	defaultJunoClusterName = "junoserv"
)

type (
	cmdRuntimeConfig struct {
		cmd.Command
		optConfigFile      string
		optServerAddr      string
		optEtcdServerAddr  string
		optJunoClusterName string

		config       cfg.Config
		clientConfig client.Config
		etcdConfig   etcd.Config
	}
	cmdRuntimeConfigSet struct {
		cmdRuntimeConfig
		optReplace bool
	}
	cmdRuntimeConfigGet struct {
		cmdRuntimeConfig
	}
)

func (c *cmdRuntimeConfig) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.StringOption(&c.optServerAddr, "s|server", defaultJunoServerAddr, "specify Juno proxy server address")
	c.StringOption(&c.optEtcdServerAddr, "etcd-server", defaultEtcdServerAddr, "specify ETCD address")
	c.StringOption(&c.optConfigFile, "c|config", "", "specify toml configuration file name.")
	c.StringOption(&c.optJunoClusterName, "cluster-name", defaultJunoClusterName, "specify Juno ClusterName")
}

func (c *cmdRuntimeConfig) Parse(args []string) (err error) {
	if err = c.Option.Parse(args); err != nil {
		return
	}

	if len(c.optConfigFile) != 0 {
		if err = c.config.ReadFromTomlFile(c.optConfigFile); err != nil {
			return
		}
	}
	c.clientConfig.SetDefault()

	if cfg, e := c.config.GetConfig("Juno"); e == nil {
		cfg.WriteTo(&c.clientConfig)
		var clsName string
		if v := cfg.GetValue("ClusterName"); v != nil {
			clsName = v.(string)
		} else {
			if v := c.config.GetValue("ClusterName"); v != nil {
				clsName = v.(string)
			}
		}
		if len(clsName) != 0 && c.optJunoClusterName == defaultJunoClusterName {
			c.optJunoClusterName = clsName
		}
	}
	if len(c.clientConfig.Server.Addr) == 0 || c.optServerAddr != defaultJunoServerAddr {
		c.clientConfig.Server.SetFromConnString(c.optServerAddr)
	}
	c.clientConfig.Appname = "juno_rt_config"
	c.clientConfig.Namespace = config.JunoInternalNamespace()

	c.etcdConfig = etcd.DefaultConfig()
	if cfg, e := c.config.GetConfig("etcd"); e == nil {
		cfg.WriteTo(&c.etcdConfig)
	} else {
		err = e
		return
	}
	if len(c.etcdConfig.Endpoints) == 0 {
		c.etcdConfig.Endpoints = append(c.etcdConfig.Endpoints, c.optEtcdServerAddr)
	}
	return
}

func (c *cmdRuntimeConfigSet) Init(name string, desc string) {
	c.cmdRuntimeConfig.Init(name, desc)
	c.BoolOption(&c.optReplace, "replace", false,
		`If false, the new properties will be applied on top of the stored
	configuration in Juno. If true, previous stored configuration will
	be replaced.`)
}

func (c *cmdRuntimeConfigSet) Parse(args []string) (err error) {
	err = c.cmdRuntimeConfig.Parse(args)
	if len(c.optConfigFile) == 0 {
		return fmt.Errorf("configuration file is required")
	}
	return
}

func (c *cmdRuntimeConfigSet) Exec() {
	c.Validate()

	limits, err := c.config.GetConfig("limits")
	if err != err {
		fmt.Printf("failed to get limits config. %s\n", err.Error())
		return
	}

	var unified cfg.Config

	cli, err := client.New(c.clientConfig)
	if err != nil {
		fmt.Printf("failed to create Juno Client. err: %s\n", err.Error())
		return
	}

	if c.optReplace == false {
		if value, _, err := cli.Get(config.JunoInternalKeyForLimits()); err == nil && len(value) != 0 {
			if err := unified.ReadFromTomlBytes(value); err != nil {
				fmt.Printf("failed to decode value. err: %s\n", err.Error())
				return
			}
		} else if err != client.ErrNoKey {
			fmt.Printf("failed when getting limits from Juno. err: %s\n", err.Error())
			return
		}

		//unified.WriteToKVList(os.Stdout)
	}
	unified.Merge(&limits)

	now := time.Now().UnixNano()
	unified.SetKeyValue("Timestamp", now)
	//unified.WriteToKVList(os.Stdout)
	var buf bytes.Buffer
	unified.WriteToToml(&buf)
	_, err = cli.Set(config.JunoInternalKeyForLimits(), buf.Bytes(), client.WithTTL(math.MaxUint32))
	if err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("\nJuno:")
	fmt.Println("---------------------------------")
	fmt.Println(buf.String())
	fmt.Println("---------------------------------")

	if etcdcli := etcd.NewEtcdClient(&c.etcdConfig, c.optJunoClusterName); etcdcli != nil {
		value := fmt.Sprintf("%d", now)
		if err := etcdcli.PutValue(etcd.TagLimitsConfig, value); err != nil {
			fmt.Printf("failed to update limits change timestamp in ETCD: %s\n", err.Error())
		} else {
			fmt.Println("\nETCD:")
			fmt.Println("---------------------------------")
			fmt.Printf("%s%s_%s=%s\n", c.etcdConfig.EtcdKeyPrefix, c.optJunoClusterName, etcd.TagLimitsConfig, value)
			fmt.Println("---------------------------------")
		}
	}
}

func (c *cmdRuntimeConfigGet) Exec() {
	c.Validate()

	cli, err := client.New(c.clientConfig)
	if err != nil {
		fmt.Printf("failed to create Juno Client. err: %s\n", err.Error())
		return
	}

	var storedcfg cfg.Config
	if value, _, err := cli.Get(config.JunoInternalKeyForLimits()); err == nil && len(value) != 0 {
		if err := storedcfg.ReadFromTomlBytes(value); err != nil {
			fmt.Printf("failed to decode value. err: %s\n", err.Error())
			return
		}
	} else if err != client.ErrNoKey {
		fmt.Printf("failed when getting limits from Juno. err: %s\n", err.Error())
		return
	} else if err == client.ErrNoKey {
		fmt.Printf("limits not set in Juno yet\n")
		return
	}

	fmt.Println("\nJuno:")
	fmt.Println("---------------------------------")
	storedcfg.WriteToToml(os.Stdout)
	fmt.Println("---------------------------------")
	fmt.Println("")

	if etcdcli := etcd.NewEtcdClient(&c.etcdConfig, "cluster"); etcdcli != nil {
		if value, err := etcdcli.GetValue(etcd.TagLimitsConfig); err == nil {
			fmt.Println("\nETCD:")
			fmt.Println("---------------------------------")
			fmt.Printf("%s=%s\n", etcd.TagLimitsConfig, value)
			fmt.Println("---------------------------------")
		} else {
			fmt.Printf("failed to get limits change timestamp in ETCD: %s\n", err.Error())
		}
	}
}

func RegisterRtConfig() {
	set := &cmdRuntimeConfigSet{}
	set.Init("set", "configure runtime properties")
	get := &cmdRuntimeConfigGet{}
	get.Init("get", "show runtime properties")
	cmd.RegisterNewGroup("runtime configuration commands", set, get)
}
