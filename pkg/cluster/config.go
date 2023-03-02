package cluster

import (
	"errors"
	"fmt"
)

type Config struct {
	AlgVersion uint32
	NumZones   uint32
	NumShards  uint32
	ConnInfo   [][]string

	//SSHosts and SSPorts are used to generate ConnInfo ONLY when ConnInfo not defined
	SSHosts [][]string
	SSPorts []uint16
}

type IConfigRepo interface {
	GetClusterConfig(c *Config) error
}

func (c *Config) Read(repo IConfigRepo) error {
	return repo.GetClusterConfig(c)
}

func (c *Config) Validate() error {
	if c.AlgVersion > 2 || c.AlgVersion < 0 {
		return errors.New("Invalid config: wrong alg version")
	}
	if c.NumZones == 0 {
		return errors.New("invalid config: zero NumZones")
	}
	if len(c.ConnInfo) == 0 {
		if len(c.SSHosts) == int(c.NumZones) && len(c.SSPorts) != 0 {
			c.ConnInfo = make([][]string, c.NumZones)
			for i := 0; i < int(c.NumZones); i++ {
				for _, host := range c.SSHosts[i] {
					for _, port := range c.SSPorts {
						c.ConnInfo[i] = append(c.ConnInfo[i], fmt.Sprintf("%s:%d", host, port))
					}
				}
			}
		}

	}
	if len(c.ConnInfo) != int(c.NumZones) {
		return errors.New("Invalid config: ConnInfo length does not match NumZones.")
	}

	unique := make(map[string]int)
	for i := 0; i < len(c.ConnInfo); i++ {
		for _, ip := range c.ConnInfo[i] {
			if len(ip) <= 2 || ip[len(ip)-1] == ':' || ip[0] == ':' {
				return errors.New(fmt.Sprintf("Invalid config: zone %d contains bad ip:port address.", i))
			}
			_, exist := unique[ip]
			if exist {
				return errors.New(fmt.Sprintf("Invalid config: ip:port address %s is not unique.", ip))
			}
			unique[ip] = 1
		}
	}

	maxNumHosts := c.GetMaxNumHostsPerZone()
	if int(c.NumShards) < maxNumHosts {
		return errors.New("Invalid config: NumShards too small.")
	}
	return nil
}

func (c *Config) GetMaxNumHostsPerZone() int {
	maxNumHosts := len(c.ConnInfo[0])
	for i := 1; i < int(c.NumZones); i++ {
		if maxNumHosts < len(c.ConnInfo[i]) {
			maxNumHosts = len(c.ConnInfo[i])
		}
	}
	return maxNumHosts
}

func (c *Config) Dump() {
	fmt.Printf("alg version: %d; num of shards: %d; num of zones: %d\n",
		c.AlgVersion, c.NumShards, c.NumZones)
	fmt.Printf("\nzone/node physical mapping:\n")
	fmt.Printf(" zoneid\tnodeid\tip:port\n")
	for i := 0; i < len(c.ConnInfo); i++ {
		if c.ConnInfo[i] == nil {
			continue
		}
		for j := 0; j < len(c.ConnInfo[i]); j++ {
			fmt.Printf("%3d\t%4d\t%s\n", i, j, c.ConnInfo[i][j])
		}
	}
}

type StatsConfig struct {
	TimeoutStatsEnabled    bool
	RespTimeStatsEnabled   bool
	MarkdownThreashold     uint32
	MarkdownExpirationBase uint32
	EMARespTimeWindowSize  uint32
	TimeoutWindowSize      uint32
	TimeoutWindowUint      uint32
}

var (
	DefaultStatsConfig = StatsConfig{
		TimeoutStatsEnabled:    false,
		RespTimeStatsEnabled:   false,
		MarkdownThreashold:     10,
		MarkdownExpirationBase: 5 * 60,
		EMARespTimeWindowSize:  39,
		TimeoutWindowSize:      5,
		TimeoutWindowUint:      60,
	}
)
