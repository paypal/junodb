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
  
package cluster

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/BurntSushi/toml"
	"juno/third_party/forked/golang/glog"
)

type Cluster struct {
	Config
	Zones            []*Zone
	RedistSingleZone bool // commit redist one zone only
	RedistZoneId     int  // zone selected for commit one zone.
}

type ClusterCache struct {
	Version     uint32
	ForRedist   bool
	ClusterInfo Config
	Zones       []Zone
}

type IWriter interface {
	Write(c *Cluster, version ...uint32) (err error)

	// write redistribution info
	WriteRedistInfo(c *Cluster, nc *Cluster) (err error)

	// write redistibution start/stop
	WriteRedistStart(c *Cluster, flag bool, zoneid int, src bool, ratelimit int) (err error)

	WriteRedistAbort(c *Cluster) (err error)

	WriteRedistResume(zoneid int, ratelimit int) (err error)
}

type IReader interface {
	// for proxy
	Read(c *Cluster) (version uint32, err error)

	// for storage server
	ReadWithRedistInfo(c *Cluster) (version uint32, err error)

	// for cluster manager
	ReadWithRedistNodeShards(c *Cluster) (err error)
}

func (c *Cluster) GetShards(zoneid uint32, nodeid uint32) (shards []uint32, err error) {
	if zoneid >= c.NumZones {
		return nil, errors.New("invalid zone id")
	}

	if nodeid >= c.Zones[zoneid].NumNodes {
		return nil, errors.New("invalid node id")
	}

	return c.Zones[zoneid].Nodes[nodeid].GetShards(), nil
}

func (c *Cluster) Validate() error {
	if c.NumZones == 0 {
		return errors.New("invalid config: zero NumZones")
	}

	if len(c.Zones) != int(c.NumZones) {
		return errors.New("invalid cluster config: Nodes does not match NumZones")
	}

	for i := 0; i < int(c.NumZones); i++ {
		if len(c.Zones[i].Nodes) == 0 || len(c.ConnInfo[i]) == 0 {
			return errors.New("missing node info")
		}

		if len(c.Zones[i].Nodes) < len(c.ConnInfo[i]) {
			return errors.New("Logical nodes do not match physical node info")
		}
	}

	return nil
}

func (c *Cluster) IsRedistZone(zoneid int) bool {
	if !c.RedistSingleZone || (c.RedistZoneId == zoneid) {
		return true
	}

	return false
}

func (c *Cluster) SetRedistZone(zoneid int) {
	if zoneid >= 0 {
		c.RedistSingleZone = true
		c.RedistZoneId = zoneid
	}
}

func (c *Cluster) Read(r IReader) (version uint32, err error) {
	if r == nil {
		return 0, errors.New("nil cluster reader")
	}

	return r.Read(c)
}

func (c *Cluster) ReadWithRetry(r IReader, cacheFile string, version uint32) (ver uint32, err error) {
	for i := 1; i < 6; i++ {
		ver, err = c.Read(r)
		if err == nil {
			return ver, nil
		}
		
		if len(cacheFile) > 0 {
			// Read from etcd cache.
			ver, err = c.ReadFromCache(cacheFile)
			if err == nil && ver == version {
				glog.Infof("Read from etcd cache.")
				return ver, nil
			}
		}

		if i < 5 {
			time.Sleep(time.Duration(i*1000+rand.Intn(2000)) * time.Millisecond)
		}
	}

	glog.Warningf("etcd error reading cluster info")
	return 0, errors.New("etcd error reading cluster info")
}

func (c *Cluster) ReadWithRedistInfo(r IReader) (version uint32, err error) {
	if r == nil {
		return 0, errors.New("nil cluster reader")
	}

	return r.ReadWithRedistInfo(c)
}

func (c *Cluster) ReadWithRedistNodeShards(r IReader) (err error) {
	if r == nil {
		return errors.New("nil cluster reader")
	}

	return r.ReadWithRedistNodeShards(c)
}

func (c *Cluster) Write(w IWriter, version ...uint32) (err error) {
	if w == nil {
		return errors.New("nil cluster writer")
	}

	if len(version) > 0 {
		return w.Write(c, version[0])
	} else {
		return w.Write(c)
	}
}

func (c *Cluster) WriteRedistInfo(w IWriter, nc *Cluster) (err error) {
	if w == nil {
		return errors.New("nil cluster writer")
	}

	return w.WriteRedistInfo(c, nc)
}

func (c *Cluster) WriteRedistAbort(w IWriter) (err error) {
	if w == nil {
		return errors.New("nil cluster writer")
	}

	return w.WriteRedistAbort(c)
}

func (c *Cluster) WriteRedistStart(w IWriter, flag bool, zoneid int, src bool, ratelimit int) (err error) {
	if w == nil {
		return errors.New("nil cluster writer")
	}

	return w.WriteRedistStart(c, flag, zoneid, src, ratelimit)
}

// used if reading cluster from config file
func (c *Cluster) PopulateFromConfig() (err error) {

	if err = c.Config.Validate(); err != nil {
		return
	}
	// Current zone slice is nil
	c.PopulateFromRedist(nil)

	return nil
}

// Populate shardmap for a config
// currZones is the existing.
func (c *Cluster) PopulateFromRedist(currZones []*Zone) {

	if IsNewMappingAlg() {

		if currZones != nil {
			numZones := int(c.NumZones)
			cutoff := make([]int, numZones)

			for i := 0; i < numZones; i++ {
				cutoff[i] = len(currZones[i].Nodes)
			}

			expected := NewZones(c.NumZones, c.NumShards, cutoff)
			ok := MatchZones(expected, currZones)

			if !ok {
				glog.Exitf("[ERROR] Current cluster info in etcd has a wrong version.")
				return
			}
		}

		cutoff := c.getCutoff()
		c.Zones = NewZones(c.NumZones, c.NumShards, cutoff)

		return
	}

	numZones := c.NumZones
	if currZones == nil {
		// Create a default
		currZones = make([]*Zone, numZones)
		for i := uint32(0); i < numZones; i++ {
			currZones[i] = &Zone{}
		}
	}

	workZones, filter := cloneZones(currZones)
	cutoff := make([]int, numZones)

	for i := uint32(0); i < numZones; i++ {
		cutoff[i] = len(c.ConnInfo[i]) - 1
	}

	c.Zones = filter.ExpandNodes(workZones, cutoff, c.NumShards)
}

func (c *Cluster) getCutoff() (cutoff []int) {

	numZones := int(c.NumZones)
	cutoff = make([]int, numZones)

	for i := 0; i < numZones; i++ {
		cutoff[i] = len(c.ConnInfo[i])
	}

	return cutoff
}

func (c *Cluster) MergeWith(other *Cluster) string {

	var skip []string = make([]string, 0, 10)
	for zoneid := 0; zoneid < int(c.NumZones); zoneid++ {

		numNodes := len(other.Zones[zoneid].Nodes)
		if c.Zones[zoneid] == nil {
			// Merge with current
			c.Zones[zoneid] = &Zone{
				Zoneid:   uint32(zoneid),
				NumNodes: uint32(numNodes),
				Nodes:    make([]Node, 0, numNodes),
			}
		}

		if c.ConnInfo[zoneid] == nil {
			c.ConnInfo[zoneid] = make([]string, 0, numNodes)
		}

		if len(c.Zones[zoneid].Nodes) > 0 {
			continue
		}

		// Merge with current
		c.ConnInfo[zoneid] = c.ConnInfo[zoneid][:0]
		c.ConnInfo[zoneid] = append(c.ConnInfo[zoneid], other.ConnInfo[zoneid][0:]...)
		c.Zones[zoneid].Nodes = append(c.Zones[zoneid].Nodes, other.Zones[zoneid].Nodes[0:]...)
		c.Zones[zoneid].NumNodes = uint32(numNodes)
		skip = append(skip, strconv.Itoa(zoneid))
	}

	if len(skip) > 0 {
		return fmt.Sprintf(">> zones skipped: %s", strings.Join(skip, ", "))
	}

	return ""
}

func writeToFile(filename string, buf *bytes.Buffer) (err error) {

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		glog.Errorf("%v", err)
		return err
	}
	defer f.Close()

	_, err = f.Write(buf.Bytes())
	if err != nil {
		glog.Errorf("%v", err)
	}

	return err
}

func (c *Cluster) WriteToCache(cachePath string, cacheName string, version uint32, forRedist bool) (err error) {

	filePrefix := filepath.Join(cachePath, cacheName)
	fileLock := fmt.Sprintf("%s.lck", filePrefix)

	fi, err := os.Create(fileLock)
	if err != nil {
		glog.Errorf("%v", err)
		return err
	}
	defer fi.Close()

	// Lock before write
	if !lockFile(int(fi.Fd()), syscall.LOCK_EX) {
		return nil // Done
	}
	defer unlockFile(int(fi.Fd()))

	var cache = ClusterCache{
		Version:     version,
		ForRedist:   forRedist,
		ClusterInfo: c.Config,
		Zones:       make([]Zone, 0, len(c.Zones)),
	}

	for i := 0; i < len(c.Zones); i++ {
		cache.Zones = append(cache.Zones, *c.Zones[i])
	}

	buf := new(bytes.Buffer)
	if err = toml.NewEncoder(buf).Encode(cache); err != nil {
		glog.Errorf("%v", err)
		return err
	}

	// Add a cache file with version in name.
	fname := fmt.Sprintf("%s.%d", filePrefix, version)

	if err = writeToFile(fname, buf); err != nil {
		return err
	}

	// Remove verison-2 cache file if any.
	if version > 2 {
		oldfile := fmt.Sprintf("%s.%d", filePrefix, version-2)
		err2 := os.Remove(oldfile)
		if os.IsExist(err2) && err2 != nil {
			glog.Errorf("%s: %v", oldfile, err2)
		}
	}

	cname := filepath.Clean(filepath.Join(cachePath, cacheName))
	// Remove file link
	err = os.Remove(cname)
	if os.IsExist(err) && err != nil {
		glog.Errorf("%v", err)
		return err
	}

	// Add new file link
	err = os.Symlink(fname, cname)
	if err != nil {
		glog.Errorf("%s: %v", fname, err)
	}

	return err
}

func (c *Cluster) ReadFromCache(cacheName string) (version uint32, err2 error) {

	var cache ClusterCache

	fileLock := fmt.Sprintf("%s.lck", cacheName)
	fi, err := os.Create(fileLock)
	if err != nil {
		glog.Errorf("%v", err)
		return 0, err
	}
	defer fi.Close()

	// Acquire a shared lock before read
	for i := 1; i < 4; i++ {
		time.Sleep(time.Second)
		if lockFile(int(fi.Fd()), syscall.LOCK_SH) {
			break
		}
		if i == 3 {
			return 0, errors.New("locked out")
		}
	}
	defer unlockFile(int(fi.Fd()))

	_, err = toml.DecodeFile(cacheName, &cache)
	if err != nil {
		glog.Errorf("[ERROR] Failed to read etcd cache.  %v", err)
		return 0, err
	}

	if cache.ClusterInfo.AlgVersion == 0 {
		cache.ClusterInfo.AlgVersion = 1
	}
	SetMappingAlg(cache.ClusterInfo.AlgVersion)

	c.Config = cache.ClusterInfo
	c.Zones = make([]*Zone, len(cache.Zones))
	for i := 0; i < len(cache.Zones); i++ {
		c.Zones[i] = &cache.Zones[i]
	}

	glog.Infof("Read etcd cache from %s", cacheName)
	if err = c.Config.Validate(); err != nil {
		glog.Errorf("Bad ConnInfo in etcd cache.")
		return 0, err
	}

	if !cache.ForRedist {
		ok := ValidateZones(c.Zones)
		if ok {
			glog.Infof("etcd cache validation passed.")
		} else {
			glog.Errorf("etcd cache validation failed.")
			return 0, errors.New("Zone validation failed in etcd cache.")
		}
	}

	return cache.Version, nil
}

func (c *Cluster) Log() {
	glog.Verbosef("num of shards: %d, num of zones: %d", c.NumShards, c.NumZones)
	glog.Verbosef("connInfo: %v", c.ConnInfo)
	for i := uint32(0); i < c.NumZones; i++ {
		c.Zones[i].Log()
	}
	glog.Flush()
}

func (c *Cluster) Dump(deep bool) {
	fmt.Printf("===================================\ncluster Info\n")
	c.Config.Dump()
	DisplayZones(c.Zones, "\nzone/node logical mapping:")

	if deep {
		fmt.Printf("\nshard map:")
		sm := NewShardMap(c)
		sm.Dump()
	}
	fmt.Printf("===================================\n")
}

func (c *Cluster) DumpChangeMap(newCluster *Cluster) {
	shards1 := NewShardMap(c).shards
	shards2 := NewShardMap(newCluster).shards

	for shardid := uint32(0); shardid < c.NumShards; shardid++ {
		fmt.Printf("\n shard %d: \t", shardid)
		for zoneid := uint32(0); zoneid < c.NumZones; zoneid++ {
			fmt.Printf("%d", shards1[shardid][zoneid].nodeid)
			if shards1[shardid][zoneid].isPrimary {
				fmt.Printf("*")
			}
			if shards1[shardid][zoneid] != shards2[shardid][zoneid] {
				fmt.Printf("-")
			}

			fmt.Printf("\t")
		}
		fmt.Printf("\t")
		for zoneid := uint32(0); zoneid < c.NumZones; zoneid++ {
			fmt.Printf("%d", shards2[shardid][zoneid].nodeid)
			if shards2[shardid][zoneid].isPrimary {
				fmt.Printf("*")
			}
			if shards1[shardid][zoneid] != shards2[shardid][zoneid] {
				fmt.Printf("+")
			}
			fmt.Printf("\t")
		}
	}
	fmt.Printf("\n")
}

func (c *Cluster) CreateShardMap() *ShardMap {
	return NewShardMap(c)
}
