package cmd

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/clustermgr/redistserv"
	"juno/pkg/cluster"
	"juno/pkg/etcd"
)

func GetStatus(configFile string) {

	LoadConfigOnly(configFile)
	etcdcli := etcd.NewEtcdClient(&cfg.Etcd, cfg.ClusterName)
	if etcdcli == nil {
		glog.Exit("[ERROR] failed to connect to etcd server")
	}

	defer etcdcli.Close()

	rw := etcd.NewEtcdReadWriter(etcdcli)
	var c cluster.Cluster
	version, err1 := c.Read(rw)
	summary, err2 := etcdcli.GetValue(etcd.TagRedistStateSummary)

	msg := "scale=NotFound&version=NotFound"
	numPorts := len(cfg.K8sClusterInfo.SSPorts)
	if err1 == nil && numPorts > 0 {
		n := len(c.Zones[0].Nodes)
		msg = fmt.Sprintf("scale=%d", n/numPorts)

		for i := 1; i < int(c.NumZones); i++ {
			msg += fmt.Sprintf(":%d", len(c.Zones[i].Nodes)/numPorts)
		}
		msg += fmt.Sprintf("&version=%d", version)
	}

	if err2 == nil {
		msg += fmt.Sprintf("&%s", summary)
	}
	fmt.Println(msg)
}

func LoadClusterInfo(configFile string) {
	LoadConfig(configFile)
	cli := etcd.NewEtcdClient(&cfg.Etcd, cfg.ClusterName)
	if cli == nil {
		glog.Exit("failed to connect to etcd server")
	}
	defer cli.Close()

	rw := etcd.NewEtcdReadStdoutWriter(cli, cfg.ClusterName)
	var cl cluster.Cluster
	version, _ := cl.Read(rw)
	//version, _ := cl.ReadWithRedistInfo(rw)
	cl.Write(rw, version)
}

func StoreClusterInfo(configFile string, dryrun bool, verbose bool) {
	LoadConfig(configFile)

	cluster.SetMappingAlg(clusterInfo[0].AlgVersion)
	clusterInfo[0].PopulateFromConfig()

	clusterInfo[0].Dump(verbose)
	if cluster.ValidateZones(clusterInfo[0].Zones) {
		fmt.Printf("Passed validation.\n\n")
	} else {
		fmt.Printf("[ERROR] Failed validation.\n\n")
		return
	}

	var writer cluster.IWriter
	etcdcli := etcd.NewEtcdClient(&cfg.Etcd, cfg.ClusterName)

	if dryrun {
		writer = etcd.NewEtcdReadStdoutWriter(etcdcli, cfg.ClusterName)
	} else {
		if etcdcli == nil {
			glog.Exit("can't connect to etcd server")
		}

		defer etcdcli.Close()
		writer = etcd.NewEtcdReadWriter(etcdcli)
	}

	version := uint32(0)
	if etcdcli != nil {
		version, _ = etcdcli.GetUint32(etcd.TagVersion)
	}

	if version > 0 {
		glog.Exit("[ERROR] store command is not allowed to update existing cluster info in etcd.  Contact Juno developers for help.")
	}

	// Add root key.
	addRootKey(&cfg, dryrun)

	version = 1

	if clusterInfo[0].Write(writer, version) != nil {
		glog.Exit("[ERROR] store command failed.")
	}
	glog.Infof(">> store command succeeded.")
}

func StoreClusterUpdate(configFile string, newConfigFile string, dryrun bool) {
	LoadConfig(configFile)

	cluster.SetMappingAlg(clusterInfo[0].AlgVersion)
	clusterInfo[0].PopulateFromConfig()

	LoadNewConfig(newConfigFile)
	clusterInfo[1].PopulateFromConfig()

	var writer cluster.IWriter
	etcdcli := etcd.NewEtcdClient(&cfg.Etcd, cfg.ClusterName)

	if dryrun {
		writer = etcd.NewEtcdReadStdoutWriter(etcdcli, cfg.ClusterName)
	} else {
		if etcdcli == nil {
			glog.Exit(errors.New("can't connect to etcd server"))
		}
		defer etcdcli.Close()
		writer = etcd.NewEtcdReadWriter(etcdcli)
	}

	fmt.Printf("------------------\n")
	fmt.Printf("redist info:")
	clusterInfo[0].WriteRedistInfo(writer, &clusterInfo[1])
}

func RedistAutoNoCommit(newConfig string, zoneSelected int, skipZone int, dryrun bool,
	maxFailures int, maxWait int, forTest bool, ratelimit int, markdown bool) {

	// (1) Load new config from a file
	LoadNewConfig(newConfig)

	// (2) Setup etcdcli
	etcdcli := etcd.NewEtcdClient(&newCfg.Etcd, newCfg.ClusterName)
	if etcdcli == nil {
		glog.Exit("can't connect to etcd server")
	}
	defer etcdcli.Close()
	rw := etcd.NewEtcdReadWriter(etcdcli)

	// (3) Do prepare if needed.
	var key = etcd.KeyRedistEnable(0)
	val, err := etcdcli.GetValue(key)
	if err != nil &&
		val != etcd.NotFound {
		glog.Exit("[ERROR] redist command failed.")
	}

	needPrepare := true
	if strings.Index(val, etcd.TagRedistEnabledReady) == 0 ||
		strings.Index(val, etcd.TagRedistEnabledSource) == 0 ||
		strings.Index(val, etcd.TagRedistEnabledTarget) == 0 ||
		strings.Index(val, etcd.TagRedistResume) == 0 ||
		strings.Index(val, etcd.TagRedistResumeRL) == 0 ||
		strings.Index(val, etcd.TagRedistAbortZone) == 0 {
		needPrepare = false
	}

	if needPrepare {
		// Do prepare for the first time.
		if !RedistPrepare(newConfig, zoneSelected, dryrun, false /*swaphost*/) {
			glog.Exit("[ERROR] prepare step failed.")
		}
		if dryrun {
			return
		}
	} else {
		_, algver, err := etcdcli.GetVersion()
		if err != nil {
			glog.Exit("[ERROR] redist command failed.")
		}
		if algver != clusterInfo[1].AlgVersion {
			glog.Errorf("[ERROR] AlgVersion: (curr=%d, new=%d) mismatch.",
				algver, clusterInfo[1].AlgVersion)
			glog.Exit("[ERROR] redist command failed.")
		}
	}

	// (4) Loop all zones to enable redist.
	glog.Info("Enable redist, and wait for finish_snapshot state ...")

	for zoneid := 0; zoneid < int(clusterInfo[1].NumZones); zoneid++ {

		// Enable one zone only.
		if zoneSelected >= 0 && zoneid != zoneSelected {
			continue
		}
		skip := ((zoneSelected < 0) && (zoneid == skipZone))
		err := rw.WaitforFinishState(zoneid, skip, maxFailures, maxWait, false, forTest, ratelimit, markdown)
		if err != nil {
			glog.Exit("[ERROR] wait for finish_snapshot step failed.")
		}
	}
}

func RedistAuto(newConfig string, zoneSelected int, skipZone int, dryrun bool,
	maxFailures int, maxWait int, forTest bool, ratelimit int, markdown bool) {

	RedistAutoNoCommit(newConfig, zoneSelected, skipZone, dryrun, maxFailures, maxWait, forTest, ratelimit, markdown)

	// (5) Commit the new config.
	RedistCommit(newConfig, zoneSelected, dryrun, false, 0, markdown)
}

func SwapHost(newConfig string) {

	// Load new config from a file
	LoadNewConfig(newConfig)

	// Do prepare.
	if !RedistPrepare(newConfig, -1, false, true /*swaphost*/) {
		glog.Exit("[ERROR] swaphost command failed.")
	}

	RedistCommit(newConfig, -1, false /*dryrun*/, false /*waitForFinish*/, 0, false /*markdown*/)
	glog.Infof(">> swaphost command succeeded.")
}

func getEtcdWriter(newConfig string, dryrun bool) (writer cluster.IWriter, etcdcli *etcd.EtcdClient) {
	// (1) Load new config from a file.
	LoadNewConfig(newConfig)

	var rw *etcd.EtcdReadWriter
	etcdcli = etcd.NewEtcdClient(&newCfg.Etcd, newCfg.ClusterName)

	if etcdcli == nil {
		glog.Exit("[ERROR]failed to connect to ETCD.")
	}
	rw = etcd.NewEtcdReadWriter(etcdcli)

	if dryrun {
		writer = etcd.NewEtcdReadStdoutWriter(etcdcli, newCfg.ClusterName)
	} else {
		writer = rw
	}
	return writer, etcdcli
}

func RedistResume(newConfig string, zoneid int, dryrun bool, ratelimit int) {
	writer, cli := getEtcdWriter(newConfig, dryrun)
	defer cli.Close()

	if writer.WriteRedistResume(zoneid, ratelimit) != nil {
		glog.Exit("[ERROR] Redist resume failed.")
	}

	glog.Info(">> Resume started")
}

func RedistAbort(newConfig string, dryrun bool) {

	writer, cli := getEtcdWriter(newConfig, dryrun)
	defer cli.Close()

	if clusterInfo[1].WriteRedistAbort(writer) != nil {
		glog.Exit("[ERROR] abort step failed.")
	}

	glog.Info(">> abort step succeeded.")
	// abort always removes mark down
	err := cli.DeleteKey(etcd.TagZoneMarkDown)
	if err != nil {
		glog.Infof("remove zone Markdown failed")
	}
}

func RedistStart(configFile string, flag bool, zoneid int, src bool, dryrun bool, ratelimit int, markdown bool) {
	LoadConfig(configFile)
	clusterInfo[0].PopulateFromConfig()

	if zoneid < 0 || zoneid >= int(clusterInfo[0].NumZones) {
		glog.Errorf("[ERROR] zone is not set in command line or outside the range [0, %d).\n",

			clusterInfo[0].NumZones)
		return
	}

	var writer cluster.IWriter
	etcdcli := etcd.NewEtcdClient(&cfg.Etcd, cfg.ClusterName)

	if dryrun {
		writer = etcd.NewEtcdReadStdoutWriter(etcdcli, cfg.ClusterName)
	} else {
		if etcdcli == nil {
			glog.Exit(errors.New("can't connect to etcd server"))
		}
		defer etcdcli.Close()
		writer = etcd.NewEtcdReadWriter(etcdcli)
	}

	clusterInfo[0].WriteRedistStart(writer, flag, zoneid, src, ratelimit)

	if markdown && src {
		etcdcli.PutValue(etcd.TagZoneMarkDown, strconv.Itoa(zoneid), 2)
	}
}

func RedistPrepare(newConfig string, zoneSelected int, dryrun bool, swaphost bool) bool {

	// (1) Load new config from a file
	LoadNewConfig(newConfig)

	// (2) Read existing config from etcd
	etcdcli := etcd.NewEtcdClient(&newCfg.Etcd, newCfg.ClusterName)
	if etcdcli == nil {
		glog.Errorf("[ERROR] prepare step failed.")
		return false
	}
	defer etcdcli.Close()

	rw := etcd.NewEtcdReadWriter(etcdcli)
	_, err := clusterInfo[0].Read(rw)
	if err != nil {
		glog.Errorf("[ERROR] Failed to get current config from etcd.\n")
		return false
	}

	cluster.SetMappingAlg(clusterInfo[0].AlgVersion)

	if clusterInfo[0].AlgVersion != clusterInfo[1].AlgVersion {
		glog.Errorf("[ERROR] AlgVersion: (curr=%d, new=%d) mismatch.\n",
			clusterInfo[0].AlgVersion, clusterInfo[1].AlgVersion)
		return false
	}

	if clusterInfo[0].NumZones != clusterInfo[1].NumZones {
		glog.Errorf("[ERROR] NumZones: curr=%d, new=%d) mismatch.\n",
			clusterInfo[0].NumZones, clusterInfo[1].NumZones)
		return false
	}

	if clusterInfo[0].NumShards != clusterInfo[1].NumShards {
		glog.Errorf("[ERROR] NumShards: (curr=%d, new=%d) mismatch.\n",
			clusterInfo[0].NumShards, clusterInfo[1].NumShards)
		return false
	}

	// Display and validate.
	fmt.Printf("\nCurrent cluster info:\n")
	clusterInfo[0].Dump(false)

	err = clusterInfo[0].Validate()
	if err != nil {
		fmt.Printf("[ERROR] Failed validation, %s", err)
		return false
	}

	if cluster.ValidateZones(clusterInfo[0].Zones) {
		fmt.Printf("Passed validation.\n\n")
	} else {
		fmt.Printf("[ERROR] Failed validation.\n\n")
		return false
	}

	// Option to prepare redist one zone only.
	if zoneSelected >= int(clusterInfo[0].NumZones) {
		glog.Errorf("[ERROR] zone set in command line is outside the range [0, %d).\n",
			clusterInfo[0].NumZones)
		return false
	}
	clusterInfo[1].SetRedistZone(zoneSelected)

	// (3) Populate new shardmap
	tail := ""
	if zoneSelected >= 0 {
		tail = fmt.Sprintf("\n zoneid selected for commit: %d", zoneSelected)
	}
	header := fmt.Sprintf("Uncommitted new cluster info: %s", tail)

	clusterInfo[1].PopulateFromRedist(clusterInfo[0].Zones)
	cluster.DisplayZones(clusterInfo[1].Zones, header)

	if cluster.ValidateZones(clusterInfo[1].Zones) {
		fmt.Printf("Passed validation.\n\n")
	} else {
		fmt.Printf("[ERROR] Failed validation.\n\n")
		return false
	}

	if swaphost &&
		!cluster.MatchZones(clusterInfo[1].Zones, clusterInfo[0].Zones) {
		return false
	}

	// Write redist info.
	var writer cluster.IWriter
	if dryrun {
		writer = etcd.NewEtcdReadStdoutWriter(etcdcli, newCfg.ClusterName)
	} else {
		writer = rw
	}

	if clusterInfo[0].WriteRedistInfo(writer, &clusterInfo[1]) != nil {
		glog.Errorf("[ERROR] prepare step failed.")
		return false
	}

	glog.Infof(">> prepare step succeeded.")
	return true
}

func RedistCommit(newConfig string, zoneSelected int, dryrun bool,
	waitForFinish bool, maxFailures int, markdown bool) {

	// (1) Load new config from a file.
	LoadNewConfig(newConfig)

	etcdcli := etcd.NewEtcdClient(&newCfg.Etcd, newCfg.ClusterName)
	if etcdcli == nil {
		glog.Exit("[ERROR] redist command failed.")
	}
	defer etcdcli.Close()

	// (2) Read current config from etcd.
	rw := etcd.NewEtcdReadWriter(etcdcli)
	version, err := clusterInfo[0].Read(rw)
	if err != nil {
		glog.Exit("[ERROR] redist command failed.")
	}

	cluster.SetMappingAlg(clusterInfo[0].AlgVersion)

	if clusterInfo[0].AlgVersion != clusterInfo[1].AlgVersion {
		glog.Exitf("[ERROR] AlgVersion: (curr=%d, new=%d) mismatch.\n",
			clusterInfo[0].AlgVersion, clusterInfo[1].AlgVersion)
		return
	}

	// Option to commit redist one zone only.
	if zoneSelected >= int(clusterInfo[0].NumZones) {
		glog.Exitf("[ERROR] zone set in command line is outside the range [0, %d).\n",
			clusterInfo[0].NumZones)
	}
	clusterInfo[1].SetRedistZone(zoneSelected)

	// (3) Read uncommited shardmap from etcd.
	if clusterInfo[1].ReadWithRedistNodeShards(rw) != nil {
		glog.Exit("[ERROR] redist command failed.")
	}

	// (4) Merge with current config if needed.
	skip := clusterInfo[1].MergeWith(&clusterInfo[0])

	// Display and verify
	fmt.Printf("\nCurrent cluster info:\n")
	clusterInfo[0].Config.Dump()

	fmt.Printf("\nNew cluster info:\n")
	clusterInfo[1].Dump(false)

	err = clusterInfo[1].Validate()
	if err != nil {
		fmt.Printf("[ERROR] Failed validation, %s", err)
		return
	}

	if cluster.ValidateZones(clusterInfo[1].Zones) {
		fmt.Printf("Passed validation.\n\n")
	} else {
		fmt.Printf("[ERROR] Failed validation.\n\n")
		return
	}

	version += 1

	if waitForFinish {
		glog.Info("Wait for finish_snapshot state ...")

		for zoneid := 0; zoneid < int(clusterInfo[1].NumZones); zoneid++ {

			// Wait for one zone only.
			if zoneSelected >= 0 && zoneid != zoneSelected {
				continue
			}
			if err := rw.WaitforFinishState(zoneid, false, maxFailures, 10, true, false, 0, markdown); err != nil {
				glog.Exit("[ERROR] wait for finish_snapshot step failed.")
			}

		}
	}

	if rw.DumpRedistState() != nil {
		glog.Exit("[ERROR] redist command failed.")
	}

	// Add root key.
	addRootKey(&newCfg, dryrun)

	// (5) Store new config.
	var writer cluster.IWriter
	if dryrun {
		writer = etcd.NewEtcdReadStdoutWriter(etcdcli, newCfg.ClusterName)
	} else {
		writer = rw
	}

	if clusterInfo[1].Write(writer, version) != nil {
		glog.Exit("[ERROR] redist command failed.")
	}

	if skip != "" {
		glog.Infof(skip)
	}

	glog.Infof(">> redist command succeeded.")

	// commit always removes mark down
	err = etcdcli.DeleteKey(etcd.TagZoneMarkDown)
	if err != nil {
		glog.Infof("remove zone Markdown failed")
	}
}

func addRootKey(cfg *Config, dryrun bool) {

	now := time.Now().String()[:19] // YYYY-MM-DD HH:MM:SS
	host, err := os.Hostname()
	if err != nil {
		host = ""
	}
	pwd, err := os.Getwd()
	if err != nil {
		pwd = ""
	}
	val := fmt.Sprintf("%s|%s|%s", now, host, pwd)

	if dryrun {
		fmt.Printf("%sroot_%s=%s\n", cfg.Etcd.EtcdKeyPrefix, cfg.ClusterName, val)
	} else {

		cli := etcd.NewEtcdClient(&cfg.Etcd, "root")
		if cli == nil {
			glog.Exit("can't connect to etcd server")
			return
		}
		defer cli.Close()

		if cli.PutValue(cfg.ClusterName, val) != nil {
			glog.Exit("[ERROR] add root key failed.")
		}
	}
}

func RestoreCache(configFile string, cacheName string, dryrun bool) {
	LoadConfig(configFile)

	version, err := clusterInfo[0].ReadFromCache(cfg.Etcd.CacheName)
	if err != nil {
		glog.Exit("[ERROR] restore command failed.")
	}
	var writer cluster.IWriter

	etcdcli := etcd.NewEtcdClient(&cfg.Etcd, cfg.ClusterName)
	if etcdcli == nil {
		glog.Exit("[ERROR] restore command failed.")
	}
	defer etcdcli.Close()

	if dryrun {
		writer = etcd.NewEtcdReadStdoutWriter(etcdcli, cfg.ClusterName)
	} else {
		writer = etcd.NewEtcdReadWriter(etcdcli)
	}

	// Add root key.
	addRootKey(&cfg, dryrun)

	if clusterInfo[0].Write(writer, version) != nil {
		glog.Exit("[ERROR] restore command failed.")
	}

	glog.Infof(">> restore command succeeded.")
}

func ZoneMarkDown(configFile string, flagType string, zoneid int) {
	LoadConfig(configFile)

	etcdcli := etcd.NewEtcdClient(&cfg.Etcd, cfg.ClusterName)
	if etcdcli == nil {
		glog.Exit("[ERROR] prepare step failed.")
	}
	defer etcdcli.Close()

	maxretry := 2

	if flagType == "set" {
		err := etcdcli.PutValue(etcd.TagZoneMarkDown, strconv.Itoa(zoneid), maxretry)
		if err != nil {
			glog.Exit("[ERROR] set zone markdown failed")
		}

		if zoneid == -1 {
			glog.Info("Zone markdown removed")
		} else {
			glog.Infof("Zone markdown set for zone %d", zoneid)
		}
	} else if flagType == "delete" {
		err := etcdcli.DeleteKey(etcd.TagZoneMarkDown)
		if err != nil {
			glog.Exit("[ERROR] remove zone markdown faield")
		}

		glog.Info("zone markdown removed")
	} else {
		val, err := etcdcli.GetValue(etcd.TagZoneMarkDown)
		if err != nil {
			glog.Exit("", err)
		}

		glog.Infof("Zone markdown, zoneid=%s", val)
	}
}

func RedistServ(newConfig string, spawnProcess bool) {
	exe, _ := os.Executable()
	pidFile := filepath.Join(filepath.Dir(exe), "redistserv.pid")

	if spawnProcess {
		getPidAndPort := func() (int, int) {
			pid, port := 0, 0
			file, err := os.Open(pidFile)
			if err != nil {
				if !os.IsNotExist(err) {
					glog.Error(err)
				}
			} else {
				scanner := bufio.NewScanner(file)
				if scanner.Scan() {
					pid, _ = strconv.Atoi(scanner.Text())
				}
				if scanner.Scan() {
					port, _ = strconv.Atoi(scanner.Text())
				}

				process, _ := os.FindProcess(pid)
				if err = process.Signal(syscall.Signal(0)); err != nil {
					pid, port = 0, 0
				}

				/*client := http.Client{
					Timeout: 1 * time.Second,
				}
				if _, err = client.Head(fmt.Sprintf("http://localhost:%d", port)); err != nil {
					pid, port = 0, 0
				}*/
			}

			return pid, port
		}

		pid, port := getPidAndPort()
		if pid == 0 {
			c := exec.Command(exe, "--new_config", newConfig, "--cmd", "redistserv")
			c.Stdout = os.Stdout
			c.Stderr = os.Stderr
			c.SysProcAttr = &syscall.SysProcAttr{
				Setpgid: true,
			}
			if err := c.Start(); err != nil {
				glog.Error(err)
			}
		}
		time.Sleep(1 * time.Second)

		pid, port = getPidAndPort()
		if pid == 0 || port == 0 {
			glog.Error("Failed to start redist watch server")
		}
	} else {
		LoadNewConfig(newConfig)
		redistserv.Run(&newCfg.RedistServ, &newCfg.Etcd, newCfg.ClusterName, pidFile)
	}

}
