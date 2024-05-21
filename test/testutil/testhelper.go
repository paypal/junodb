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

package testutil

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"fmt"
	"net"
	"os"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/paypal/junodb/cmd/proxy/config"
	"github.com/paypal/junodb/internal/cli"
	"github.com/paypal/junodb/pkg/client"
	"github.com/paypal/junodb/pkg/cluster"
	"github.com/paypal/junodb/pkg/etcd"
	"github.com/paypal/junodb/pkg/io"
	"github.com/paypal/junodb/pkg/util"
	"github.com/paypal/junodb/test/testutil/mock"
	"github.com/paypal/junodb/test/testutil/server"
)

type KVMap map[string]string

type TestConfig struct {
	gconfig  KVMap
	sconfigs map[string]KVMap
}

var dirConfig = server.ClusterConfig{
	ProxyAddress: io.ServiceEndpoint{Addr: "127.0.0.1:8080"},
}

/**************************************************************
 *  Note: add/remove host test case depends on sharding info
 *  which depends on host/port config, please keep host number
 *  and port assign sequence same if you change host/port
 **************************************************************/
func NewTestConfig() *TestConfig {
	return &TestConfig{
		gconfig:  make(KVMap),
		sconfigs: make(map[string]KVMap),
	}
}

func (tc *TestConfig) CreateTomlFile(fname string) error {
	f, err := os.Create(fname)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	for k, v := range tc.gconfig {
		w.WriteString(k)
		w.WriteString("=")
		w.WriteString(v)
		w.WriteString("\n")
	}

	for section, kvmap := range tc.sconfigs {
		w.WriteString("\n[")
		w.WriteString(section)
		w.WriteString("]\n")

		for k, v := range kvmap {
			w.WriteString(k)
			w.WriteString("=")
			w.WriteString(v)
			w.WriteString("\n")
		}
	}

	w.Flush()
	return nil
}

func (tc *TestConfig) DeleteTomlFile(fname string) error {
	return os.Remove(fname)
}

func (tc *TestConfig) AddConfig(section string, name string, value string) {

	if section == "" {
		if tc.gconfig == nil {
			tc.gconfig = make(KVMap)
		}
		tc.gconfig[name] = value
		return
	}

	if tc.sconfigs == nil {
		tc.sconfigs = make(map[string]KVMap)
	}

	if tc.sconfigs[section] == nil {
		tc.sconfigs[section] = make(KVMap)
	}

	tc.sconfigs[section][name] = value
}

func (tc *TestConfig) Dump() {
	fmt.Println(tc)

}

func (tc *TestConfig) Reset() {
	tc.gconfig = nil
	tc.sconfigs = nil
}

type TestServer struct {
	port              string
	ss_exename        string
	tconfig           TestConfig
	tconfig_file_name string
}

func NewTestServer(config_file_name string, ss string, proxy_port string) *TestServer {
	ts := &TestServer{
		port:              proxy_port,
		ss_exename:        ss,
		tconfig_file_name: config_file_name,
	}

	// default config for test proxy
	connInfoValue :=
		`[["localhost:5010", "localhost:5011", "localhost:5012"],
		["localhost:6010", "localhost:6011", "localhost:6012"],
		["localhost:7010", "localhost:7011", "localhost:7012"],
		["localhost:8010", "localhost:8011", "localhost:8012"],
		["localhost:9010", "localhost:9011", "localhost:9012"]]`

	ts.AddConfig("", "ConnInfo", connInfoValue)
	ts.AddConfig("", "NumZones", "5")
	ts.AddConfig("", "NumConnPerSS", "1")
	ts.AddConfig("Inbound", "ReadTimeout", `"1000ms"`)
	ts.AddConfig("Inbound", "WriteTimeout", `"200ms"`)

	return ts
}

func NewTestServerWithConnectInfo(config_file_name string, ss string, proxy_port string, conn_info string, num_zones string) *TestServer {
	ts := &TestServer{
		port:              proxy_port,
		ss_exename:        ss,
		tconfig_file_name: config_file_name,
	}

	ts.AddConfig("", "ConnInfo", conn_info)
	ts.AddConfig("", "NumZones", num_zones)
	ts.AddConfig("", "NumConnPerSS", "1")
	ts.AddConfig("Inbound", "ReadTimeout", `"1000ms"`)
	ts.AddConfig("Inbound", "WriteTimeout", `"200ms"`)

	return ts
}

func (ts *TestServer) AddConfig(section string, name string, value string) {
	ts.tconfig.AddConfig(section, name, value)
}

func exe_cmd(cmd string, wg *sync.WaitGroup, shell bool) {
	if wg != nil {
		defer wg.Done()
	}

	//fmt.Println("execute command here:" + cmd)
	if shell {
		if err := exec.Command("bash", "-c", cmd).Run(); err != nil {
			fmt.Printf("%s\n", err)
		}
	} else {
		parts := strings.Fields(cmd)
		head := parts[0]
		parts = parts[1:len(parts)]
		if err := exec.Command(head, parts...).Run(); err != nil {
			//fmt.Printf("%s\n", err)
		}
	}
}

/************************************
 * Function to generate a random key
 ************************************/
func GenerateRandomKey(n int) []byte {
	b := make([]byte, n)
	_, err := rand.Read(b)
	// Note that err == nil only if we read len(b) bytes.
	if err != nil {
		return []byte("error")
	}
	return b
}

func ApproxEqual(v1 uint32, v2 uint32, epsilon uint32) bool {
	if v1 > v2 {
		return (v1 - v2) <= epsilon
	}
	return (v2 - v1) <= epsilon
}

// This function has to be called right after getting RecordInfo
func VerifyRecordInfo(recInfo client.IContext, ver uint32, ttl uint32, creationTime uint32) error {
	if recInfo == nil {
		return fmt.Errorf("nil recInfo")
	}
	now := time.Now()
	if recInfo.GetVersion() != ver {
		return fmt.Errorf("wrong version %d. expected %d", recInfo.GetVersion(), ver)
	}
	if creationTime == 0 {
		creationTime = uint32(now.Unix())
	}
	if ttl == 0 {
		ttl = uint32(config.Conf.DefaultTimeToLive)
	}
	if !ApproxEqual(recInfo.GetCreationTime(), creationTime, 2) {
		return fmt.Errorf("wrong creation time %d. expected %d", recInfo.GetCreationTime(), creationTime)
	}
	if !ApproxEqual(util.GetExpirationTimeFrom(now, recInfo.GetTimeToLive()), util.GetExpirationTimeFrom(time.Unix(int64(creationTime), 0), ttl), 2) {
		return fmt.Errorf("wrong TTL.")
	}
	return nil
}

func CreateAndValidate(c client.IClient, key []byte,
	inputData []byte, lifetime uint32, expect_err error) error {
	start := uint32(time.Now().Unix())

	recInfo, err := c.Create(key, inputData, client.WithTTL(lifetime))
	if err != expect_err {
		if err == nil {
			return fmt.Errorf("expected create to return %s. returned nil", expect_err)
		} else if expect_err == nil {
			return fmt.Errorf("expected create to return nil. returned %s", err)
		} else {
			return fmt.Errorf("expected create to return %s. returned %s", expect_err, err.Error())
		}
	}

	if err != nil { ///failed but as expected. err == expect_err
		return nil
	}

	if err = VerifyRecordInfo(recInfo, 1, lifetime, start); err != nil {
		return err
	}

	value, recInfo, err := c.Get([]byte(key))

	if err != nil {
		return fmt.Errorf("get after create fail with error: %s", err)
	}
	if err = VerifyRecordInfo(recInfo, 1, lifetime, start); err != nil {
		return err
	}

	if bytes.Compare(value, inputData) != 0 {
		fmt.Printf("value: %x\n", value)
		fmt.Printf("inputData: %x\n", inputData)
		return fmt.Errorf("data value != inputData")
	}

	glog.Debugf("Version no from get inside of createAndValidate is %d\nAll create validations passed!!",
		recInfo.GetVersion())

	return nil
}

/***********************************************************
 * Function to set and get record to do needed validation
 ***********************************************************/
func SetAndValidate(c client.IClient, key []byte,
	inputData []byte, lifetime uint32, expect_err error) error {

	var oldVersion uint32
	var compareLifetime uint32
	/* If no record and pass in lifetime is 0, it sets lifetime with default value:100
	* If there is a record and pass in lifetime is 0, it sets lifetime with the existing value
	 */
	value, recInfo, err := c.Get([]byte(key))
	if err != nil { //it has no record yet
		oldVersion = 0
		if lifetime == 0 { //if no record, lifetime 0 will be default 100
			compareLifetime = 1800
		} else {
			compareLifetime = lifetime
		}
	} else { //if it has record, lifetime 0 means it will keep the original lifetime value
		oldVersion = recInfo.GetVersion()
		if lifetime < recInfo.GetTimeToLive() {
			compareLifetime = recInfo.GetTimeToLive()
		} else {
			compareLifetime = lifetime
		}
	}

	time.Sleep(200 * time.Millisecond)

	recInfo, err = c.Set(key, inputData, client.WithTTL(lifetime))
	if err != expect_err {
		if err == nil {
			return fmt.Errorf("Set returned nil. expected to return %s", expect_err)
		} else if expect_err == nil {
			return fmt.Errorf("Set returned %s. expected to return nil", err)
		} else {
			return fmt.Errorf("Set returned %s. expected to return %s", err, expect_err)
		}
	}
	if err == nil {
		value, recInfo, err = c.Get([]byte(key))
		if err != nil {
			return fmt.Errorf("get after set fail with error: %s", err)
		}

		if bytes.Compare(value, inputData) != 0 {
			return fmt.Errorf("data value != inputData")
		}

		//Ask Xuetao, how default lifetime 0 set can remember previous original lifetime number??????
		//If lifetime use 0 which is default setting, set could reset the original lifetime value
		//compareLifetime which got from Get() could be a little small then the new value, so add 10
		if recInfo.GetTimeToLive() > compareLifetime+9 || recInfo.GetTimeToLive()+9 < compareLifetime {
			return fmt.Errorf("getlifetime != inputlifetime %d:%d", recInfo.GetTimeToLive(), compareLifetime)
		}

		if recInfo.GetVersion() != oldVersion+1 {
			return fmt.Errorf("new version != oldVersion + 1, new version:%d, old version:%d",
				recInfo.GetVersion(), oldVersion)
		}

		glog.Debugf("Version no from get inside of setAndValidate is %d\nAll set validations passed!!",
			recInfo.GetVersion())
	}

	return nil
}

/************************************************************
 * Function to update and get record to do needed validation
 ************************************************************/
func UpdateAndValidate(c client.IClient, key []byte,
	inputData []byte, lifetime uint32, expect_err error) error {

	var oldVersion uint32
	var compareLifetime uint32
	/* If has record and pass in lifetime is 0, it keeps the previous lifetime???
	* If no previous record, it actually could return early before update call,
	* but we want to let update run and test update, so pass this step
	 */
	_, recInfo, err := c.Get([]byte(key))
	/*	if err != nil { //it has no record yet
		oldVersion = 0
		if lifetime == 0 { //assume the liftime 0 means default lifetime setting which is 100
			lifetime = 100
		} */
	if err == nil { //it has record
		oldVersion = recInfo.GetVersion()
		if lifetime < recInfo.GetTimeToLive() {
			compareLifetime = recInfo.GetTimeToLive()
		} else {
			compareLifetime = lifetime
		}
	}

	///TODO:******
	time.Sleep(200 * time.Millisecond)
	recInfo, err = c.Update(key, inputData, client.WithTTL(lifetime))
	if err != expect_err {
		if err == nil {
			return fmt.Errorf("Update returned nil. expected to return %s", expect_err)
		} else if expect_err == nil {
			return fmt.Errorf("Update returned %s. expected to return nil", err)
		} else {
			return fmt.Errorf("Update returned %s. expected to return %s", err, expect_err)
		}
	}

	if err == nil {
		value, recInfo, err := c.Get([]byte(key))
		if err != nil {
			return fmt.Errorf("get after update fail with error: %s", err.Error())
		}

		if bytes.Compare(value, inputData) != 0 {
			return fmt.Errorf("data value != inputData")
		}

		if recInfo.GetTimeToLive() > compareLifetime || recInfo.GetTimeToLive()+9 < compareLifetime {
			return fmt.Errorf("getlifetime != inputlifetime %d:%d", recInfo.GetTimeToLive(), compareLifetime)
		}

		if recInfo.GetVersion() != oldVersion+1 {
			return fmt.Errorf("new version != oldVersion + 1, new version: %d, old version: %d",
				recInfo.GetVersion(), oldVersion)
		}
		glog.Debugf("Version no from get inside of updateAndValidate is %d\nAll update validations passed!!",
			recInfo.GetVersion())
	}

	return nil
}

/**************************************************
 * Function to get record and do needed validation
 **************************************************/
func GetRecord(c client.IClient, key []byte, inputData []byte,
	lifetime uint32, version uint32, expect_err error, creationTime uint32) error {

	value, recInfo, err := c.Get(key)
	if err != expect_err {
		if err == nil {
			return fmt.Errorf("get returned nil. expected to return %s", expect_err)
		} else if expect_err == nil {
			return fmt.Errorf("get returned %s. expected to return nil", err)
		} else {
			return fmt.Errorf("get returned %s. expected to return %s", err, expect_err)
		}
	}
	if err == nil {
		now := time.Now()
		if creationTime == 0 {
			creationTime = uint32(now.Unix())
		}

		if bytes.Compare(value, inputData) != 0 {
			return fmt.Errorf("data value != inputData: %s %s", string(value), string(inputData))
		}

		if recInfo.GetVersion() != version {
			return fmt.Errorf("actual version != expected version: %d:%d", recInfo.GetVersion(), version)
		}

		if !ApproxEqual(util.GetExpirationTimeFrom(now, recInfo.GetTimeToLive()), util.GetExpirationTimeFrom(time.Unix(int64(creationTime), 0), lifetime), 2) {
			return fmt.Errorf("getlifetime != inputlifetime %d:%d", recInfo.GetTimeToLive(), lifetime)
		}
	}
	glog.Debug("All getRecord validations passed!!")
	return nil
}

/**************************************************
 * Function to get record and do needed validation
 **************************************************/
func GetRecordUpdateTTL(c client.IClient, key []byte, inputData []byte,
	newLifetime uint32, version uint32) error {

	var oldVersion uint32
	var compareLifetime uint32

	_, recInfo, err := c.Get([]byte(key))
	if err != nil { //it has no record yet
		return fmt.Errorf("no key available, no data to do TTL update during record %s", err)
	} else { //it has record
		oldVersion = recInfo.GetVersion()
		if newLifetime < recInfo.GetTimeToLive() { //if target lifetime < left time, it won't update
			compareLifetime = recInfo.GetTimeToLive()
		} else {
			compareLifetime = newLifetime
		}
	}

	value, recInfo, err := c.Get(key, client.WithTTL(newLifetime))
	glog.Debug("lifetime is", recInfo.GetTimeToLive(), "version is", recInfo.GetVersion())

	if err == nil {
		if bytes.Compare(value, inputData) != 0 {
			return fmt.Errorf("data value != inputData: %s %s", string(value), string(inputData))
		}

		if recInfo.GetTimeToLive() > compareLifetime || recInfo.GetTimeToLive()+9 < compareLifetime {
			return fmt.Errorf("getlifetime != newLifetime %d:%d", recInfo.GetTimeToLive(), newLifetime)
		}

		if recInfo.GetVersion() != oldVersion {
			return fmt.Errorf("actual version != expected version: %d:%d", recInfo.GetVersion(), oldVersion)
		}
	}
	glog.Debug("All GetRecordUpdateTTL validations passed!!")
	return nil
}

/*******************************************************************
 * Function to get record, this is similar as GetRecord. It added
 * one parameter create time so the lifetime calculation could be
 * more accurate. Ideally all the GetRecord should be like this but
 * we do this particularlly for ETCD as ETCD test runs with multiple
 * records operation and time would be a necessary parameter.
 ********************************************************************/
func ETCDSSGetRecord(ssNodes []server.SSNode, ns string, key []byte, inputData []byte,
	lifetime uint32, version uint32, creationTime uint32) (err error, noNode bool) {

	hasNode := 0
	for i := 0; i < 3; i++ { //assign 3 zonenodes
		if ssNodes[i] == (server.SSNode{}) && i != 2 { //if empty struct
			continue
		} else if ssNodes[i] == (server.SSNode{}) && i == 2 { //node1,2,3 are all empty node
			if hasNode == 0 {
				return nil, true
			} else {
				return nil, false
			}
		}
		hasNode = 1 //at least have one non-empty node in node1,2,3
		glog.Debug("ssNodes", i, " ip:", ssNodes[i].Server.IPAddress(), " port:", ssNodes[i].Server.Port())
		value, recInfo, err := ssNodes[i].Get(ns, key)

		if err == nil {
			now := time.Now()
			if creationTime == 0 {
				creationTime = uint32(now.Unix())
			}

			if lifetime == 0 {
				lifetime = uint32(config.Conf.DefaultTimeToLive)
			}

			if recInfo.GetVersion() != version {
				return fmt.Errorf("node %d %s %s :actual version != expected version: %d:%d", i, ssNodes[i].Server.IPAddress(), ssNodes[i].Server.Port(), recInfo.GetVersion(), version), false
			}

			if bytes.Compare(value, inputData) != 0 {
				return fmt.Errorf("node %d %s %s :data value != inputData: %s %s", i, ssNodes[i].Server.IPAddress(), ssNodes[i].Server.Port(), string(value), string(inputData)), false
			}

			if !ApproxEqual(util.GetExpirationTimeFrom(now, recInfo.GetTimeToLive()), util.GetExpirationTimeFrom(time.Unix(int64(creationTime), 0), lifetime), 2) {
				return fmt.Errorf("node %d :wrong TTL. Real ttl get is: %d record creation time is: %d expire1: %d  expire2: %d",
					i, recInfo.GetTimeToLive(), creationTime, util.GetExpirationTimeFrom(now, recInfo.GetTimeToLive()),
					util.GetExpirationTimeFrom(time.Unix(int64(creationTime), 0), lifetime)), false
			}
		} else {
			return fmt.Errorf("Error: node %d  %s %s  has no data retrieved", i, ssNodes[i].Server.IPAddress(), ssNodes[i].Server.Port()), false
		}
		glog.Debug("All getRecord validations passed!!")
	}
	return nil, false
}

/**************************************************
 * Function to delete record in old sharding node
 **************************************************/
func ETCDSSDeleteRecord(ssNodes []server.SSNode, ns string, key []byte) {

	for i := 0; i < 3; i++ { //assign 3 zonenodes
		if ssNodes[i] != (server.SSNode{}) {
			glog.Debug("delete keys in ssNodes", i, " ip:", ssNodes[i].Server.IPAddress(), " port:", ssNodes[i].Server.Port())
			if err := ssNodes[i].Delete(ns, key); err != nil {
				glog.Debug("delete keys in ssNodes", i, " ip:", ssNodes[i].Server.IPAddress(), " port:", ssNodes[i].Server.Port(), "fail")
			}
		}
	}
}

func DestroyAndValidate(c client.IClient, key []byte, expect_err error) error {
	err := c.Destroy(key)
	if err != expect_err {
		if err == nil {
			return fmt.Errorf("destroy returned nil. expected to return %s", expect_err)
		} else if expect_err == nil {
			return fmt.Errorf("destroy returned %s. expected to return nil", err)
		} else {
			return fmt.Errorf("destroy returned %s. expected to return %s", err, expect_err)
		}
	}
	time.Sleep(200 * time.Millisecond) ///TODO to revisit after implemenation two phase delete
	if err == nil {

		_, _, err = c.Get([]byte(key))
		if err == nil {
			return fmt.Errorf("Record is not deleted as the getRecord still success ")
		}
	}

	return nil
}

func RemoveLog(t *testing.T, hostip string, inProxy bool) error {
	//	if ResolveHostIp() != hostip {
	//		return fmt.Errorf("remove log only update log on local host, please run this on local host")
	//	}
	var cmd string

	if inProxy == true { //this is not really be used now
		cmd = "echo -n > " + "./server/proxy.log; /bin/rm -rf " + "./cal.log"
		glog.Info("remove command is ", cmd)
		_, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			t.Error("remove log failed", err)
		}
	} else { //clean proxy log
		if ResolveHostIp() != hostip {
			cmd = "ssh " + hostip + " \"sudo -u website bash -c 'echo -n > " + dirConfig.Proxydir +
				"/proxy.log';  sudo -u website bash -c 'chmod 666 " + dirConfig.Proxydir + "/config.toml' \" "
		} else {
			cmd = "sudo -u website bash -c 'echo -n > " + dirConfig.Proxydir +
				"/proxy.log '; sudo -u website bash -c 'chmod 666  " + dirConfig.Proxydir + "/config.toml'  "
		}
		glog.Info("remove log command is ", cmd)
		_, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			t.Error("remove remove log failed", err)
		}
	}
	return nil
}

func CheckLog(t *testing.T, hostip string, grepstr string, inProxy bool) error {
	//	if ResolveHostIp() != hostip {
	//		return fmt.Errorf("check log only be executed on local host, please run this on local host")
	//	}
	var cmd string

	if inProxy == true { //this is not really be used now
		//check local log
		appLogfile := "../server/proxy.log" //app log
		grepCmd := "grep -Ei '" + grepstr + "' " + appLogfile + " | grep -v grep | wc -l"
		glog.Info("grep command is ", grepCmd)

		output, err := exec.Command("bash", "-c", grepCmd).Output()
		if (err == nil) && (strings.TrimSpace(string(output)) == "0") {
			t.Error("expected'" + grepstr + "'doesn't exist in local app log file")
		} else if err != nil {
			t.Error("local grep in app log fail", err)
		}
	} else { //check proxy log
		appLogfile := dirConfig.Proxydir + "/proxy.log" //app log
		grepCmd := "sudo -u website grep -Ei \"" + grepstr + "\" " + appLogfile + " | grep -v grep | wc -l"
		if ResolveHostIp() != hostip {
			cmd = "ssh " + hostip + " ' " + grepCmd + " ' "
		} else {
			cmd = grepCmd
		}
		glog.Info("checkLog command is ", cmd)

		output, err := exec.Command("bash", "-c", cmd).Output()
		if (err == nil) && (strings.TrimSpace(string(output)) == "0") {
			t.Error("expected'" + grepstr + "'doesn't exist in app log file")
		} else if err != nil {
			t.Error("grep in app log fail, expected string may not exist in log, please check", err)
		}
	}
	return nil
}

func AddProxyConfig(file string, config string) error {
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		return fmt.Errorf("not able to open proxy config file: %s", err.Error())
	}

	//add idletimeout setting into file
	if _, err = fmt.Fprintf(f, config); err != nil {
		return fmt.Errorf("not able to append string to proxy config file: %s", err.Error())
	}
	return nil
}

func RemoveProxyConfig(file string, config string) error {
	input, err := os.ReadFile(file)
	if err != nil {
		return fmt.Errorf("not able to read proxy config file: %s", err.Error())
	}
	re := regexp.MustCompile(config)
	res := re.ReplaceAllString(string(input), "")
	lines := strings.Split(res, "\n")
	for i, line := range lines {
		if strings.Contains(line, config) {
			lines[i] = ""
		}
	}
	output := strings.Join(lines, "\n")
	if err = os.WriteFile(file, []byte(output), 0644); err != nil {
		return fmt.Errorf("not able to write to file with replaced string: %s", err.Error())
	}
	return nil
}

/**********************************************************************
 * Function to add dedicated config value in proxy config file if the
 * setting is not there due to it's in default list
 **********************************************************************/
func AddRemoveProxyConfigValue(file string, config string, parentConfig string, adflag bool, hostip string) error {
	var lfile string
	var dcmd string
	var acmd string

	if ResolveHostIp() != hostip { //scp remote proxy config into local if remote ip,  or use local if local ip
		cmd := "scp @" + hostip + ":" + file + " /tmp/config.toml"
		glog.Info("scp copy remote proxy config to local: command is ", cmd)
		_, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			return fmt.Errorf("update config scp remote proxy config file failed, please check: %s", err.Error())
		}
		lfile = "/tmp/config.toml"
	} else {
		lfile = file
	}

	_, err := os.ReadFile(lfile)
	if err != nil {
		return fmt.Errorf("not able to read proxy config file: %s err: %s", lfile, err.Error())
	}

	dConfigs := strings.Split(string(config), "=")
	if ResolveHostIp() != hostip { //remove config parameter
		dcmd = "sed -i '/.*" + strings.Trim(dConfigs[0], " ") + ".*/d' " + lfile
	} else {
		dcmd = "sudo -u website sed -i '/.*" + strings.Trim(dConfigs[0], " ") + ".*/d' " + lfile
	}
	glog.Info("delete config command is " + dcmd)
	_, err = exec.Command("bash", "-c", dcmd).Output()
	if err != nil {
		fmt.Errorf("delete config command fail: %s", err.Error())
	}

	if adflag == true { //add config parameter
		if ResolveHostIp() != hostip {
			acmd = "sed -i '/.*" + parentConfig + ".*/a \\" + config + " ' " + lfile
		} else {
			acmd = "sudo -u website sed -i '/.*" + parentConfig + ".*/a \\" + config + " ' " + lfile
		}
		glog.Info("add config command is " + acmd)
		_, err = exec.Command("bash", "-c", acmd).Output()
		if err != nil {
			fmt.Errorf("add config command fail: %s", err.Error())
		}
	}

	if ResolveHostIp() != hostip { //scp updated file to remote proxy config
		cmd := "scp " + lfile + " @" + hostip + ":/tmp; ssh " + hostip + " 'sudo -u website cp " + lfile + " " + file +
			"; /bin/rm -rf  " + lfile + " '; " + "/bin/rm -rf " + lfile
		glog.Info("scp to remote proxy back command is ", cmd)
		_, err = exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			return fmt.Errorf("update scp to remote proxy back command fail:  %s", err.Error())
		}
	}

	return nil
}

/**************************************************************************
 * Function to get dedicated config value in proxy config file, this value
 * will be used for later recovery, if config doesn't exist, return error
 **************************************************************************/
func GetProxyConfigValue(file string, config string, parentConfig string, hostip string) (string, error) {
	var lfile string

	if ResolveHostIp() != hostip { //scp remote proxy config file
		cmd := "scp @" + hostip + ":" + file + " /tmp/config.toml"
		glog.Info("scp command is ", cmd)
		_, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			return "", fmt.Errorf("scp remote proxy config file failed, please check: %s", err.Error())
		}
		lfile = "/tmp/config.toml"
	} else {
		lfile = file
	}

	input, err := os.ReadFile(lfile)
	if err != nil {
		return "", fmt.Errorf("not able to read proxy config file: %s", err.Error())
	}

	glog.Debug("parentConfig is " + parentConfig + " config is " + config)
	lines := strings.Split(string(input), "\n")
	lineno := 0
	for _, line := range lines {
		if parentConfig == "" {
			for _, line := range lines[lineno:len(lines)] { //continue scan from the current line til end of file
				lineno = lineno + 1
				glog.Debug("line number is " + strconv.Itoa(lineno) + ", line is " + line)
				if strings.Contains(line, config) {
					s := strings.Split(line, "=")
					return s[1], nil
				}
			}
		} else {
			glog.Debug("line no is " + strconv.Itoa(lineno) + ", line is " + line)
			lineno = lineno + 1 //get the current scanned line no
			if strings.Contains(line, parentConfig) {
				for _, line := range lines[lineno:len(lines)] { //continue scan from the current line til end of file
					lineno = lineno + 1
					glog.Debug("line number is " + strconv.Itoa(lineno) + ", line is " + line)
					if strings.Contains(line, config) {
						s := strings.Split(line, "=")
						return s[1], nil
					}
					if strings.Contains(line, "[") { //not found under parentconfig,return error
						return "", fmt.Errorf("can not find config, please add it into proxy config first")
					}
				}
				return "", fmt.Errorf("can not find config, please add it into proxy config first")
			}
		}
	}
	return "", fmt.Errorf("can not find target config, please add it into proxy config first")
}

/*****************************************************************
 * Function to update dedicated config value in proxy config file
 *****************************************************************/
func UpdateProxyConfig(file string, old string, new string, parentConfig string, hostip string) error {
	var lfile string

	if ResolveHostIp() != hostip { //scp remote proxy config into local if remote ip,  or use local if local ip
		cmd := "scp @" + hostip + ":" + file + " /tmp/config.toml"
		glog.Info("scp copy remote proxy config to local: command is ", cmd)
		_, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			return fmt.Errorf("update config scp remote proxy config file failed, please check: %s", err.Error())
		}
		lfile = "/tmp/config.toml"
	} else {
		lfile = file
	}

	input, err := os.ReadFile(lfile)
	if err != nil {
		return fmt.Errorf("not able to read proxy config file: %s err: %s", lfile, err.Error())
	}

	update := 0
	lineno := 0
	lines := strings.Split(string(input), "\n")
	for _, line := range lines {
		if parentConfig == "" { //parent config is the base config which the real needed one is under that
			for _, line := range lines[lineno:len(lines)] {
				glog.Debug("line number is " + strconv.Itoa(lineno) + ", line is " + line)
				if strings.Contains(line, old) {
					lines[lineno] = new
					update = 1
					break
				}
				lineno = lineno + 1 //get the current scanned line no
			}
			break
		} else { //the string need to be replaced is under certain string(parentConfig), so need this
			lineno = lineno + 1
			if strings.Contains(line, parentConfig) {
				for _, line := range lines[lineno:len(lines)] {
					glog.Debug("line number is " + strconv.Itoa(lineno) + ", line is " + line)
					if strings.Contains(line, old) {
						lines[lineno] = new
						update = 1
						break
					}
					if strings.Contains(line, "[") {
						return fmt.Errorf("can not find config, please add it to proxy config before replace")
					}
					lineno = lineno + 1 //get the current scanned line no
				}
				break
			}
			glog.Debug("line number is " + strconv.Itoa(lineno) + ", line is " + line)
		}
	}

	if update == 0 {
		return fmt.Errorf("not able to find replaced string in config file or under parent config: %s", err.Error())
	}
	output := strings.Join(lines, "\n") //change the file with new config
	if err = os.WriteFile(lfile, []byte(output), 0644); err != nil {
		return fmt.Errorf("not able to write to file with replaced string, file is: %s err: %s", lfile, err.Error())
	}

	if ResolveHostIp() != hostip { //scp updated file to remote proxy config
		cmd := "scp " + lfile + " @" + hostip + ":/tmp; ssh " + hostip + " 'sudo -u website cp " + lfile + " " + file +
			"; /bin/rm -rf  " + lfile + " '; " + "/bin/rm -rf " + lfile
		glog.Info("scp to remote proxy back command is ", cmd)
		_, err := exec.Command("bash", "-c", cmd).Output()
		if err != nil {
			return fmt.Errorf("update scp to remote proxy back command fail: %s", err.Error())
		}
	}

	return nil
}

func CopyCtlMgr(hostip string) {
	var cmd string

	//if etcd server box is not the same as go test run box, copy ctlmgr.sh to etcd box ss folder
	if ResolveHostIp() != hostip {
		cmd = "scp -p -r " + dirConfig.Githubdir + "/cmd/etcdsvr/test/ctlmgr.sh " +

			dirConfig.Githubdir + "/test/functest/etcd_test/connInfo " +
			hostip + ":/tmp; ssh " + hostip + " 'sudo -u website cp /tmp/ctlmgr.sh /tmp/connInfo " +
			dirConfig.SSdir + "; /bin/rm -rf /tmp/ctlmgr.sh /tmp/connInfo; ' "
	} else {
		cmd = "sudo -u website cp " + dirConfig.Githubdir + "/cmd/etcdsvr/test/ctlmgr.sh " +
			dirConfig.Githubdir + "/test/functest/etcd_test/connInfo " + dirConfig.SSdir + ";"
	}

	glog.Info("CopyCtlMgr \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
	time.Sleep(2 * time.Second)
}

func RunlimitsConfig(t *testing.T, hostip string) {
	var cmd string
	if ResolveHostIp() != hostip {
		cmd = "scp " + dirConfig.Githubdir + "/test/functest/limits_def.toml " + hostip +
			":/tmp; sudo -u website cp /tmp/limits_def.toml " + dirConfig.Proxydir + "; ssh " + hostip + " '" +
			dirConfig.Proxydir + "/junocfg set --config " + dirConfig.Proxydir + "/limits_def.toml '; "
	} else {
		cmd = "sudo -u website cp " + dirConfig.Githubdir + "/test/functest/limits_def.toml " +
			dirConfig.Proxydir + "; " + dirConfig.Proxydir + "/junocfg set --config " + dirConfig.Proxydir +
			"/limits_def.toml > /tmp/haha; "
	}
	glog.Info("RunlimitsConfig \"" + cmd + " \"")
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		t.Error("RunlimitsConfig fail", err)
	}
}

func LoadInitConfig(hostip string) {
	var cmd string

	if ResolveHostIp() != hostip {
		cmd = "ssh " + hostip + " 'sudo -u website " + dirConfig.SSdir + "/ctlmgr.sh init config.toml ' "
	} else {
		glog.Info("localhost run")
		cmd = dirConfig.Githubdir + "/cmd/etcdsvr/test/ctlmgr.sh init config.toml; "
	}
	glog.Info("LoadInitConfig \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
	time.Sleep(3 * time.Second)
}

// temporally create redist info
func UpdateRedistConfig(t *testing.T, hostip string, connNo string, configFile string) {
	var cmd string
	var cmd1 string

	glog.Debug("ResolveHostIp is " + ResolveHostIp() + "hostip is " + hostip)
	if ResolveHostIp() != hostip {
		cmd = "ssh " + hostip + " 'sudo -u website " + dirConfig.SSdir + "/ctlmgr.sh replaceConn " +
			dirConfig.AddRemoveSecondHost + " " + connNo + " ; ' "
		cmd1 = "ssh " + hostip + " ' cd " + dirConfig.SSdir + ";" + " sudo -u website " + dirConfig.SSdir +
			"/ctlmgr.sh dist " + configFile + " >/dev/null 2>&1; ' "
	} else {
		glog.Debug("githubdir is " + dirConfig.Githubdir)
		cmd = "sudo -u website " + dirConfig.Githubdir + "/cmd/etcdsvr/test/ctlmgr.sh replaceConn " +
			dirConfig.AddRemoveSecondHost + " " + connNo + ";"
		cmd1 = "cd " + dirConfig.SSdir + ";" + " bash -c 'sudo -u website " + dirConfig.Githubdir +
			"/cmd/etcdsvr/test/ctlmgr.sh dist " + configFile + ">/dev/null 2>&1 '; "
	}
	glog.Info("UpdateRedistConfig partone \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
	//	if err != nil {
	//		t.Error("UpdateRedistConfig part one fail", err)
	//	}
	glog.Info("UpdateRedistConfig parttwo \"" + cmd1 + " \"")
	exec.Command("bash", "-c", cmd1).Output()
	//	if err != nil {
	//		t.Error("UpdateRedistConfig part two fail", err)
	//	}
	time.Sleep(3 * time.Second)
}

// start redist
func StartRedistConfig(t *testing.T, hostip string, markdown string) {
	var localIp bool = false
	var cmd string
	if ResolveHostIp() == hostip {
		localIp = true
	}

	for zoneid := 0; zoneid < 5; zoneid++ {
		if localIp == false {
			cmd = " ssh " + hostip + " 'sudo -u website " + dirConfig.SSdir + "/ctlmgr.sh z" +
				strconv.Itoa(zoneid) + " new_config.toml " + markdown + " > /dev/null; ' "
		} else {
			cmd = "sudo -u website " + dirConfig.Githubdir + "/cmd/etcdsvr/test/ctlmgr.sh z" + strconv.Itoa(zoneid) + " new_config.toml " + markdown + " > /dev/null 2>&1 "
		}
		glog.Info("StartRedistConfig \"" + cmd + " \"")
		exec.Command("bash", "-c", cmd).Output()
	}
}

// start auto redistribution
func StartAutoRedistConfig(t *testing.T, hostip string, markdown string) {
	var localIp bool = false
	var cmd string
	if ResolveHostIp() == hostip {
		localIp = true
	}

	if localIp == false {
		cmd = " ssh " + hostip + " \"bash -c 'cd " + dirConfig.SSdir + ";" + " sudo -u website " + dirConfig.SSdir +
			"/ctlmgr.sh auto new_config.toml " + markdown + " > /dev/null 2>&1 '; \" "
	} else {
		cmd = "bash -c 'cd " + dirConfig.SSdir + ";" + " sudo -u website " + dirConfig.Githubdir + "/cmd/etcdsvr/test/ctlmgr.sh auto new_config.toml " + markdown + " > /dev/null 2>&1'; "
	}
	glog.Info("StartAutoRedistConfig \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
}

// temporally check forward finish, all zones are snapshot_finish
func FinishForwardCheck(t *testing.T, hostip string) {
	var cmd string
	if ResolveHostIp() != hostip {
		cmd = "ssh " + hostip + " 'sudo -u website " + dirConfig.SSdir + "/ctlmgr.sh checkfin'; "
	} else {
		cmd = dirConfig.Githubdir + "/cmd/etcdsvr/test/ctlmgr.sh checkfin; "
	}
	glog.Info("FinishForwardCheck \"" + cmd + " \"")
	//	exec.Command("bash", "-c", cmd).Output()
	if out, e := exec.Command("bash", "-c", cmd).Output(); e == nil {
		glog.Infoln(string(out))
	} else {
		glog.Infoln(e)
	}
}

// resume the aborted redistribution
func ResumeAbortedReq(t *testing.T, hostip string) {
	var cmd string
	if ResolveHostIp() != hostip {
		cmd = "ssh " + hostip + " \"bash -c 'cd " + dirConfig.SSdir + ";" + " sudo -u website " + dirConfig.SSdir +
			"/ctlmgr.sh resume new_config.toml > /dev/null 2>&1 '; \" "
	} else {
		cmd = "bash -c 'cd " + dirConfig.SSdir + ";" + " sudo -u website " + dirConfig.Githubdir +
			"/cmd/etcdsvr/test/ctlmgr.sh resume new_config.toml > /dev/null 2>&1 '; "
	}
	glog.Info("ResumeAbortedReq \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
}

// commit the new change
func FinalizeConfig(t *testing.T, hostip string) {
	var cmd string
	if ResolveHostIp() != hostip {
		cmd = "ssh " + hostip + " \"bash -c 'cd " + dirConfig.SSdir + ";" + " sudo -u website " + dirConfig.SSdir +
			"/ctlmgr.sh apply new_config.toml > /dev/null 2>&1 '; \" "
	} else {
		cmd = "bash -c 'cd " + dirConfig.SSdir + ";" + " sudo -u website " + dirConfig.Githubdir +
			"/cmd/etcdsvr/test/ctlmgr.sh apply new_config.toml > /dev/null 2>&1 '; "
	}
	glog.Info("FinalizeConfig \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
}

func ZoneMarkup(t *testing.T, hostip string) {
	var cmd string
	if ResolveHostIp() != hostip {
		cmd = "ssh " + hostip + " \"bash -c 'cd " + dirConfig.SSdir + ";" + " sudo -u website " + dirConfig.SSdir +
			"/ctlmgr.sh markup new_config.toml > /dev/null 2>&1 '; \" "
	} else {
		cmd = "bash -c 'cd " + dirConfig.SSdir + ";" + " sudo -u website " + dirConfig.Githubdir +
			"/cmd/etcdsvr/test/ctlmgr.sh markup new_config.toml > /dev/null 2>&1 '; "
	}
	glog.Info("ZoneMarkup \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
}

func AbortRedist(t *testing.T, hostip string) {
	var cmd string
	if ResolveHostIp() != hostip {
		cmd = "ssh " + hostip + " \"bash -c 'cd " + dirConfig.SSdir + ";" + " sudo -u website " + dirConfig.SSdir +
			"/ctlmgr.sh abort new_config.toml > /dev/null 2>&1 '; \" "
	} else {
		cmd = "bash -c 'cd " + dirConfig.SSdir + ";" + " sudo -u website " + dirConfig.Githubdir +
			"/cmd/etcdsvr/test/ctlmgr.sh abort new_config.toml > /dev/null 2>&1 '; "
	}
	glog.Info("AbortRedist \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
}

/***********************************************************
 * Second host means the host snapshot will be forwarded to
 ***********************************************************/
func EtcdStartSecondHost(t *testing.T, start int) {
	var cmd string

	if start == 0 {
		cmd = "ssh " + dirConfig.AddRemoveSecondHost + " '" + dirConfig.SecondHostSSdir +
			"/shutdown.sh; sleep 5; sudo -u website /bin/rm -rf " + dirConfig.SecondHostSSdir + "/rocksdb/* " + dirConfig.WalDir + ";' "
	} else if start == 1 {
		cmd = "ssh " + dirConfig.AddRemoveSecondHost + " '" + dirConfig.SecondHostSSdir +
			"/shutdown.sh; sleep 5; cd " + dirConfig.SecondHostSSdir + "; bash -c \" ./start.sh > /dev/null; sleep 3 \"; ' "
	} else {
		cmd = "ssh " + dirConfig.AddRemoveSecondHost + " '" + dirConfig.SecondHostSSdir +
			"/shutdown.sh; sleep 5; ' "
	}

	glog.Info("etcdStartSecondHost \"" + cmd + " \"")
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		t.Error("EtcdStartSecondHost fail", err)
	}
}

/***********************************************************
 * This is a temporally function to make shutdown easier
 ***********************************************************/
func TempEtcdStartFirstHost(t *testing.T, hostip string, start bool) {
	var cmd string
	if start == true {
		if ResolveHostIp() != hostip {
			cmd = "ssh " + hostip + " '" + dirConfig.SSdir + "/shutdown.sh; " + dirConfig.Proxydir +
				"/shutdown.sh; bash -c \" " + dirConfig.SSdir + "/start.sh > /dev/null 2>&1; " +
				dirConfig.Proxydir + "/start.sh > /dev/null 2>&1; \" '"
		} else {
			cmd = dirConfig.SSdir + "/shutdown.sh; " + dirConfig.Proxydir + "/shutdown.sh; bash -c \" " +
				dirConfig.SSdir + "/start.sh > /dev/null 2>&1; " + dirConfig.Proxydir + "/start.sh > /dev/null 2>&1; \" "
		}
	} else {
		if ResolveHostIp() != hostip {
			cmd = "ssh " + hostip + " '" + dirConfig.SSdir + "/shutdown.sh; " + dirConfig.Proxydir + "/shutdown.sh; " +
				"sleep 3; sudo -u website /bin/rm -rf " + dirConfig.SSdir + "/rocksdb/* " + dirConfig.WalDir + ";' "
		} else {
			cmd = dirConfig.SSdir + "/shutdown.sh; " + dirConfig.Proxydir + "/shutdown.sh; sleep 3; " +
				" sudo -u website /bin/rm -rf " + dirConfig.SSdir + "/rocksdb/* " + dirConfig.WalDir + ";' "
		}
	}

	glog.Info("TempEtcdStartFirstHost \"" + cmd + " \"")
	_, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		t.Error("TempEtcdStartFirstHost fail", err)
	}
	time.Sleep(5 * time.Second)
}

func ReInitializeCluster(config server.ClusterConfig) (c *server.Cluster) {
	clusterInfo := &cluster.ClusterInfo[0]
	rw := etcd.GetClsReadWriter()
	clusterInfo.Read(rw)
	chWatch := etcd.WatchForProxy()
	cluster.Initialize(&cluster.ClusterInfo[0], &config.ProxyConfig.Outbound, chWatch, etcd.GetClsReadWriter())
	glog.Info("new cluster info is ", clusterInfo)
	return server.NewClusterWithConfig(&config)
}

// This definitely will be deleted as it's a temporally workaround for shutdown issue
func SSShutdown(hostip string, secondhost bool) {
	var cmd string
	if ResolveHostIp() != hostip {
		cmd = "ssh " + hostip + " '" + dirConfig.SSdir + "/shutdown.sh; " + dirConfig.Proxydir + "/shutdown.sh; '"
		if secondhost == true {
			cmd = cmd + " ssh " + dirConfig.AddRemoveSecondHost + " '" + dirConfig.SecondHostSSdir + "/shutdown.sh; ' "
		}
	} else {
		cmd = dirConfig.SSdir + "/shutdown.sh; " + dirConfig.Proxydir + "/shutdown.sh; "
		if secondhost == true {
			cmd = cmd + " ssh " + dirConfig.AddRemoveSecondHost + " '" + dirConfig.SecondHostSSdir + "/shutdown.sh; ' "
		}
	}
	glog.Info("SSShutdown \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
}

func SSRestart(hostip string, secondhost bool) {
	var cmd string
	if ResolveHostIp() != hostip {
		cmd = "ssh " + hostip + " '" + dirConfig.SSdir + "/start.sh > /dev/null 2>&1; " + dirConfig.Proxydir + "/start.sh  > /dev/null 2>&1'; "
		if secondhost == true {
			cmd = cmd + " ssh " + dirConfig.AddRemoveSecondHost + " '" + dirConfig.SecondHostSSdir + "/start.sh > /dev/null 2>&1' "
		}
	} else {
		cmd = "bash -c \" " + dirConfig.SSdir + "/start.sh > /dev/null; " + dirConfig.Proxydir + "/start.sh > /dev/null; "
		if secondhost == true {
			cmd = cmd + " ssh " + dirConfig.AddRemoveSecondHost + " '" + dirConfig.SecondHostSSdir + "/start.sh > /dev/null 2>&1' \" "
		} else {
			cmd = cmd + " \" "
		}
	}
	glog.Info("SSRestart \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
}

/************************************************************************
 * Function to get new sharding SS node as well as old sharding SS node.
 * All updated node info will be used to check record update after
 * sharding redistribution done. After ETCD redistribution done, old
 * sharding node won't get record update while new sharding node will.
 ************************************************************************/
func GetUpdatedShardNodes(oldNodes []server.SSNode, newNodes []server.SSNode) ([]server.SSNode, []server.SSNode) {
	oriSNodes := make([]server.SSNode, 5)
	newSNodes := make([]server.SSNode, 5)

	for i := 0; i < 5; i++ {
		newSNodes[i] = newNodes[i]
		if (oldNodes[i].Server.IPAddress() != newNodes[i].Server.IPAddress()) ||
			(oldNodes[i].Server.Port() != newNodes[i].Server.Port()) {
			oriSNodes[i] = oldNodes[i] //sharding changed, save original node info
		}
		if newSNodes[i] != (server.SSNode{}) { //not equal empty
			glog.Debug("newSNodes ", i, " ip:", newSNodes[i].Server.IPAddress(), " port:", newSNodes[i].Server.Port())
		}
		if oriSNodes[i] != (server.SSNode{}) { //not equal empty
			glog.Debug("oriSNodes ", i, " ip:", oriSNodes[i].Server.IPAddress(), " port:", oriSNodes[i].Server.Port())

		}
	}
	return oriSNodes, newSNodes
}

func ShutdownETCD(t *testing.T, hostip string) {
	var cmd string
	if ResolveHostIp() != hostip {
		cmd = "ssh " + hostip + " \"cd " + dirConfig.EtcdServerRestartGitDir + "; test/shutdown.sh; sleep 3; \" "
	} else {
		cmd = " bash -c 'cd " + dirConfig.Githubdir + "/cmd/etcdsvr; test/shutdown.sh > /dev/null'; sleep 10; "
	}
	glog.Info("ShutdownETCD command is: \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
	//	if err != nil {
	//		t.Error("ShutdownETCD fail", err)
	//	}
}

func RestartETCD(t *testing.T, hostip string) {
	var cmd string
	if ResolveHostIp() != hostip {
		cmd = "ssh " + hostip + " \"cd " + dirConfig.EtcdServerRestartGitDir + ";test/start.sh > /dev/null; sleep 10; \" "
	} else {
		cmd = "bash -c 'cd " + dirConfig.Githubdir + "/cmd/etcdsvr; test/start.sh > /dev/null'; sleep 10; "
	}
	glog.Info("RestartETCD \"" + cmd + " \"")
	exec.Command("bash", "-c", cmd).Output()
	//		if err != nil {
	//			t.Error("RestartETCD fail", err)
	//		}
}

func ResolveHostIp() string {
	netInterfaceAddresses, err := net.InterfaceAddrs()
	if err != nil {
		return ""
	}
	for _, netInterfaceAddress := range netInterfaceAddresses {
		networkIp, ok := netInterfaceAddress.(*net.IPNet)
		if ok && !networkIp.IP.IsLoopback() && networkIp.IP.To4() != nil {
			ip := networkIp.IP.String()
			return ip
		}
	}
	return ""
}

func PrintStatus(funcname string, params *mock.MockParams, err error) {
	fmt.Println("Print out "+funcname+" "+
		"1{"+strconv.Itoa(int(params.MockInfoList[0].Status))+"}2{"+strconv.Itoa(int(params.MockInfoList[1].Status))+
		"}3{"+strconv.Itoa(int(params.MockInfoList[2].Status))+"}4{"+strconv.Itoa(int(params.MockInfoList[3].Status))+
		"}5{"+strconv.Itoa(int(params.MockInfoList[4].Status))+"} rcerr=", err)
}

func HaveSameOriginator(ctx1 client.IContext, ctx2 client.IContext) bool {
	var r1, r2 *cli.RecordInfo
	var ok bool

	if r1, ok = ctx1.(*cli.RecordInfo); ok {
		if r2, ok = ctx2.(*cli.RecordInfo); ok {
			return r1.IsSameOriginator(r2)
		}
	}
	return false
}
