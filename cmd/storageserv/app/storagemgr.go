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

package app

import (
	"fmt"
	"io/ioutil"
	"math"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/cmd/storageserv/stats/shmstats"
	"juno/pkg/util"
)

type (
	ChildInfo struct {
		Id  int
		Cmd *exec.Cmd
	}

	ConnectInfo struct {
		Listener     string
		ZoneId       int
		MachineIndex int
	}

	/// ***
	/// TODO to inherit from service.ServerManager
	httpMonitoringT struct {
		listeners []net.Listener
		lsnrFiles []*os.File
	}
	ServerManager struct {
		monitoring *httpMonitoringT

		connectInfo        []ConnectInfo
		pidFileName        string
		cmdPath            string
		cmdArgs            []string
		numChildren        int
		pidMap             map[int]ChildInfo
		doneCh             chan struct{}
		deadCh             chan int
		stopping           bool
		procCreateItvlBase int
		procCreateItvlMax  int
		lruCacheSizeInMB   int
		dbScanPort         int
		cloudEnabled       bool
	}
)

func NewServerManager(num int, pidFileName string, path string, args []string,
	connInfo []ConnectInfo, httpMonAddr string, dbScanPort int, cloudEnabled bool) *ServerManager {
	s := &ServerManager{
		connectInfo:        connInfo,
		pidFileName:        pidFileName,
		cmdPath:            path,
		cmdArgs:            args,
		numChildren:        num,
		pidMap:             make(map[int]ChildInfo),
		doneCh:             make(chan struct{}),
		deadCh:             make(chan int, num),
		stopping:           false,
		procCreateItvlBase: 100,
		procCreateItvlMax:  20000,
		// For rocksdb.  min(10% * mem / num_of_db_instances, 3072)
		lruCacheSizeInMB: int(math.Min(float64(util.GetTotalMemMB()/(10*num)), 3072)),
		dbScanPort:       dbScanPort,
		cloudEnabled:     cloudEnabled,
	}
	if len(httpMonAddr) != 0 { ///TODO validate addr?
		s.monitoring = &httpMonitoringT{
			listeners: make([]net.Listener, s.numChildren),
			lsnrFiles: make([]*os.File, s.numChildren),
		}
		for i := 0; i < s.numChildren; i++ {
			if ln, err := net.Listen("tcp", "127.0.0.1:0"); err == nil {
				s.monitoring.listeners[i] = ln
				s.monitoring.lsnrFiles[i], _ = ln.(*net.TCPListener).File()
				glog.Debugf("monitoring listener (%s) created for worker %d", ln.Addr(), i)

			} else {
				glog.Errorf("fail to create listeners for monitoring")
			}
		}
	}

	return s
}

func (s *ServerManager) Run() {
	pidFile := s.pidFileName

	if data, err := ioutil.ReadFile(pidFile); err == nil {
		if pid, err := strconv.Atoi(strings.TrimSpace(string(data))); err == nil {
			if process, err := os.FindProcess(pid); err == nil {
				if err := process.Signal(syscall.Signal(0)); err == nil {
					fmt.Fprintf(os.Stderr, "process pid: %d in %s is still running\n", pid, pidFile)
					///TODO check if it is storageserv process
					os.Exit(-1)
				}
			}
		}
	}

	if s.dbScanPort > 0 {
		cmdPath := fmt.Sprintf("%s/%s", filepath.Dir(s.cmdPath), "dbscanserv")
		_, err := os.Stat(cmdPath)
		if os.IsNotExist(err) {
			glog.Exitf("missing executable file: dbscanserv.")
		}
	}

	ioutil.WriteFile(pidFile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0644)
	defer os.Remove(pidFile)
	defer shmstats.Finalize()

	if err := shmstats.InitForManager(s.numChildren); err != nil {
		glog.Error(err.Error())
		return
	}

	s.handleSignals()

	hostName, err := os.Hostname()
	spawn := true
	if err == nil && s.cloudEnabled {
		shutdownList := fmt.Sprintf(" %s ", os.Getenv("SHUTDOWN_LIST"))
		name := fmt.Sprintf(" %s ", hostName)
		glog.Infof("host=%s shutdownList=%s", hostName, shutdownList)
		if strings.Contains(shutdownList, " all ") || strings.Contains(shutdownList, name) {
			glog.Infof("Skip starting workers on:%s", shutdownList)
			spawn = false
		}
	}
	if spawn {
		s.spawnChildren()
	}

Loop:
	for {
		select {
		case pid := <-s.deadCh:
			s.handleDeadChild(pid)
		case <-s.doneCh:
			s.shutdown()
			break Loop
		}
	}
}

func (s *ServerManager) spawnChildren() {
	for i := 0; i < s.numChildren; i++ {
		s.spawnOneChild(i)
	}
	s.spawnMonitoringChild()
	if s.dbScanPort > 0 {
		s.spawnDbScanChild()
	}
}

func (s *ServerManager) spawnMonitoringChild() {
	if s.monitoring != nil {
		var addrs []string
		for _, ln := range s.monitoring.listeners {
			addrs = append(addrs, ln.Addr().String())
		}
		if len(addrs) == 0 {
			return
		}
		shmstats.SetMonPorts(addrs)

		var args []string = []string{
			"monitor",
			"-child",
			fmt.Sprintf("-worker-monitoring-addresses=%s", strings.Join(addrs, ",")),
		}
		args = append(args, s.cmdArgs...)

		glog.Verbosef("%s %s", s.cmdPath, strings.Join(args, " "))
		cmd := exec.Command(s.cmdPath, args...)

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr

		err := cmd.Start()
		if err != nil {
			glog.Fatalf("Failed to launch child process, error: %v", err)
		}

		// save the cmd for later
		s.pidMap[cmd.Process.Pid] = ChildInfo{-1, cmd}
	}
}

func (s *ServerManager) spawnDbScanChild() {

	cmdPath := fmt.Sprintf("%s/%s", filepath.Dir(s.cmdPath), "dbscanserv")

	var argConfig string
	for _, val := range s.cmdArgs {
		if strings.Index(val, "-config") == 0 || strings.Index(val, "-c") == 0 {
			argConfig = val
			break
		}
	}
	glog.Verbosef("%s %s", s.cmdPath, argConfig)
	cmd := exec.Command(cmdPath, argConfig)

	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	err := cmd.Start()
	if err != nil {
		glog.Fatalf("Failed to launch child process, error: %v", err)
	}

	// save the cmd for later
	s.pidMap[cmd.Process.Pid] = ChildInfo{-2, cmd}

}

func (s *ServerManager) spawnOneChild(i int) {
	var args []string = []string{
		"worker",
		"-child",
		fmt.Sprintf("-worker-id=%d", i),
		fmt.Sprintf("-zone-id=%d", s.connectInfo[i].ZoneId),
		fmt.Sprintf("-machine-index=%d", s.connectInfo[i].MachineIndex),
		fmt.Sprintf("-listen=%s", s.connectInfo[i].Listener),
	}
	if s.lruCacheSizeInMB > 0 {
		args = append(args, fmt.Sprintf("-lru-cache-mb=%d", s.lruCacheSizeInMB))
	}
	args = append(args, s.cmdArgs...)
	glog.Debugf("%s %s", s.cmdPath, strings.Join(args, " "))
	cmd := exec.Command(s.cmdPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if s.monitoring != nil {
		cmd.ExtraFiles = append(cmd.ExtraFiles, s.monitoring.lsnrFiles[i])
	}
	//cmd.ExtraFiles = s.files
	interval := s.procCreateItvlBase

	for {
		err := cmd.Start()
		if err != nil {
			glog.Infof("Failed to launch child process, error: %v . Back-off and Retry Launching", err)
			/*
				Backoff and retry launching the process if failed
				It is observed that when the system is low on thread/memory resources,the storage manager process used
				to exit after failing to create the child process by calling glog.Fatalf. The change is done to make the
				manager retry creating the process using an exponential back-off.
			*/
			time.Sleep(time.Duration(interval) * time.Millisecond)
			interval = func() int {
				if s.procCreateItvlMax > (2 * interval) {
					return (2 * interval)
				}
				return s.procCreateItvlMax
			}()
		} else {
			break
		}
	}

	// save the cmd for later
	s.pidMap[cmd.Process.Pid] = ChildInfo{i, cmd}
}

func After(value string, str string) string {
	// Get substring after a string.
	pos := strings.LastIndex(value, str)
	if pos == -1 {
		return ""
	}
	adjustedPos := pos + len(str)
	if adjustedPos >= len(value) {
		return ""
	}
	return value[adjustedPos:len(value)]
}

func (s *ServerManager) handleSignals() {
	sigCh := make(chan os.Signal, 10)
	signal.Notify(sigCh) //, syscall.SIGCHLD, syscall.SIGTERM, syscall.SIGINT)
	signal.Ignore(syscall.SIGPIPE, syscall.SIGURG)

	go func(sigCh chan os.Signal) {
	Loop:
		for {
			sig := <-sigCh
			if sig == syscall.SIGURG {
				continue
			}
			glog.Infof("signal %d (%s) received", sig, sig)
			switch sig {
			case syscall.SIGTERM, syscall.SIGINT:
				break Loop

			case syscall.SIGCHLD:
				s.handleDeadChildren()
			default:
				glog.Warningf("signal %d (%s) not handled", sig, sig)
			}
		}

		close(s.doneCh)
	}(sigCh)
}

func (s *ServerManager) handleDeadChildren() {
	var (
		status syscall.WaitStatus
		usage  syscall.Rusage
	)

	// Multiple child processes dying quickly could generate a single SIGCHLD
	// so would have to syscall.Wait4 in a loop till you get -1
	for {
		pid, err := syscall.Wait4(-1, &status, syscall.WNOHANG, &usage)
		if err != nil || pid <= 0 {
			break
		}

		s.deadCh <- pid
	}
}

func (s *ServerManager) handleDeadChild(pid int) {
	glog.Infof("child %d process exit", pid)

	child, ok := s.pidMap[pid]
	if ok {
		delete(s.pidMap, pid)
	} else {
		return
	}

	// cleanup
	child.Cmd.Wait()

	if !s.stopping {
		switch child.Id {
		case -1:
			s.spawnMonitoringChild()
		case -2:
			s.spawnDbScanChild()
		default:
			s.spawnOneChild(child.Id)
		}
	}
}

func (s *ServerManager) shutdown() {

	s.stopping = true
	glog.Info("shutting down child processes")

	for _, child := range s.pidMap {
		//cmd.Process.Kill()
		child.Cmd.Process.Signal(syscall.SIGTERM)
	}

	// TODO, do we need to have timeout and do kill if needed?
	for _, child := range s.pidMap {
		child.Cmd.Wait()
	}
	s.pidMap = nil
}
