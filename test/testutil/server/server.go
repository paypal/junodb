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

package server

import (
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"juno/third_party/forked/golang/glog"

	"juno/internal/cli"
	"juno/pkg/io"
	"juno/pkg/net/netutil"
	"juno/pkg/proto"
	"juno/test/testutil/log/frwk"
)

const (
	kMaxStartStopRetryCount = 10
	kRetryBackOff           = 500 * time.Millisecond
)

type IServer interface {
	Name() string
	String() string
	Address() string
	IPAddress() string
	Port() string
	IsSSLEnabled() bool
	GetHttpMonAddr() string
	Id() uint
	Start() error
	Stop() error
	Restart() error
	IsUp() bool
	IsDown() bool
}

type ServerBase struct {
	name          string
	ipAddress     string
	port          string
	sslEnabled    bool
	httpMonAddr   string
	id            uint
	startWaitTime time.Duration
	stopWaitTime  time.Duration
}

type Server struct {
	ServerBase
	startCmd     string
	stopCmd      string
	startCmdArgs []string
	stopCmdArgs  []string
	cmd          *exec.Cmd
	pid          int
}

func (s *ServerBase) init(name string, id uint, ipAddr string, port string, sslEnabled bool, httpMonAddr string, srv *ServerDef) {
	s.name = name
	s.id = id
	s.ipAddress = ipAddr
	s.port = port
	s.sslEnabled = sslEnabled
	if httpMonAddr == "" {
		httpport, err := strconv.Atoi(port)
		if err != nil {
			glog.Fatal(err)
		}
		s.httpMonAddr = net.JoinHostPort(ipAddr, strconv.Itoa(httpport+1))
	} else {
		if !strings.Contains(httpMonAddr, ":") {
			httpMonAddr = ":" + httpMonAddr
		}
		if httpHost, httpPort, err := net.SplitHostPort(httpMonAddr); err == nil {
			if httpHost == "" {
				httpHost = ipAddr
			}
			s.httpMonAddr = net.JoinHostPort(httpHost, httpPort)
		}

	}
	s.startWaitTime = srv.StartWaitTime.Duration
	s.stopWaitTime = srv.StopWaitTime.Duration

	if s.startWaitTime == 0 {
		s.startWaitTime = 1000 * time.Millisecond
	}
	if s.stopWaitTime == 0 {
		s.stopWaitTime = 1000 * time.Millisecond
	}
}

func (s *ServerBase) Name() string {
	return s.name
}

func (s *ServerBase) Address() string {
	return net.JoinHostPort(s.ipAddress, s.port)
}

func (s *ServerBase) IPAddress() string {
	return s.ipAddress
}

func (s *ServerBase) Port() string {
	return s.port
}

func (s *ServerBase) IsSSLEnabled() bool {
	return s.sslEnabled
}

func (s *ServerBase) GetHttpMonAddr() string {
	return s.httpMonAddr
}

func (s *ServerBase) IsUp() bool {
	if frwk.LOG_DEBUG {
		glog.DebugInfof("Testing if %s is up", s.Address())
	}
	clientProcessor := cli.NewProcessor(io.ServiceEndpoint{Addr: s.Address(), SSLEnabled: s.IsSSLEnabled()}, "testFramework", s.startWaitTime, s.startWaitTime, 0)
	clientProcessor.Start()
	defer clientProcessor.Close()
	request := &proto.OperationalMessage{}
	request.SetOpCode(proto.OpCodeGet)
	request.SetKey([]byte("testKey"))
	request.SetAsRequest()
	request.SetNewRequestID()
	request.SetNamespace([]byte("test_framework"))
	_, err := clientProcessor.ProcessRequest(request)
	if err == nil {
		if frwk.LOG_DEBUG {
			glog.DebugInfof("server %s is up", s.Address())
		}
		return true

	} else {
		if frwk.LOG_DEBUG {
			glog.DebugInfof("Failed to get response from %s %s", s.Address(), err)
		}
	}
	return false
}

func (s *ServerBase) Id() uint {
	return s.id
}

func (s *Server) String() string {
	return fmt.Sprintf("%s(%d)@%s:%s", s.name, s.id, s.ipAddress, s.port)
	//	return fmt.Sprintf("%s(%d)@%s:%s startCmd=%s %s stopCmd=%s %s",
	//		s.name, s.id, s.ipAddress, s.port,
	//		s.startCmd, strings.Join(s.startCmdArgs, " "), s.stopCmd, strings.Join(s.stopCmdArgs, " "))
}

func (s *Server) Restart() error {
	if err := s.Stop(); err != nil {
		return err
	}
	return s.Start()
}

func (s *Server) IsUp() bool {
	query := fmt.Sprintf("http://%s/stats?info=get_pid", s.GetHttpMonAddr())
	if resp, err := http.Get(query); err == nil {
		if body, err := ioutil.ReadAll(resp.Body); err == nil {
			pids := strings.Split(string(body), ",")
			if len(pids) == 0 {
				return false
			}

			s.pid, err = strconv.Atoi(string(pids[0]))
			//glog.Errorf("pid = %d returned", s.pid)
			if s.pid == 0 {
				return false
			}
			return s.ServerBase.IsUp()
		}
	} else {
		if frwk.LOG_DEBUG {
			glog.DebugInfoln(err)
		}
	}
	return false
}

func (s *Server) startServer() (err error) {
	for i := 0; i < kMaxStartStopRetryCount; i++ {
		s.cmd = exec.Command(s.startCmd, s.startCmdArgs...)
		s.cmd.Stdout = os.Stdout
		s.cmd.Stderr = os.Stderr

		err = s.cmd.Run()
		if err == nil {
			return
		}
		if err != nil {
			if frwk.LOG_DEBUG {
				glog.DebugInfof("%#v", err)
			}
			if i == 9 {
				glog.Fatal(err)
			}
			time.Sleep(kRetryBackOff)
			continue
		}
		break
	}
	return
}

func (s *Server) Start() (err error) {
	if s.IsUp() {
		if frwk.LOG_DEBUG {
			glog.DebugInfof("Server %s UP", s.Name())
		}
		return
	}
	if err = s.startServer(); err != nil {
		return
	}
	up := WaitForUp(s.startWaitTime, s)
	if up {
		if frwk.LOG_DEBUG {
			glog.DebugInfof("Server %s:%s started", s.IPAddress(), s.Port())
		}
	} else {
		err = fmt.Errorf("Server %s:%s failed to start in %s", s.IPAddress(), s.Port(), s.startWaitTime.String())
		glog.Error(err)
	}
	return
}

func (s *Server) stopServer() (err error) {
	if len(s.stopCmd) == 0 {
		if s.cmd != nil {
			if s.cmd.Process != nil {
				s.cmd.Process.Kill()
				s.cmd.Wait()
			}
			s.cmd = nil
		}
	} else {
		if frwk.LOG_VERBOSE {
			glog.VerboseInfof("Stopping %s by calling %v %v", s, s.stopCmd, s.stopCmdArgs)
		}
		for i := 0; i < kMaxStartStopRetryCount; i++ {
			s.cmd = exec.Command(s.stopCmd, s.stopCmdArgs...)
			s.cmd.Stdout = os.Stdout
			s.cmd.Stderr = os.Stderr
			err = s.cmd.Run()
			if err != nil {
				if exErr, ok := err.(*exec.ExitError); ok {
					if frwk.LOG_DEBUG {
						glog.DebugInfof("%#v\n", exErr)
					}
				}
				time.Sleep(kRetryBackOff)
				continue
			}
			s.cmd = nil
			break
		}
	}
	return
}

func (s *Server) Stop() (err error) {
	//	if s.IsDown() {
	//		return nil
	//	}
	if err = s.stopServer(); err == nil {
		down := WaitForDown(s.stopWaitTime, s)
		if down {
			if frwk.LOG_DEBUG {
				glog.DebugInfof("Server %s:%s down", s.IPAddress(), s.Port())
			}
		} else {
			err = fmt.Errorf("Server %s:%s failed to shutdown in %s", s.IPAddress(), s.Port(), s.stopWaitTime.String())
			glog.Error(err)
		}
	}

	return
}

func (s *Server) AddStartCmdArg(arg string) {
	s.startCmdArgs = append(s.startCmdArgs, arg)
}

func (s *Server) IsDown() bool {
	if s.pid != 0 {
		cmd := exec.Command("ps", "-p", strconv.Itoa(s.pid))
		err := cmd.Run()
		if err == nil {
			//glog.Errorf("%s %d %d still up", s.Name(), s.Id(), s.pid)
			return false
		}
		//glog.Errorf("%s %d %d down", s.Name(), s.Id(), s.pid)
		return true
	} else {
		//glog.Errorf("0 pid %s %d", s.Name(), s.Id())
		return true
	}
	//glog.Errorf("%s %d %d still up", s.Name(), s.Id(), s.pid)
	return false
}

func waitForServer(up bool, waittime time.Duration, servers ...IServer) (allTrue bool) {
	numSrvs := len(servers)
	chSrvState := make(chan bool, numSrvs)

	testStartFunc := func(chState chan bool, s IServer, timeout time.Duration) {
		timer := time.NewTimer(waittime)
		srvAddr := net.JoinHostPort(s.IPAddress(), s.Port())
		ticker := time.NewTicker(500 * time.Millisecond)
		defer func() {
			timer.Stop()
			ticker.Stop()
		}()
		for {
			if up {
				if s.IsUp() {
					chState <- true
					return
				}
			} else {
				if s.IsDown() {
					chState <- true
					return
				}
			}
			select {
			case <-timer.C:
				chState <- false
				glog.Infof("Failed to connect to %s in time", srvAddr)

				return
			case <-ticker.C:
				continue
			}
		}
	}

	for i := range servers {
		go testStartFunc(chSrvState, servers[i], waittime)
	}

	allTrue = true
	for i := 0; i < numSrvs; i++ {
		st := <-chSrvState
		if st == false {
			allTrue = false //could set multiple times.
		}
	}
	return
}

func WaitForUp(waittime time.Duration, servers ...IServer) (allUp bool) {
	return waitForServer(true, waittime, servers...)
}

func WaitForDown(waittime time.Duration, servers ...IServer) (allDown bool) {
	return waitForServer(false, waittime, servers...)
}

func getBasenameAndArgs(cmd string, id uint, port string) (bn string, args []string) {
	if len(cmd) == 0 {
		return
	}
	fields := strings.Fields(cmd)
	numFields := len(fields)
	if numFields == 0 {
		glog.Fatalf("Invalid command: %s", cmd)
	}
	bn = fields[0]
	for i := 1; i < numFields; i++ {

		field := fields[i]
		if strings.EqualFold(field, "$ID") {
			field = strconv.Itoa(int(id))
		} else if strings.EqualFold(field, "$PORT") {
			field = port
		}
		args = append(args, field)
	}

	return
}

func (s *Server) init(name string, id uint, ipAddr string, port string, sslEnabled bool, httpMonAddr string, srv *ServerDef) {
	if frwk.LOG_DEBUG {
		glog.DebugInfof("Creating Server Stub %s(%d)@%s:%s", name, id-1, ipAddr, port)
	}

	s.ServerBase.init(name, id, ipAddr, port, sslEnabled, httpMonAddr, srv)

	startCmd, startCmdArgs := getBasenameAndArgs(srv.StartCmd, id, port)
	stopCmd, stopCmdArgs := getBasenameAndArgs(srv.StopCmd, id, port)

	if len(startCmd) == 0 {
		glog.Fatalf("Invalid command: %s", startCmd)
	}
	if netutil.IsLocalAddress(ipAddr) {
		s.startCmd = srv.BinDir + "/" + startCmd
		s.startCmdArgs = startCmdArgs

		if len(stopCmd) != 0 {
			s.stopCmd = srv.BinDir + "/" + stopCmd
			s.stopCmdArgs = stopCmdArgs
		}

	} else {

		s.startCmd = "ssh"
		//s.startCmdArgs = append(s.startCmdArgs, "-v")
		s.startCmdArgs = append(s.startCmdArgs, s.ipAddress)
		s.startCmdArgs = append(s.startCmdArgs, srv.BinDir+"/"+startCmd)
		s.startCmdArgs = append(s.startCmdArgs, startCmdArgs...)
		if len(stopCmd) == 0 {
			glog.Fatal("Need to define stop command for a remote server")
		}
		s.stopCmd = "ssh"
		s.stopCmdArgs = append(s.stopCmdArgs, s.ipAddress)
		s.stopCmdArgs = append(s.stopCmdArgs, srv.BinDir+"/"+stopCmd)
		s.stopCmdArgs = append(s.stopCmdArgs, stopCmdArgs...)
	}
}

func NewServer(name string, id uint, ipAddr string, port string, sslEnabled bool, httpMonAddr string, srv *ServerDef) *Server {
	s := &Server{}
	s.init(name, id, ipAddr, port, sslEnabled, httpMonAddr, srv)
	return s
}
