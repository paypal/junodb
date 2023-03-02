package service

import (
	"fmt"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"juno/third_party/forked/golang/glog"
)

type (
	ChildInfo struct {
		Id  int
		Cmd *exec.Cmd
	}

	httpMonitoringT struct {
		listeners []net.Listener
		lsnrFiles []*os.File
	}
	ServerManager struct {
		monitoring *httpMonitoringT

		listeners    []net.Listener
		files        []*os.File
		cmdPath      string
		cmdArgs      []string
		numChildren  int
		pidMap       map[int]ChildInfo
		doneCh       chan struct{}
		deadCh       chan int
		stopping     bool
		cloudEnabled bool
	}
)

func NewServerManager(num int, path string, args []string, cfg Config, httpMonAddr string, cloudEnabled bool) *ServerManager {
	s := &ServerManager{
		cmdPath:      path,
		cmdArgs:      args,
		numChildren:  num,
		pidMap:       make(map[int]ChildInfo),
		doneCh:       make(chan struct{}),
		deadCh:       make(chan int, num),
		stopping:     false,
		cloudEnabled: cloudEnabled,
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
				glog.Debugf("monitoring listener (%s) created for child %d", ln.Addr(), i)

			} else {
				glog.Errorf("fail to create listeners for monitoring")
			}
		}
	}

	cfgListeners := cfg.Listener
	numListeners := len(cfgListeners)

	s.listeners = make([]net.Listener, numListeners)

	s.files = make([]*os.File, numListeners)

	for i, lsnrCfg := range cfgListeners {

		ln, err := net.Listen("tcp", lsnrCfg.Addr)
		if err != nil {
			glog.Exitf("Cannot Listen on %s", lsnrCfg.Addr)
		}
		s.listeners[i] = ln
		file, _ := ln.(*net.TCPListener).File()
		s.files[i] = file
	}

	return s
}

func (s *ServerManager) Run() {
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

		param := fmt.Sprintf("-worker-monitoring-addresses=%s", strings.Join(addrs, ","))
		var args []string = []string{
			"monitor",
			"-child",
			param,
		}
		args = append(args, s.cmdArgs...)

		cmd := exec.Command(s.cmdPath, args...)

		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		cmd.ExtraFiles = s.files

		err := cmd.Start()
		if err != nil {
			glog.Fatalf("Failed to launch child process, error: %v", err)
		}

		// save the cmd for later
		s.pidMap[cmd.Process.Pid] = ChildInfo{-1, cmd}
		glog.Verbosef("%s %s", s.cmdPath, strings.Join(args, " "))
	}
}

func (s *ServerManager) spawnOneChild(i int) {
	param := fmt.Sprintf("-worker-id=%d", i)
	var args []string = []string{
		"worker",
		param,
		"-child",
	}
	args = append(args, s.cmdArgs...)

	cmd := exec.Command(s.cmdPath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if s.monitoring != nil {
		cmd.ExtraFiles = append(s.files, s.monitoring.lsnrFiles[i])
	} else {
		cmd.ExtraFiles = s.files
	}

	err := cmd.Start()
	if err != nil {
		glog.Fatalf("Failed to launch child process, error: %v", err)
	}

	// save the cmd for later
	s.pidMap[cmd.Process.Pid] = ChildInfo{i, cmd}
	glog.Debugf("%s %s", s.cmdPath, strings.Join(args, " "))
}

func (s *ServerManager) handleSignals() {
	sigCh := make(chan os.Signal, 10)
	signal.Notify(sigCh)
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
		if child.Id == -1 {
			s.spawnMonitoringChild()
		} else {
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
