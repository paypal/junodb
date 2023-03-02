package server

import (
	"fmt"
	"sync"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/service"
	"juno/test/testutil/log/frwk"
)

var _ IServer = (*InProcessServer)(nil)

type InProcessServer struct {
	ServerBase
	service *service.Service
	wg      sync.WaitGroup
	up      bool
}

func (s *InProcessServer) String() string {
	return fmt.Sprintf("%s(%d)@%s:%s", s.name, s.id, s.ipAddress, s.port)
}

func (s *InProcessServer) StartNoWait() {
	if s.service != nil {
		s.wg.Add(1)
		go func() {

			if frwk.LOG_DEBUG {
				glog.DebugInfof("Starting InProcess Server")
			}
			s.service.Run()
			if frwk.LOG_DEBUG {
				glog.DebugInfof("InProcess Server Stopped")
			}
			s.wg.Done()
			s.up = false
		}()
	}
}

func (s *InProcessServer) Start() (err error) {
	s.StartNoWait()
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

func (s *InProcessServer) StopNoWait() {
	if s.service != nil {
		s.service.Shutdown()
		s.wg.Wait()
	}
}

func (s *InProcessServer) Stop() (err error) {
	s.StopNoWait()
	down := WaitForDown(s.stopWaitTime, s)
	if down {
		s.up = false
		if frwk.LOG_DEBUG {
			glog.DebugInfof("Server %s:%s down", s.IPAddress(), s.Port())
		}
	} else {
		err = fmt.Errorf("Server %s:%s failed to shutdown in %s", s.IPAddress(), s.Port(), s.stopWaitTime.String())
		glog.Error(err)
	}
	return
}

func (s *InProcessServer) Restart() error {
	if err := s.Stop(); err != nil {
		return err
	}
	return s.Start()
}

func (s *InProcessServer) IsDown() bool {
	return !s.up
}

var inprocessServerID uint = 0

func NewInProcessServer(name string, ipAddr string, port string, s *service.Service, sslEnabled bool, conf *ServerDef) (srv *InProcessServer) {
	if frwk.LOG_DEBUG {
		glog.DebugInfof("Creating InProcess Server Stub %s(%d)@%s:%s", name, inprocessServerID, ipAddr, port)
	}
	inprocessServerID++
	srv = &InProcessServer{
		service: s,
	}
	srv.init(name, inprocessServerID, ipAddr, port, sslEnabled, "", conf)
	//	go func() {
	//		addr := ":" + srv.httpPort
	//		http.ListenAndServe(addr, nil)
	//	}()
	return
}
