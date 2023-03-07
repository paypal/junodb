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
  
package service

import (
	"net"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/io"
	"juno/pkg/logging/cal"
)

type Service struct {
	listeners      []io.IListener
	wg             sync.WaitGroup
	chDone         chan bool
	chSuspend      chan bool // true: suspend, false: resume
	suspended      bool
	config         Config
	requestHandler io.IRequestHandler
	inShutdown     int32 ///TODO to be renamed
	acceptLimiter  ILimiter
	Zoneid         int
}

type SuspendFunc func(b bool)

func New(config Config, reqHandler io.IRequestHandler, listeners ...io.IListener) (service *Service) {
	service = &Service{
		listeners:      listeners,
		chDone:         make(chan bool),
		chSuspend:      make(chan bool, 1),
		config:         config,
		requestHandler: reqHandler,
		Zoneid:         -1,
	}
	return
}

func NewService(cfg Config, reqHandler io.IRequestHandler) (*Service, SuspendFunc) {
	cfg.SetDefaultIfNotDefined()

	cfgListeners := cfg.Listener

	var listeners []io.IListener
	for _, lsnrCfg := range cfgListeners {

		ln, err := io.NewListener(lsnrCfg, cfg.GetIoConfig(&lsnrCfg), reqHandler)
		if err == nil {
			listeners = append(listeners, ln)
		} else {
			glog.Warningf("Cannot Listen on %s, err=%s", lsnrCfg.Addr, err.Error())
		}
	}
	if len(listeners) == 0 {
		glog.Fatal("No listener created")
	}

	svc := New(cfg, reqHandler, listeners...)
	f := func(b bool) {
		svc.chSuspend <- b
	}
	return svc, f
}

func NewWithListenFd(cfg Config, reqHandler io.IRequestHandler, fds ...*os.File) (service *Service) {
	glog.Debugf("NewWithListenFd")
	var listeners []io.IListener
	cfgListeners := cfg.Listener
	if len(cfgListeners) != len(fds) {
		glog.Fatal("number of listener config not match number of FDs")
	}

	for i, fd := range fds {

		ln, err := io.NewListenerWithFd(cfgListeners[i], cfg.GetIoConfig(&cfgListeners[i]), fd, reqHandler)
		if err == nil {
			listeners = append(listeners, ln)
		} else {
			glog.Warning("Cannot Listen on ", fd.Fd())
		}
	}
	if len(listeners) == 0 {
		glog.Fatal("No listener created")
	}
	return New(cfg, reqHandler, listeners...)
}

func NewWithLimiterAndListenFd(cfg Config, reqHandler io.IRequestHandler, limiter ILimiter, fds ...*os.File) (service *Service) {
	service = NewWithListenFd(cfg, reqHandler, fds...)
	service.acceptLimiter = limiter
	return
}

func (s *Service) serve(l io.IListener) {
	s.wg.Add(1)
	go func() {
		defer func() {
			if s.shuttingDown() {
				l.WaitForShutdownToComplete(s.config.ShutdownWaitTime.Duration)
			}
			s.wg.Done()
			glog.Debug("Listener stopped")
		}()

		for {
			err := l.AcceptAndServe()
			if err != nil {
				if nerr, ok := err.(net.Error); ok && nerr.Temporary() {
					glog.Debug("Temporary accept error: ", err)
					continue
				} else {
					if !s.shuttingDown() && !s.suspended {
						// compare error string -- "use of closed network connection" for return ???
						glog.Warningf("%s accept error: %s", l.GetConnString(), err.Error())

						if cal.IsEnabled() {
							cal.Event(cal.TxnTypeAccept, "Error", cal.StatusSuccess, []byte(err.Error()))
						}
					}
					return
				}
			}
			//			if s.acceptLimiter != nil {
			//				if s.acceptLimiter.LimitReached() {
			//					s.acceptLimiter.Throttle()
			//				}
			//			}
		}
	}()
}

func (s *Service) Run() {
	s.initSignalHandler()
	s.requestHandler.Init()
	for _, ln := range s.listeners {
		s.serve(ln)
	}

loop:
	for {
		select {
		case <-s.chDone:
			s.doShutdown()
			break loop
		case yes := <-s.chSuspend:
			if yes {
				glog.Infof("Suspend service")
				s.doShutdown()
				break
			}

			// Shutdown() has been called.
			if s.shuttingDown() {
				break loop
			}
			// Restart listeners
			for _, ln := range s.listeners {
				ln.Refresh()
				s.suspended = false
				glog.Infof("Resume service")
				s.serve(ln)
			}
		}
	}

	s.wg.Wait()
	s.requestHandler.Finish()
}

func (s *Service) shuttingDown() bool {
	return atomic.LoadInt32(&s.inShutdown) != 0
}

func (s *Service) Shutdown() {
	if !s.shuttingDown() {
		atomic.AddInt32(&s.inShutdown, 1)
		close(s.chDone)
		s.chDone = nil
	}
}

func (s *Service) doShutdown() {
	if s.suspended {
		return
	}

	s.suspended = true
	for _, lsnr := range s.listeners {
		lsnr.Shutdown()
	}
}

func (s *Service) initSignalHandler() {
	signal.Ignore(syscall.SIGPIPE, syscall.SIGURG)
	sigs := make(chan os.Signal)

	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func(sigCh chan os.Signal) {
	loop:
		for {
			select {
			case sig := <-sigCh:
				glog.Infof("signal %d (%s) received", sig, sig)
				break loop
			}
		}
		s.Shutdown()
	}(sigs)
}

func (s *Service) GetListeners() []io.IListener {
	return s.listeners
}
