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
//  Package utility provides the utility interfaces for mux package
//  
package redistserv

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
	"juno/third_party/forked/golang/glog"

	"juno/pkg/etcd"
)

const (
	DialTimeout          = 30 * time.Second
	DialKeepAliveTimeout = 30 * time.Second
	DialKeepAliveTime    = 30 * time.Second
	SessionTTL           = 600
)

type Config struct {
	Port              uint16
	ShutdownDelaySecs uint
}

func NewConfig(port uint16, shutdownDelaySecs uint) (cfg *Config) {
	config := &Config{
		Port:              port,
		ShutdownDelaySecs: shutdownDelaySecs,
	}
	return config
}

var redistStateLock sync.Mutex
var redistState map[string]string

var shutdownSync sync.Once

func signalShutdown(shutdown chan struct{}) {
	shutdownSync.Do(func() {
		close(shutdown)
	})
}

func Run(cfg *Config, etcdCfg *etcd.Config, clusterName string, pidFile string) error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	webDir := filepath.Join(filepath.Dir(exe), "web/")

	redistState = make(map[string]string)

	shutdown := make(chan struct{})
	var wg sync.WaitGroup

	go watcher(webDir, cfg, etcdCfg, clusterName, shutdown, shutdownSync, &wg)
	wg.Add(1)
	go server(webDir, cfg, pidFile, shutdown, shutdownSync, &wg)
	wg.Add(1)

	wg.Wait()

	os.Remove(pidFile)

	return nil
}

func server(webDir string, cfg *Config, pidFile string, shutdown chan struct{}, shutdownSync sync.Once, wg *sync.WaitGroup) {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Port))
	if err != nil {
		glog.Fatal(err)
		return
	}

	// Write PID, port and url to pidFile
	port := listener.Addr().(*net.TCPAddr).Port
	var ip string
	if addrs, err := net.InterfaceAddrs(); err == nil {
		for _, addr := range addrs {
			if !strings.HasPrefix(addr.String(), "127") && !strings.Contains(addr.String(), ":") {
				ip = strings.Split(addr.String(), "/")[0]
				break
			}
		}
	}

	url := fmt.Sprintf("Failed to determine IP address. Redist monitoring server launched on port %d", port)
	if ip != "" {
		url = fmt.Sprintf("http://%s:%d\n", ip, port)
	}

	file, err := os.Create(pidFile)
	if err != nil {
		glog.Error(err)
	} else {
		file.WriteString(fmt.Sprintf("%d\n%d\n%s\n", os.Getpid(), port, url))
		glog.Infof("Redist server running on port %d", port)
		glog.Info(url)
	}

	server := &http.Server{}

	// Launch server
	fs := http.FileServer(http.Dir(webDir))
	http.Handle("/", fs)
	http.HandleFunc("/etcd_state.json", handleHTTPRequest)
	http.HandleFunc("/redist_state.json", handleRedistStateLogRequest)
	http.HandleFunc("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Server shutting down"))
		go func() { // If not in goroutine, response isn't sent
			server.Shutdown(context.Background())
		}()
	})

	go func() {
		if err := server.Serve(listener); err != nil {
			if err != http.ErrServerClosed {
				glog.Error(err)
			}
			signalShutdown(shutdown)
		}
	}()

	select {
	case <-shutdown:
		server.Close()
		wg.Done()
	}
}

func handleHTTPRequest(w http.ResponseWriter, r *http.Request) {
	redistStateLock.Lock()
	defer redistStateLock.Unlock()

	data, err := json.Marshal(redistState)
	if err != nil {
		glog.Error(err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
	} else {
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
	}
}

type redistStateLogEntry struct {
	Zone  int
	Node  int
	Shard int
	State string
}

func handleRedistStateLogRequest(w http.ResponseWriter, r *http.Request) {
	f, err := os.Open("redist_state.json")
	if err != nil {
		if os.IsNotExist(err) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	defer f.Close()

	w.Header().Set("Content-Type", "application/json")
	bufio.NewReader(f).WriteTo(w)
}

func watcher(webDir string, cfg *Config, etcdCfg *etcd.Config, clusterName string, shutdown chan struct{}, shutdownSync sync.Once, wg *sync.WaitGroup) {
	//historyDir := filepath.Join(webDir, "redistHistory/")

	prefix := etcdCfg.EtcdKeyPrefix + clusterName
	redistState["clusterName"] = prefix

	cli, err := clientv3.New(etcdCfg.Config)
	if err != nil {
		log.Fatal(err)
	}
	defer cli.Close()

	session, err := concurrency.NewSession(
		cli, concurrency.WithTTL(SessionTTL),
	)
	if err != nil {
		glog.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	watchRet := make(chan error, 1)
	stopWatcher := make(chan struct{})

	go func() {
		select {
		case <-stopWatcher:
			return
		default:
			watchRet <- func() error { // wrap by a func to make sure watchRet gets item
				rch := cli.Watch(ctx, prefix, clientv3.WithPrefix(), clientv3.WithPrevKV())

				log.Printf("Watcher started on prefix %v\n", prefix)

				// Get initial state
				//originalVersion := ""
				//var lastRedistSave time.Time
				gresp, err := cli.Get(ctx, prefix, clientv3.WithPrefix())
				if err != nil {
					log.Printf("Failed to retrieve intitial cluster state from etcd: %v\n", err)
				} else {
					redistStateLock.Lock()
					for _, kv := range gresp.Kvs {
						redistState[string(kv.Key)] = string(kv.Value)
						/*if string(kv.Key) == prefix+"_version" {
							originalVersion = string(kv.Value)
						}*/
					}
					//saveRedistState(historyDir, originalVersion, 0, redistState)
					//lastRedistSave = time.Now()
					redistStateLock.Unlock()
				}
				log.Printf("Initial cluster state retrieved")

				// Start handling watcher events
				//saveIteration := 1
				for wresp := range rch {
					if wresp.Canceled {
						log.Println("\tWatcher cancelled")
					}

					if err := wresp.Err(); err != nil {
						return err
					}

					redistStateLock.Lock()
					redistDone := false
					for _, ev := range wresp.Events {
						if ev.Type == clientv3.EventTypeDelete {
							delete(redistState, string(ev.Kv.Key))
						} else {
							if string(ev.Kv.Key) == prefix+"_version" {
								redistDone = true
							}
							redistState[string(ev.Kv.Key)] = string(ev.Kv.Value)
						}
					}
					/*if time.Now().Sub(lastRedistSave) >= 1*time.Second {
						saveRedistState(historyDir, originalVersion, saveIteration, redistState)
						lastRedistSave = time.Now()
						saveIteration++
					}*/
					redistStateLock.Unlock()

					if redistDone && cfg.ShutdownDelaySecs > 0 {
						time.Sleep(time.Duration(cfg.ShutdownDelaySecs) * time.Second)
						signalShutdown(shutdown)

						//saveRedistState(historyDir, originalVersion, saveIteration, redistState)
					}
				}

				return nil
			}()
		}

	}()

	select {
	case <-session.Done():
	case <-ctx.Done():
	case err := <-watchRet:
		glog.Error(err)
	case <-shutdown:
	}

	cancel()
	close(stopWatcher)
	wg.Done()
}

/*func saveRedistState(historyDir string, originalVersion string, eventIteration int, state map[string]string) {
	dir := filepath.Join(historyDir, originalVersion)
	fpath := filepath.Join(dir, fmt.Sprintf("%d.json", eventIteration))

	os.MkdirAll(dir, 0777)

	file, err := os.Create(fpath)
	if err == nil {
		encoder := json.NewEncoder(file)
		encoder.Encode(state)
		file.Close()
	}
}*/
