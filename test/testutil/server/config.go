package server

import (
	"juno/cmd/proxy/config"
	"juno/pkg/io"
	cal "juno/pkg/logging/cal/config"
	sherlock "juno/pkg/logging/sherlock"
	"juno/pkg/sec"
	"juno/pkg/util"
)

type ServerDef struct {
	//Currently, only used for mockss
	Type string
	//Specifies the path to the server executables
	BinDir string
	//Specifies the command to start the server
	//If not defined, an InProcess server stub will be created.
	StartCmd string
	//Specifies the command to stop the server.
	//If the server runs on the same host as the tests, and the StartCmd is
	//defined as a binary executable, one may leave StopCmd empty. When
	//stopping the server, the cluster will call Kill <pid of the server>
	StopCmd string
	//Specifies how long to wait for the server to start. (temporary)
	StartWaitTime util.Duration
	//Specifies how long to wait for the server to stop. (temporary)
	StopWaitTime util.Duration
}

func (s *ServerDef) IsInProcess() bool {
	return len(s.StartCmd) == 0
}

type ClusterConfig struct {
	ProxyAddress            io.ServiceEndpoint
	ProxyToBeReplicate      io.ServiceEndpoint
	Proxy                   ServerDef
	StorageServer           ServerDef
	ProxyConfig             *config.Config
	CAL                     cal.Config
	SSdir                   string
	WalDir                  string
	LogLevel                string
	Proxydir                string
	Githubdir               string
	MarkDown                string
	RedistType              string
	SecondHostSSdir         string
	AddRemoveSecondHost     string
	EtcdServerRestartGitDir string
	Sec                     sec.Config
	Sherlock                sherlock.Config
}
