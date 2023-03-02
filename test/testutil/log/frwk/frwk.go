package frwk

import (
	"juno/third_party/forked/golang/glog"
)

var (
	LOG_ALERT   glog.Verbose
	LOG_WARN    glog.Verbose
	LOG_INFO    glog.Verbose
	LOG_DEBUG   glog.Verbose
	LOG_VERBOSE glog.Verbose

	level logLevel
)

type logLevel struct{}

func (l *logLevel) SetLevel() {
	LOG_ALERT = glog.V(1)
	LOG_WARN = glog.V(2)
	LOG_INFO = glog.V(3)
	LOG_DEBUG = glog.V(4)
	LOG_VERBOSE = glog.V(5)
}

func init() {
	glog.RegisterPackage("frwk", &level)
}
