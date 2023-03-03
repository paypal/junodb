package ioutil

import (
	"io"
	"net"
	"os"
	"syscall"

	"juno/third_party/forked/golang/glog"
)

func LogError(err error) {
	if err == nil {
		return
	}

	if nerr, ok := err.(net.Error); ok {
		if nerr.Timeout() {
			glog.WarningDepth(1, err)
			return
		}
	}

	if opErr, ok := err.(*net.OpError); ok {
		if sErr, ok := opErr.Err.(*os.SyscallError); ok {
			if sErr.Err == syscall.ECONNRESET {
				glog.DebugDepth(1, err)
				return
			}
		}
		if opErr.Err.Error() == "use of closed network connection" {
			glog.DebugDepth(1, err)
			return
		}
	}

	if err == io.EOF {
		glog.DebugDepth(1, err)
	} else {
		glog.WarningDepth(1, err)
	}
}
