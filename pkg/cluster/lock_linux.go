package cluster

import (
	"juno/third_party/forked/golang/glog"
	"syscall"
)

func lockFile(fd int, mode int) bool {

	err := syscall.Flock(fd, mode|syscall.LOCK_NB)
	if err != nil {
		return false // locked out
	}

	return true // acquired lock
}

func unlockFile(fd int) (err error) {

	err = syscall.Flock(fd, syscall.LOCK_UN)
	if err != nil {
		glog.Errorf("%v", err)
	}

	return err
}
