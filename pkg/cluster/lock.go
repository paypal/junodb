// +build !linux

package cluster

func lockFile(fd int, mode int) bool {

	return false
}

func unlockFile(fd int) (err error) {

	return nil
}
