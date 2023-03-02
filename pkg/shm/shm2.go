// +build darwin freebsd

package shm

import (
	"os"
	"syscall"
	"unsafe"
)

func Open(name string, flag int, mode os.FileMode) (file *os.File, err error) {
	var bname *byte
	if bname, err = syscall.BytePtrFromString(name); err != nil {
		return
	}
	fd, _, errno := syscall.Syscall(syscall.SYS_SHM_OPEN,
		uintptr(unsafe.Pointer(bname)),
		uintptr(flag), uintptr(mode),
	)
	if errno != 0 {
		err = errno
		return
	}
	file = os.NewFile(fd, name)
	return
}

func Close(name string) (err error) {
	var bname *byte
	if bname, err = syscall.BytePtrFromString(name); err != nil {
		return
	}
	if _, _, errno := syscall.Syscall(syscall.SYS_SHM_UNLINK,
		uintptr(unsafe.Pointer(bname)), 0, 0,
	); errno != 0 {
		err = errno
		return
	}
	return
}
