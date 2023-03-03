//Package shm implements functions for using shared memory
package shm

import (
	"fmt"
	"os"
	"syscall"
)

func Ftruncate(file *os.File, size int) (err error) {
	if file == nil {
		err = fmt.Errorf("nil file")
		return
	}
	err = syscall.Ftruncate(int(file.Fd()), int64(size))
	return
}

func MmapForReadWrite(file *os.File, offset int64, length int) (data []byte, err error) {
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flag := syscall.MAP_SHARED
	return Mmap(file, offset, length, prot, flag)
}

func MmapForRead(file *os.File, offset int64, length int) (data []byte, err error) {
	prot := syscall.PROT_READ
	flag := syscall.MAP_PRIVATE
	return Mmap(file, offset, length, prot, flag)
}

func Mmap(file *os.File, offset int64, length int, prot int, flags int) (data []byte, err error) {
	if file == nil {
		err = fmt.Errorf("nil file")
		return
	}
	return syscall.Mmap(int(file.Fd()), offset, length, prot, flags)
}

func Munmap(data []byte) error {
	return syscall.Munmap(data)
}
