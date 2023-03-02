// +build linux

package shm

import (
	"fmt"
	"os"
	"path/filepath"
)

func shmName(name string) string {
	return filepath.Join("/dev/shm", name)
}

func Open(name string, flag int, mode os.FileMode) (file *os.File, err error) {
	file, err = os.OpenFile(shmName(name), flag, mode)
	return
}

func Close(name string) error {
	if err := os.Remove(shmName(name)); err != nil {
		fmt.Println(err)
		return err
	}
	return nil
}
