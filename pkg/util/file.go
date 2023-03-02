package util

import (
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"text/tabwriter"
)

func GetNumOpenFDs() (n int) {
	// alternatives on Unix/Linux:
	// * /proc/<pid>/fd
	// * lsof
	var rlim syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlim); err == nil {
		for i := 0; i < int(rlim.Cur); i++ {
			var stat syscall.Stat_t
			if e := syscall.Fstat(i, &stat); e == nil {
				n++
			}
		}
	}
	return
}

func IsSocketFD(fd int) bool {
	if fd != -1 {
		var stat syscall.Stat_t
		if e := syscall.Fstat(fd, &stat); e == nil {
			if stat.Mode&syscall.S_IFSOCK != 0 {
				return true
			}
		}
	}
	return false
}

func IsSocket(f *os.File) bool {
	if f != nil {
		if st, err := f.Stat(); err == nil {
			if st.Mode()&os.ModeSocket != 0 {
				return true
			}
		}
	}
	return false
}

func WriteFileInfo(files []*os.File, w io.Writer) {
	wo := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	for _, f := range files {
		if st, err := f.Stat(); err == nil {
			fmt.Fprintf(w, "\t%s\t%s\n", f.Name(), st.Mode().String())
		}
	}
	wo.Flush()
}

func Lsof(w io.Writer) {
	if lsof, err := exec.Command("lsof", "-b", "-n", "-p", strconv.Itoa(os.Getpid())).Output(); err == nil {
		fmt.Fprintf(w, string(lsof))
	}
}
