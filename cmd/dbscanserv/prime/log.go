package prime

import (
	"fmt"
	"os"

	"juno/third_party/forked/golang/glog"
)

var (
	fileWriter *os.File
	logfile    string
	cmdMode    bool
)

func SetCommandMode() {
	cmdMode = true
}

func IsCommandMode() bool {
	return cmdMode
}

func InitFileWriter(file string) {

	if cmdMode {
		return
	}

	if len(file) == 0 {
		file = "dbscan.log"
	}

	logfile = file

	var err error
	fileWriter, err = os.OpenFile(file,
		os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0666)

	if err != nil {
		glog.Errorf("%s", err)
	}
	os.Chmod(file, 0666)
}

func TruncateLog(checkSize bool) {

	if fileWriter == nil {
		return
	}

	if checkSize {
		info, err := os.Stat(logfile)
		if err != nil {
			return
		}
		if info.Size() < 128*1024*1024 { // 128 MB
			return
		}
	}

	var err error
	fileWriter.Close()
	fileWriter, err = os.OpenFile(logfile,
		os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0666)

	if err != nil {
		glog.Errorf("%s", err)
	}
	os.Chmod(logfile, 0666)
}

func LogMsg(format string, a ...interface{}) (n int, err error) {
	if cmdMode {
		glog.Infof(format, a...)
		return 0, nil
	}

	if fileWriter == nil {
		return 0, nil
	}

	return fmt.Fprintf(fileWriter, format+"\n", a...)
}
