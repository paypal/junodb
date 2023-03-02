package glog

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

type ILogLevel interface {
	SetLevel()
}

// default is LOG_INFO
var (
	LOG_ERROR   Verbose = true
	LOG_WARN    Verbose = true
	LOG_INFO    Verbose = true
	LOG_DEBUG   Verbose = false
	LOG_VERBOSE Verbose = false
	pmap        map[string]ILogLevel

	chLogWrite  = make(chan *buffer, 10000)
	chDone      = make(chan bool)
	wg          sync.WaitGroup
	cleanupOnce sync.Once
)

func init() {
	wg.Add(1)
	go doLogWrite()
}

func Initialize(args ...interface{}) (err error) {
	sz := len(args)
	if sz < 2 {
		err = fmt.Errorf("two arguments expected")
		return
	}
	var level string
	var appName string
	var ok bool
	if level, ok = args[0].(string); !ok {
		err = fmt.Errorf("a string log level expected")
		return
	}
	if appName, ok = args[1].(string); !ok {
		err = fmt.Errorf("a string appname expected")
		return
	}
	InitLogging(level, appName)
	return
}

func finalizeOnce() {
	cleanupOnce.Do(func() {
		close(chDone)
		wg.Wait()
	})
}

func Finalize() {
	finalizeOnce()
}

func InitLogging(level string, appName string) {
	flag.Lookup("logtostderr").Value.Set("true")
	SetAppName(appName)

	var glevel string

	if strings.EqualFold("error", level) {
		glevel = "1"
	} else if strings.EqualFold("warning", level) {
		glevel = "2"
	} else if strings.EqualFold("debug", level) {
		glevel = "4"
	} else if strings.EqualFold("verbose", level) {

		glevel = "5"
	} else { //default is info
		glevel = "3"
	}

	flag.Lookup("v").Value.Set(glevel)

	LOG_ERROR = V(1)
	LOG_WARN = V(2)
	LOG_INFO = V(3)
	LOG_DEBUG = V(4)
	LOG_VERBOSE = V(5)

	for _, value := range pmap {
		value.SetLevel()
	}
}

func doLogWrite() {
	drainLogWriteChannel := func(timeout time.Duration) {
		timer := time.NewTimer(timeout)

		for {
			select {
			case <-timer.C:
				return
			case buf, ok := <-chLogWrite:
				if !ok {
					return
				}
				data := buf.Bytes()
				os.Stderr.Write(data)
				logging.putBuffer(buf)
			default:
				return
			}
		}
	}
	defer func() {
		drainLogWriteChannel(10 * time.Second)
		wg.Done()
	}()

	for {
		select {
		case <-chDone:
			return
		case buf, ok := <-chLogWrite:
			if !ok {
				return
			}
			data := buf.Bytes()
			os.Stderr.Write(data)
			logging.putBuffer(buf)
		}
	}
}

// wrappers to glog APIs so we can check log_level before callling into real code
func Info(args ...interface{}) {
	if LOG_INFO {
		logging.print(infoLog, args...)
	}
}

func InfoDepth(depth int, args ...interface{}) {
	if LOG_INFO {
		logging.printDepth(infoLog, depth, args...)
	}
}

func Infoln(args ...interface{}) {
	if LOG_INFO {
		logging.println(infoLog, args...)
	}
}

func Infof(format string, args ...interface{}) {
	if LOG_INFO {
		logging.printf(infoLog, format, args...)
	}
}

func Warning(args ...interface{}) {
	if LOG_WARN {
		logging.print(warningLog, args...)
	}
}

func WarningDepth(depth int, args ...interface{}) {
	if LOG_WARN {
		logging.printDepth(warningLog, depth, args...)
	}
}

func Warningln(args ...interface{}) {
	if LOG_WARN {
		logging.println(warningLog, args...)
	}
}

func Warningf(format string, args ...interface{}) {
	if LOG_WARN {
		logging.printf(warningLog, format, args...)
	}
}

func Error(args ...interface{}) {
	if LOG_ERROR {
		logging.print(errorLog, args...)
	}
}

func ErrorDepth(depth int, args ...interface{}) {
	if LOG_ERROR {
		logging.printDepth(errorLog, depth, args...)
	}
}

func Errorln(args ...interface{}) {
	if LOG_ERROR {
		logging.println(errorLog, args...)
	}
}

func Errorf(format string, args ...interface{}) {
	if LOG_ERROR {
		logging.printf(errorLog, format, args...)
	}
}

func VerboseDepth(depth int, args ...interface{}) {
	if LOG_VERBOSE {
		logging.printDepth(verboseLog, depth, args...)
	}
}

func Verboseln(args ...interface{}) {
	if LOG_VERBOSE {
		logging.println(verboseLog, args...)
	}
}

func Verbosef(format string, args ...interface{}) {
	if LOG_VERBOSE {
		logging.printf(verboseLog, format, args...)
	}
}

func VerboseInfof(format string, args ...interface{}) {
	logging.printf(verboseLog, format, args...)
}

func Debug(args ...interface{}) {
	if LOG_DEBUG {
		logging.print(debugLog, args...)
	}
}

func DebugDepth(depth int, args ...interface{}) {
	if LOG_DEBUG {
		logging.printDepth(debugLog, depth, args...)
	}
}

func Debugln(args ...interface{}) {
	if LOG_DEBUG {
		logging.println(debugLog, args...)
	}
}

func Debugf(format string, args ...interface{}) {
	if LOG_DEBUG {
		logging.printf(debugLog, format, args...)
	}
}

func DebugInfoln(args ...interface{}) {
	logging.println(debugLog, args...)
}

func DebugInfof(format string, args ...interface{}) {
	logging.printf(debugLog, format, args...)
}

func init() {
	pmap = make(map[string]ILogLevel)
}

func RegisterPackage(name string, level ILogLevel) {
	if pmap == nil {
		pmap = make(map[string]ILogLevel)
	}

	pmap[name] = level
}

func SetVModule(value string) {
	logging.vmodule.Set(value)
}
