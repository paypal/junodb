package app

import (
	"flag"
	"fmt"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"strings"

	"juno/pkg/cmd"
	"juno/pkg/initmgr"
	"juno/pkg/version"
)

func init() {
	var (
		cmdManager          Manager
		cmdWorker           Worker
		cmdMonitoringWorker MonitoringWorker
	)
	cmdManager.Init("manager", "start as storage server manager")
	cmdWorker.Init("worker", "start as storage worker")
	cmdMonitoringWorker.Init("monitor", "start as storage monitoring worker")
	cmd.Register(&cmdManager)
	cmd.Register(&cmdWorker)
	cmd.Register(&cmdMonitoringWorker)
}

func Main() {
	defer initmgr.Finalize()

	var versionFlag bool
	var help bool

	flag.BoolVar(&versionFlag, "version", false, "display version information.")
	flag.BoolVar(&help, "h", false, "help")
	flag.BoolVar(&help, "help", false, "help")
	flag.Parse()

	if versionFlag {
		version.PrintVersionInfo()
		return
	}
	if help {
		printUsage()
	}
	numArgs := len(os.Args)

	if numArgs < 2 {
		fmt.Println("command is required")
		printUsage()
		os.Exit(1)
	}
	indexCommand := 1

	for i := 1; i < numArgs; i++ {
		if strings.HasPrefix(os.Args[i], "-") {
			indexCommand++
		} else {
			break
		}
	}

	if indexCommand < numArgs {
		cmd := cmd.GetCommand(os.Args[indexCommand])
		if cmd != nil {
			if err := cmd.Parse(os.Args[indexCommand+1:]); err == nil {
				cmd.Exec()
			} else {
				fmt.Printf("* command '%s' failed. %s\n", cmd.GetName(), err)
			}
		} else {
			fmt.Printf("command '%s' not specified", os.Args[indexCommand])
			return
		}
	}

}

//TODO may customize this or remove inappalicable glob flags
func printUsage() {
	progName := filepath.Base(os.Args[0])
	fmt.Printf(`
USAGE
  %s <command> <-c|-config>=<config file> [<options>]

`, progName)
	fmt.Printf(`OPTION
  -version
        print version info
  -h
        print usage info
`)
	cmd.PrintUsage()
}
