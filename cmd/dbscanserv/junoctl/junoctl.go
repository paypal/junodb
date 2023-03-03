package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"juno/cmd/dbscanserv/app"
	"juno/pkg/version"
	"juno/third_party/forked/golang/glog"
)

func main() {

	var (
		cfgFile     = "./config.toml"
		cmd         string
		zoneid      int
		nsFile      string
		showVersion bool
	)

	glog.InitLogging("info", "[junoctl] ")
	defer glog.Finalize()

	flag.StringVar(&cmd, "cmd", "", "specify command.")
	flag.IntVar(&zoneid, "start", 0, "specify starting zone for ns_delete.")
	flag.StringVar(&nsFile, "f", "", "specify ns file for ns_delete.")
	flag.BoolVar(&showVersion, "version", false, "display version info")

	flag.Parse()
	if showVersion {
		version.PrintVersionInfo()
		if len(cmd) == 0 {
			return
		}
	}

	if len(cmd) == 0 {
		printUsage()
		return
	}

	if cmd != "delete_ns" {
		glog.Exitf("[ERROR] Invalid cmd: %s.", cmd)
		return
	}

	cmdLine := app.NewCmdLine2(cfgFile, cmd, zoneid, nsFile)
	cmdLine.HandleCommand()
}

func printUsage() {
	progName := filepath.Base(os.Args[0])
	fmt.Printf("Usage:        ./%s -cmd delete_ns -f <file>\n",
		progName)
}
