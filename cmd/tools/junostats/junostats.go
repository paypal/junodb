package main

import (
	"fmt"

	"juno/cmd/tools/cmd/stats"
	"juno/pkg/cmd"
)

func main() {
	if command, args := cmd.ParseCommandLine(); command != nil {
		if err := command.Parse(args); err == nil {
			command.Exec()
		} else {
			fmt.Printf("* command '%s' failed. %s\n", command.GetName(), err)
		}
	} else {
		cmd.PrintVersionOrUsage()
	}
}

func init() {
	pstats := &stats.CmdProxyStats{}
	pstats.Init("proxy", "get proxy statistics")
	cmd.Register(pstats)
	sstats := &stats.CmdStorageStats{}
	sstats.Init("storage", "get storageserv statistics")
	cmd.Register(sstats)
}
