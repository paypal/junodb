package main

import (
	"fmt"

	"juno/cmd/tools/cmd/cfg"
	"juno/pkg/cmd"
)

func main() {
	cfg.RegisterRtConfig()
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
