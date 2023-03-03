package main

import (
	"fmt"

	_ "juno/cmd/tools/cmd/cfg"
	_ "juno/cmd/tools/cmd/cli"
	_ "juno/cmd/tools/cmd/insp"
	"juno/pkg/cmd"
	"juno/pkg/logging/cal"
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
	if cal.IsEnabled() {
		cal.CalClient().Flush()
	}
}
