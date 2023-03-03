package cfg

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"juno/pkg/cfg"
	"juno/pkg/cmd"
)

type cmdConfUnify struct {
	cmd.Command
	optOutFileName string
	optOutFormat   string
}

func (c *cmdConfUnify) Init(name string, desc string) {
	c.Command.Init(name, desc)
	c.StringOption(&c.optOutFileName, "o|output-filename", "config.toml", "output filename")
	c.StringOption(&c.optOutFormat, "f|output-format", "toml", "output format {toml|text}")
	c.SetSynopsis("[options] <toml file name> [<toml file name>]")
}

func (c *cmdConfUnify) Exec() {
	c.Validate()
	c.optOutFormat = strings.ToLower(c.optOutFormat)
	file, err := os.Create(c.optOutFileName)
	if err != nil {
		fmt.Printf("fail to create file %s\n", c.optOutFileName)
		return
	}
	defer file.Close()

	if c.NArg() < 1 {
		fmt.Println("no input file")
		return
	}

	var unified cfg.Config

	for _, f := range c.Args() {
		var cfg cfg.Config
		if err := cfg.ReadFromTomlFile(f); err != nil {
			fmt.Printf("%s, Error: %s", f, err)
			return
		}
		unified.Merge(&cfg)
	}
	writer := bufio.NewWriter(file)

	if c.optOutFormat == "toml" {
		unified.WriteToToml(writer)

	} else {
		unified.WriteToKVList(writer)
	}
	writer.Flush()
}

func init() {
	c := &cmdConfUnify{}
	c.Init("config", "unify the given toml configuration file(s)")

	cmd.Register(c)
}
