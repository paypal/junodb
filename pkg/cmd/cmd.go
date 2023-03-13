//
//  Copyright 2023 PayPal Inc.
//
//  Licensed to the Apache Software Foundation (ASF) under one or more
//  contributor license agreements.  See the NOTICE file distributed with
//  this work for additional information regarding copyright ownership.
//  The ASF licenses this file to You under the Apache License, Version 2.0
//  (the "License"); you may not use this file except in compliance with
//  the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

package cmd

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"juno/third_party/forked/golang/glog"

	"juno/pkg/version"
)

var (
	commands           = make(map[string]ICommand)
	groups             = make(map[string]*Group)
	notGroupedCommands []ICommand
)

type (
	ICommand interface {
		GetName() string
		GetDesc() string //get short description
		GetSynopsis() string
		GetDetails() string
		GetOptionDesc() string
		GetExample() string
		AddExample(cmdExample string, desc string)
		AddDetails(txt string)
		Init(name string, desc string)
		Exec()
		Parse(args []string) error
		PrintUsage()
	}

	Command struct {
		Option
		name       string
		desc       string //short description. (one ine)
		synopsis   string
		details    string
		examples   string
		optVModule string
	}

	Group struct {
		cmds []ICommand
		name string
	}
)

func (c *Command) Init(name string, desc string) {
	c.name = name
	c.desc = desc
	c.Option.Init(name, flag.ExitOnError)
	c.StringVar(&c.optVModule, "vmodule", "", "comma-separated list of pattern=N settings for file-filtered logging")
	c.Option.Usage = c.PrintUsage
}

func optionString(name, shortName string) string {
	var opts []string
	if name != "" {
		opts = append(opts, "-"+name)
	}
	if shortName != "" {
		opts = append(opts, "-"+shortName)
	}
	return strings.Join(opts, ",")
}

func (c *Command) SetSynopsis(str string) {
	c.synopsis = str
}

func (c *Command) GetName() string {
	return c.name
}

func (c *Command) GetDesc() string {
	return c.desc
}

func (c *Command) GetSynopsis() string {
	return c.synopsis
}

func (c *Command) GetDetails() string {
	return c.details
}

func (c *Command) GetExample() string {
	return c.examples
}

func (c *Command) AddExample(cmdExample string, desc string) {
	c.examples += desc + "\n\t\t" + cmdExample + "\n\n"
}

func (c *Command) AddDetails(txt string) {
	c.details += txt
}

func (c *Command) Write(w io.Writer) {
	wo := tabwriter.NewWriter(w, 0, 4, 2, ' ', 0)
	err := usageTemplate.Execute(wo, c)
	if err != nil {
		fmt.Fprintln(w, err)
	}
	wo.Flush()
}

func (c *Command) PrintUsage() {
	less := exec.Command("less")
	var buf bytes.Buffer
	c.Write(&buf)
	less.Stdin = &buf
	less.Stdout = os.Stdout
	err := less.Run()
	if err != nil {
		c.Write(os.Stdout)
	}
}

func (c *Command) Validate() {
	if !c.Parsed() {
		glog.Exit("not parsed")
	}
}

func (c *Command) Parse(arguments []string) (err error) {
	if err = c.Option.Parse(arguments); err == nil {
		if c.optVModule != "" {
			glog.SetVModule(c.optVModule)
		}
	}
	return
}

func RegisterNewGroup(name string, cmds ...ICommand) (grp *Group) {
	if _, grpFound := groups[name]; grpFound {
		fmt.Printf("group %s has been registered.", name)
		return
	}
	grp = &Group{name: name}
	//	commonOpt := make(map[string]bool)
	for _, c := range cmds {
		if register(c) {
			grp.cmds = append(grp.cmds, c)
		}
	}
	groups[name] = grp
	return
}

func Register(c ICommand) bool {
	if register(c) {
		notGroupedCommands = append(notGroupedCommands, c)
		return true
	}
	return false
}

func register(c ICommand) bool {
	if _, found := commands[c.GetName()]; found {
		fmt.Printf("Command %s has been registered.", c.GetName())
		return false
	}
	commands[c.GetName()] = c
	return true
}

func GetCommand(name string) ICommand {
	if cmd, ok := commands[name]; ok {
		return cmd
	}
	return nil
}

func ParseCommandLine() (cmd ICommand, args []string) {
	numArgs := len(os.Args)

	for i := 1; i < numArgs; i++ {
		arg := os.Args[i]
		if cmd == nil {
			cmd = GetCommand(arg)
			if cmd != nil {
				args = append(args, os.Args[i+1:]...)
				break
			}
		}
		args = append(args, arg)
	}
	return
}

func Write(w io.Writer) {
	progName := filepath.Base(os.Args[0])
	fmt.Fprintf(w, "\nUSAGE\n  %s [-version] [[options] <command> [<args>]] \n\n", progName)
	WriteCommand(w)
}

func WriteCommand(w io.Writer) {
	if len(groups)+len(notGroupedCommands) == 0 {
		return
	}
	fmt.Fprintln(w, "\nCOMMAND")

	for _, g := range groups {
		fmt.Fprintf(w, "  %s\n", g.name)
		for _, c := range g.cmds {
			fmt.Fprintf(w, "    * %s\n      %s\n", c.GetName(), c.GetDesc())
		}
	}
	if len(notGroupedCommands) != 0 {
		if len(groups) != 0 {
			fmt.Fprintln(w, "  others")
		}
		for _, c := range notGroupedCommands {
			fmt.Fprintf(w, "    * %s\n      %s\n", c.GetName(), c.GetDesc())
		}
	}
}

func PrintUsage() {
	less := exec.Command("less")
	var buf bytes.Buffer
	Write(&buf)
	less.Stdin = &buf
	less.Stdout = os.Stdout
	err := less.Run()
	if err != nil {
		Write(os.Stdout)
	}
}

func PrintVersionOrUsage() {
	var option Option
	var displayVersion bool
	option.BoolOption(&displayVersion, "version", false, "display version info.")
	option.Usage = PrintUsage
	if err := option.Parse(os.Args[1:]); err == nil {
		if displayVersion {
			version.PrintVersionInfo()
		}
	}
}
