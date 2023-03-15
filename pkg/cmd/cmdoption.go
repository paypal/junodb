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
	"flag"
	"fmt"
	"strings"
)

type (
	Option struct {
		flag.FlagSet
		optsDesc string
	}
)

func (o *Option) ValueOption(value flag.Value, name string, usage string) {
	if name != "" {
		names := strings.Split(name, "|")
		var opt string
		first := true
		for _, n := range names {
			if n != "" {
				o.Var(value, n, "")
				if first {
					first = false
				} else {
					opt += ", "
				}
				opt += "-" + n
			}
		}
		o.optsDesc += fmt.Sprintf("  %s value\n    \t%s \n\n", opt, usage)
	}
}

func (o *Option) StringOption(p *string, name string, value string, usage string) {
	if name != "" {
		names := strings.Split(name, "|")
		var opt string
		first := true
		for _, n := range names {
			if n != "" {
				o.StringVar(p, n, value, "")
				if first {
					first = false
				} else {
					opt += ", "
				}
				opt += "-" + n
			}
		}
		o.optsDesc += fmt.Sprintf("  %s string\n    \t(default \"%s\")\n    \t%s\n\n", opt, value, usage)
	}
}

func (o *Option) BoolOption(p *bool, name string, value bool, usage string) {
	if name != "" {
		names := strings.Split(name, "|")
		var opt string
		first := true
		for _, n := range names {
			if n != "" {
				o.BoolVar(p, n, value, "")
				if first {
					first = false
				} else {
					opt += ", "
				}
				opt += "-" + n
			}
		}
		o.optsDesc += fmt.Sprintf("  %s\n    \t(default %v)\n    \t%s\n\n", opt, value, usage)
	}
}

func (o *Option) UintOption(p *uint, name string, value uint, usage string) {
	if name != "" {
		names := strings.Split(name, "|")
		var opt string
		first := true
		for _, n := range names {
			if n != "" {
				o.UintVar(p, n, value, "")
				if first {
					first = false
				} else {
					opt += ", "
				}
				opt += "-" + n
			}
		}
		o.optsDesc += fmt.Sprintf("  %s uint\n    \t(default %v)\n    \t%s\n\n", opt, value, usage)
	}
}

func (o *Option) IntOption(p *int, name string, value int, usage string) {
	if name != "" {
		names := strings.Split(name, "|")
		var opt string
		first := true
		for _, n := range names {
			if n != "" {
				o.IntVar(p, n, value, "")
				if first {
					first = false
				} else {
					opt += ", "
				}
				opt += "-" + n
			}
		}
		o.optsDesc += fmt.Sprintf("  %s int\n\t    (default %v)\n    \t%s\n\n", opt, value, usage)
	}
}

func (o *Option) GetOptionDesc() string {
	return o.optsDesc
}
