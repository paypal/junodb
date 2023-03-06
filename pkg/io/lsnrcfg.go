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
  
package io

import (
	"fmt"
	"strings"
)

type (
	ServiceEndpoint struct {
		Addr       string
		Network    string
		SSLEnabled bool
	}

	ListenerConfig struct {
		ServiceEndpoint
		Name string
	}
)

func (p *ServiceEndpoint) Validate() (err error) {
	if len(p.Addr) == 0 {
		err = fmt.Errorf("ServiceEndpoint.Addr not specified")
	}
	return
}

///TODO . now, it is just a very simple one to construct connection string
func (p *ServiceEndpoint) GetConnString() (str string) {
	if p.SSLEnabled {
		str = "ssl:"
	}
	if strings.Contains(p.Addr, ":") {
		str += p.Addr
	} else {
		str += ":" + p.Addr
	}
	return
}

///TODO . now, it is just a very simple function to parse connection string
func (p *ServiceEndpoint) SetFromConnString(connStr string) error {
	str := strings.ToLower(connStr)
	if strings.HasPrefix(str, "ssl:") {
		str = strings.TrimPrefix(str, "ssl:")
		p.SSLEnabled = true
	}
	if !strings.Contains(str, ":") { ///TODO may check further if it is a port number, or use regexp
		p.Addr = ":" + str
	} else {
		p.Addr = str
	}
	return nil
}

func (cfg *ListenerConfig) SetDefaultIfNotDefined() {
	///TODO
}
