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
//  Package utility provides the utility interfaces for mux package
//  
// -*- tab-width: 2 -*-

package sherlock

import (
	"bufio"
	"errors"

	//"fmt"
	"io"
	"os"
	"strings"
)

//var frontierEndPoints = map[string]string{
//	"phx":   "frontierproxy-vip.phx.paypal.com",
//	"ccg01": "frontierproxy-vip.phx.paypal.com",
//	"slc-a": "frontierproxy-vip-a.slc.paypal.com",
//	"slc-b": "frontierproxy-vip-a.slc.paypal.com",
//	"slca":  "frontierproxy-vip-a.slc.paypal.com",
//	"slcb":  "frontierproxy-vip-a.slc.paypal.com",
//	"ccg23": "frontierproxy-vip.ccg23.lvs.paypalinc.com",
//	"qa":    "sherlock-frontier-vip.qa.paypal.com",
//}

//const defaultPort = 80

// getEnvFromSyshieraYaml returns the env: line from /etc/syshiera.yaml
func getEnvFromSyshieraYaml() (string, error) {
	filePath := "/etc/syshiera.yaml"
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	fileReader := bufio.NewReader(file)
	scanner := bufio.NewScanner(fileReader)
	for scanner.Scan() {
		line := scanner.Text()
		err = scanner.Err()
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}
		pos := strings.Index(line, "dc: ")
		if pos == -1 {
			continue
		}
		return strings.TrimSpace(line[3:len(line)]), nil
	}
	err = errors.New("dc: not found in /etc/syshiera.yaml")
	return "", err
}

// NewFrontierClientNormalEndpoints uses /etc/syshiera.yaml or else qa endpoint.
func NewFrontierClientNormalEndpoints(appSvc string,
	profile string) (*FrontierClient, error) {
	var sherlockEnv string
	host := ShrLockConfig.SherlockEndpoint
	if host == "" {
		host = "sherlock-frontier-vip.qa.paypal.com"
	}
	if strings.Index(host, "qa") > -1 {
		sherlockEnv = "qa"
	} else {
		sherlockEnv = "prod"
	}
	//fmt.Println("Frontier Details : ", host, defaultPort, sherlockEnv, appSvc, profile)
	return NewFrontierClient(host,
		ShrLockConfig.SherlockPort,
		"pp",
		sherlockEnv,
		appSvc,
		profile)
}
