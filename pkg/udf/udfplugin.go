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

package udf

import (
	"errors"
	"os"
	"path/filepath"
	"plugin"
	"strings"

	"juno/third_party/forked/golang/glog"
)

func loadOneUDFbyName(dir string, name string) (iudf IUDF, err error) {
	p, err := plugin.Open(dir + "/" + name)
	if err != nil {
		return nil, err
	}

	getinterface, err := p.Lookup("GetUDFInterface")
	if err != nil {
		return nil, err
	}

	udf, err := getinterface.(func() (interface{}, error))()
	if err != nil {
		return nil, err
	}

	iudf, ok := udf.(IUDF)
	if !ok {
		return nil, errors.New("bad UDF")
	}
	glog.Infof("loaded one udf plugin: %s", name)
	return iudf, nil
}

func loadUDFPlugins(udfdir string, mp *UDFMap) {
	if len(udfdir) == 0 {
		return
	}

	file, err := os.Open(udfdir)

	if err != nil {
		glog.Infof("udf not exists under %s", udfdir)
		return
	}
	defer file.Close()

	list, _ := file.Readdirnames(0)
	for _, name := range list {
		// load one udf.
		if filepath.Ext(name) != ".so" {
			continue
		}

		iudf, err := loadOneUDFbyName(udfdir, name)
		if err == nil {
			pluginName := strings.TrimSuffix(name, filepath.Ext(name))
			if _, exists := (*mp)[pluginName]; exists {
				glog.Errorf("udf with same name %s already exists, ignore", pluginName)
			} else {
				(*mp)[pluginName] = iudf
			}
		}
	}
}
