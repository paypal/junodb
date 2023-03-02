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
package frwk

import (
	"juno/third_party/forked/golang/glog"
)

var (
	LOG_ALERT   glog.Verbose
	LOG_WARN    glog.Verbose
	LOG_INFO    glog.Verbose
	LOG_DEBUG   glog.Verbose
	LOG_VERBOSE glog.Verbose

	level logLevel
)

type logLevel struct{}

func (l *logLevel) SetLevel() {
	LOG_ALERT = glog.V(1)
	LOG_WARN = glog.V(2)
	LOG_INFO = glog.V(3)
	LOG_DEBUG = glog.V(4)
	LOG_VERBOSE = glog.V(5)
}

func init() {
	glog.RegisterPackage("frwk", &level)
}
