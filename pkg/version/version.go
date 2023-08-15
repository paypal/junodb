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

package version

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"

	"github.com/paypal/junodb/pkg/debug"
)

var (
	Version   string = "1.1"
	Revision  string = ""
	BuildId   string = ""
	BuildTime string = ""
)

func OnelineVersionString() string {
	return Version + "." + Revision + "." + BuildId
}

func WriteVersionInfo(w io.Writer) {
	binName := filepath.Base(os.Args[0])
	var buildType = "release"
	if debug.DEBUG {
		buildType = "debug"
	}
	fmt.Fprintf(w, "\nJuno %s %s (%s build)\n\n", filepath.Base(binName), Version, buildType)

	if BuildId != "" {
		fmt.Fprintf(w, "  Build No. : %s\n", BuildId)
	}
	if Revision != "" {
		fmt.Fprintf(w, "  Git Commit: %s\n", Revision)
	}
	fmt.Fprintf(w, "  Go Version: %s\n  OS/Arch   : %s/%s\n", runtime.Version(), runtime.GOOS, runtime.GOARCH)
	if BuildTime != "" {
		fmt.Fprintf(w, "  Built     : %s\n", BuildTime)
	}
	fmt.Fprintf(w, "\n")
}

func PrintVersionInfo() {
	WriteVersionInfo(os.Stdout)
}

func HttpHandler(w http.ResponseWriter, r *http.Request) {
	WriteVersionInfo(w)
}
