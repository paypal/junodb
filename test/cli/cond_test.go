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

package cli

import (
	"testing"

	"juno/pkg/client"
	"juno/third_party/forked/golang/glog"
)

func TestCond(t *testing.T) {
	glog.Info("")
	glog.Info("=== TestCond")

	server, _ = NewCmdWithConfig(serverAddr, 5)

	if server == nil {
		t.Errorf("Failed to init")
		return
	}
	for i := 100; i < 105; i++ {
		ctx, err := server.createKey(i)
		if err != nil {
			t.Errorf("Create failed")
		}
		_, err = server.updateKeyWithCond(i, ctx)
		if err != nil {
			t.Errorf("Update WithCond failed")
			continue
		}
		_, err = server.updateKeyWithCond(i, ctx)
		if err == nil || err != client.ErrConditionViolation {
			t.Errorf("Expected condition violation")
		}
	}
}
