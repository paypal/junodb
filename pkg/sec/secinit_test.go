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
package sec

import (
	"fmt"
	"testing"

	"github.com/BurntSushi/toml"
)

func loadCfg() (*Config, error) {
	cfg := &Config{}
	if _, err := toml.DecodeFile("./config.toml", cfg); err != nil {
		fmt.Println(err)
		return nil, err
	}
	return cfg, nil
}

func initCfg() error {
	cfg := &Config{}
	if _, err := toml.DecodeFile("./config.toml", cfg); err != nil {
		fmt.Println(err)
		return err
	}
	if err := InitSecConfig(cfg); err != nil { // ####
		return err
	}
	return nil
}

func loadFileCfg(file string) (*Config, error) {
	cfg := &Config{}
	if _, err := toml.DecodeFile(file, cfg); err != nil {
		fmt.Println(err)
		return nil, err
	}
	return cfg, nil
}

func Test_initializeSec(t *testing.T) {
	type args struct {
		cfg             *Config
		flag            Flag
		isServerManager bool
	}

	testcfg, err := loadCfg()
	if err != nil {
		panic(err)
	}

	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
		{"GenSecretsInit", args{testcfg, Flag(KFlagServerTlsEnabled | KFlagClientTlsEnabled), true}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := initializeSec(tt.args.cfg, tt.args.flag, tt.args.isServerManager); (err != nil) != tt.wantErr {
				t.Errorf("initializeSec() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
