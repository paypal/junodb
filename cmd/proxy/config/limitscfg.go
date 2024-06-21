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

package config

import (
	"fmt"
	"math"
	"sync"

	"github.com/paypal/junodb/third_party/forked/golang/glog"

	"github.com/paypal/junodb/pkg/cfg"
)

var (
	limitsMutex sync.RWMutex

	limitsConfig *LimitsConfig = &LimitsConfig{ // limitsConfig has to be reset when Conf getting updated
		Limits: Limits{
			MaxTimeToLive:    uint32(Conf.MaxTimeToLive),
			MaxPayloadLength: uint32(Conf.MaxPayloadLength),
			MaxKeyLength:     uint32(Conf.MaxKeyLength),
		},
		Namespace: map[string]Limits{
			JunoInternalNamespace(): defaultJunoInternalLimits(),
		},
	}
)

type (
	Limits struct {
		MaxTimeToLive    uint32
		MaxPayloadLength uint32
		MaxKeyLength     uint32
	}

	LimitsConfig struct {
		Limits
		Timestamp int64
		Namespace map[string]Limits
	}
)

func resetDefaultLimitsConfig() {
	limitsMutex.Lock()
	conf := defaultLimitsConfig()
	limitsConfig = &conf
	limitsMutex.Unlock()
}

// JunoInternalNamespace returns the namespace for Juno internal records
func JunoInternalNamespace() string {
	return "juno_internal"
}

// JunoInternalKeyForLimits returns the key for limits configuration
func JunoInternalKeyForLimits() []byte {
	return []byte("limits")
}

func defaultLimits() (l Limits) {
	l.MaxTimeToLive = uint32(Conf.MaxTimeToLive)
	l.MaxPayloadLength = uint32(Conf.MaxPayloadLength)
	l.MaxKeyLength = uint32(Conf.MaxKeyLength)
	return
}

func defaultJunoInternalLimits() (l Limits) {
	l.MaxTimeToLive = math.MaxUint32
	l.MaxPayloadLength = 1024 * 4
	l.MaxKeyLength = 32 //
	return
}

func defaultLimitsConfig() LimitsConfig {
	return LimitsConfig{
		Limits: defaultLimits(),
		Namespace: map[string]Limits{
			JunoInternalNamespace(): defaultJunoInternalLimits(),
		},
	}
}

func getLimitsConfig() (conf *LimitsConfig) {
	limitsMutex.Lock()
	conf = limitsConfig
	limitsMutex.Unlock()
	return
}

func IsLimitsConfigBefore(tm int64) bool {
	if conf := getLimitsConfig(); conf != nil {
		return conf.Timestamp < tm
	}
	return false
}

func GetCopyOfLimitsConfig() (cconf LimitsConfig) {
	limitsMutex.Lock()
	conf := limitsConfig
	limitsMutex.Unlock()
	if conf == nil {
		dcfg := defaultLimitsConfig()
		conf = &dcfg

	}
	cconf.Copy(conf)
	return
}

func setLimitsConfig(cfg *LimitsConfig) error {
	if cfg != nil {
		limitsMutex.Lock()
		defer limitsMutex.Unlock()
		if limitsConfig == nil || cfg.Timestamp > limitsConfig.Timestamp {
			limitsConfig = &LimitsConfig{}
			limitsConfig.Copy(cfg)
		} else {
			return fmt.Errorf("%d <= %d", cfg.Timestamp, limitsConfig.Timestamp)
		}
	}
	return nil
}

func SetLimitsConfig(storedcfg *cfg.Config) {
	if storedcfg != nil {
		conf := defaultLimitsConfig()

		conf.Merge(storedcfg)
		if err := setLimitsConfig(&conf); err == nil {
			glog.Infof("limits config updated")
		} else {
			glog.Warningf("failed to update limits config. %s", err.Error())
		}
	}
}

// GetLimits returns the limits setting for the given namespace
func GetLimits(namespace []byte) (limits Limits) {
	if cfg := getLimitsConfig(); cfg != nil {
		if l, ok := cfg.Namespace[string(namespace)]; ok {
			limits = l
		} else {
			limits = cfg.Limits
		}
	} else {
		limits = defaultLimits()
	}

	return
}

// Copy deep copy the given LimitsConfig
func (lc *LimitsConfig) Copy(ilc *LimitsConfig) {
	lc.Timestamp = ilc.Timestamp
	lc.Limits = ilc.Limits
	lc.Namespace = make(map[string]Limits)
	for k, v := range ilc.Namespace {
		lc.Namespace[k] = v
	}
}

func (lc *LimitsConfig) Merge(cmap *cfg.Config) {
	if m := cmap.GetValue("Namespace"); m != nil {
		//this block is to set the default values for the Limits memebers not being defined in cmap
		if im, ok := m.(map[string]interface{}); ok {
			for k := range im {
				if _, ok := lc.Namespace[k]; !ok {
					lc.Namespace[k] = lc.Limits
				}
			}
		}
	}
	var merge cfg.Config
	merge.ReadFrom(lc)
	merge.Merge(cmap)
	merge.WriteTo(lc)
}
