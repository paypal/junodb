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

// package cfg implements functionalites for configuration
package cfg

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/BurntSushi/toml"

	"juno/third_party/forked/golang/glog"
)

type (
	// Config is introduced for primarily solving some problems when handling TOML configuration files.

	// Note: It is not thread/goroutine safe, and is intended to be used in places where
	// it is goroutine/thread safety
	Config struct {
		kvMap map[string]keyValue
	}
	keyValue struct {
		key   string
		value interface{}
	}
)

// ReadFrom reads configuration properties i, which pointing to a struct or a map
func (c *Config) ReadFrom(i interface{}) (err error) {
	var buf bytes.Buffer
	if i != nil {
		env := toml.NewEncoder(&buf)
		if err := env.Encode(i); err != nil {
			return err
		}
	}
	return c.ReadFromToml(&buf)
}

// ReadFromToml reads configuration properties in TOML format
func (c *Config) ReadFromToml(r io.Reader) (err error) {
	m := make(map[string]interface{})
	if _, err = toml.DecodeReader(r, &m); err == nil {
		c.setFrom(m)
	}
	return
}

// ReadFromTomlBytes reads configuration properties from byte array in TOML format
func (c *Config) ReadFromTomlBytes(b []byte) (err error) {
	var buf bytes.Buffer
	_, err = buf.Write(b)
	if err != nil {
		return
	}
	return c.ReadFromToml(&buf)
}

// ReadFromTomlFile reads configuration properties from a file in TOML format
func (c *Config) ReadFromTomlFile(file string) (err error) {
	m := make(map[string]interface{})
	if _, err = toml.DecodeFile(file, &m); err == nil {
		c.setFrom(m)
	}
	return
}

// WriteToToml writes the configuration properties in TOML format
func (c *Config) WriteToToml(w io.Writer) (err error) {
	encoder := toml.NewEncoder(w)
	m := make(map[string]interface{})
	setMap(m, c.kvMap)
	encoder.Encode(m)
	return nil
}

// WriteTo writes the configuration properties to a struct or map
func (c *Config) WriteTo(v interface{}) (err error) {
	var buf bytes.Buffer
	c.WriteToToml(&buf)
	_, err = toml.Decode(buf.String(), v)
	return
}

// Merge merges the properties from another Config
// When merging the properties, the keys are considered as case insensitive.
// The value for the same key will be overriden
func (c *Config) Merge(overrides *Config) error {
	if c.kvMap == nil {
		c.kvMap = make(map[string]keyValue)
	}
	return merge(c.kvMap, overrides.kvMap)
}

// WriteToKVList writes the configuration properties as a dot-delimited-key value list
func (c *Config) WriteToKVList(w io.Writer) {
	for _, v := range c.kvMap {
		writeKeyValue(w, v.key, &v)
	}
}

// GetValue returns the assiciated value of the given dot-delimited key
func (c *Config) GetValue(dotDelimitedKey string) interface{} {
	strs := strings.Split(dotDelimitedKey, ".")
	return getValueFromMap(c.kvMap, strs)
}

// GetConfig returns the config properties of a given key
func (c *Config) GetConfig(dotDelimitedKey string) (conf Config, err error) {
	if i := c.GetValue(dotDelimitedKey); i != nil {
		if vm, ok := i.(map[string]keyValue); ok {
			m := make(map[string]interface{})
			setMap(m, vm)
			err = conf.ReadFrom(&m)
		} else {
			err = conf.ReadFrom(i)
		}
	}
	return
}

// SetKeyValue sets the value of a given key
func (c *Config) SetKeyValue(dotDelimitedKey string, v interface{}) {
	strs := strings.Split(dotDelimitedKey, ".")
	nKeys := len(strs)
	if nKeys == 0 {
		return
	}
	tmap := make(map[string]keyValue)
	cm := tmap

	key := strings.ToLower(strs[0])
	for len(strs) > 1 {
		nmap := make(map[string]keyValue)
		cm[key] = keyValue{strs[0], nmap}
		cm = nmap
		strs = strs[1:]
	}

	cm[key] = keyValue{strs[0], v}
	if c.kvMap == nil {
		c.kvMap = make(map[string]keyValue)
	}
	merge(c.kvMap, tmap)
}

func writeKeyValue(w io.Writer, k string, v *keyValue) {
	if vm, ok := v.value.(map[string]keyValue); ok {
		for _, sv := range vm {
			writeKeyValue(w, k+"."+sv.key, &sv)
		}
	} else {
		fmt.Fprintf(w, "%s=%v\n", k, v.value)
	}
}

func (c *Config) setFrom(m map[string]interface{}) {
	c.kvMap = make(map[string]keyValue)
	setKvMap(c.kvMap, m)
}

func merge(to, from map[string]keyValue) error {
	for k, v := range from {
		vm, vismap := v.value.(map[string]keyValue)

		if toV, found := to[k]; !found {
			if vismap {
				nmap := make(map[string]keyValue)
				to[k] = keyValue{v.key, nmap}
				merge(nmap, vm)
			} else {
				to[k] = v
			}
		} else {
			toMap, toIsMap := toV.value.(map[string]keyValue)
			if toIsMap && vismap {
				merge(toMap, vm)
			} else {
				tto := reflect.TypeOf(toV)
				tfrom := reflect.TypeOf(v)
				if tto == tfrom {
					to[k] = v
				} else {
					return fmt.Errorf("type mismatch. target: %v  source: %v", tto, tfrom)
				}
			}
		}
	}
	return nil
}

func getValueFromMap(imap map[string]keyValue, keys []string) interface{} {

	nKeys := len(keys)
	if nKeys > 0 {
		key := strings.ToLower(keys[0])
		if v, ok := imap[key]; ok {
			if nKeys == 1 {
				if vm, ok := v.value.(map[string]keyValue); ok {
					nmap := make(map[string]interface{})
					setMap(nmap, vm)
					return nmap
				} else {
					return v.value
				}
			} else {
				if vm, ok := v.value.(map[string]keyValue); ok {
					return getValueFromMap(vm, keys[1:])
				} else {
					return nil
				}
			}
		} else {
			return nil
		}
	}
	return nil
}

func setKvMap(to map[string]keyValue, from map[string]interface{}) {
	if to == nil || from == nil {
		return
	}
	for k, v := range from {
		lkey := strings.ToLower(k)
		if _, found := to[lkey]; found {
			glog.Warningf("key: %s found, skip", k)
		} else {
			if vm, ok := v.(map[string]interface{}); ok {
				kvmap := make(map[string]keyValue)
				to[lkey] = keyValue{key: k, value: kvmap}
				setKvMap(kvmap, vm)
			} else {
				to[lkey] = keyValue{k, v}
			}
		}
	}
}

func setMap(to map[string]interface{}, from map[string]keyValue) {
	if to == nil || from == nil {
		return
	}
	for _, v := range from {
		if _, found := to[v.key]; found {
			glog.Warningf("key: %s found, skip", v.key)
		} else {
			if vm, ok := v.value.(map[string]keyValue); ok {
				nmap := make(map[string]interface{})
				to[v.key] = nmap
				setMap(nmap, vm)
			} else {
				to[v.key] = v.value
			}
		}
	}
}
