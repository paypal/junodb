#  
#  Copyright 2023 PayPal Inc.
#  
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#  
#     http://www.apache.org/licenses/LICENSE-2.0
#  
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#  
#  Package utility provides the utility interfaces for mux package
#  
#! /bin/bash

pkg_dir=${1:-$BUILDTOP/package_config/package/junoclusterserv}
pkg_basename=$(basename $pkg_dir)

if [[ "$JUNO_BUILD_DIR" == "" ]]; then
  echo "JUNO_BUILD_DIR required but not defined"
  exit
fi

if [[ ! -d "$JUNO_BUILD_DIR" ]]; then
  echo "$JUNO_BUILD_DIR not exist"
  exit
fi


function generate_config() {
  local config_chain

  cfg=$pkg_dir/config/config.toml
  if [[ -f "$cfg" ]]; then
      config_chain+=" $cfg"
  fi

  #echo $config_chain
  local conf_file

  conf_file=config-$pkg_basename.txt 
  $JUNO_BUILD_DIR/junocli config -o $pkg_dir/$conf_file -f text $config_chain
}

generate_config
