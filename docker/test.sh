#!/bin/bash
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
 
set -euo pipefail
IFS=$'\n\t'
cd "$(dirname "$0")"
if [ $# -gt 0 ] 
then
  echo "***********************************************************"
  echo "This script will run test tools in junoclient containers"
  echo "********************==========*****************************"
  echo "Note: All options are set thru env variables/config files"
  echo "Refer manifest/docker-compose.yaml for container def"
  echo "Refer manifest/.env for environment variables"
  echo "Refer  manifest/client/config.toml for cleint config.toml"
  echo "1. VERSION - Default version is 'latest'"
  echo "2. TZ - Default time zone is 'America/Los_Angeles'"
  echo "********************==========*****************************"
  exit
fi

wd=`pwd`

tls_port=5080
tcp_port=8080
proxy="proxy"
junocli="/opt/juno/junocli"
junoload="/opt/juno/junoload"
cfg="/opt/juno/config.toml"

echo "***********************************************************"
echo "    Testing client tools in junoclient container           "

echo "***********************************************************"
echo "Run junoload against ssl endpoint ${proxy}:${tls_port}"
docker exec junoclient sh -c "${junoload} -s ${proxy}:${tls_port} -o 1 -ssl -c ${cfg} -t 3" ;
echo "********************==========*****************************"

echo "***********************************************************"
echo "Run junoload against tcp endpoint ${proxy}:${tcp_port}"
docker exec junoclient sh -c "${junoload} -s ${proxy}:${tcp_port} -o 1 -t 3" ;
echo "********************==========*****************************"

echo "***********************************************************"
echo "Run junocli against ssl endpoint ${proxy}:${tls_port}"
echo "Set key/value k1 v1"
docker exec junoclient sh -c "${junocli} set -s ${proxy}:${tls_port}  -ns testns1 -ssl -c ${cfg} k1 v1"
sleep 1
echo "Get key k1"
docker exec junoclient sh -c "${junocli} get -s ${proxy}:${tls_port}  -ns testns1 -ssl -c ${cfg} k1"
echo "Delete key k1"
docker exec junoclient sh -c "${junocli} destroy -s ${proxy}:${tls_port}  -ns testns1 -ssl -c ${cfg} k1"
sleep 1
echo "Get key k1"
docker exec junoclient sh -c "${junocli} get -s ${proxy}:${tls_port}  -ns testns1 -ssl -c ${cfg} k1"
echo "********************==========*****************************"


echo "***********************************************************"
echo "Run junocli against tcp endpoint ${proxy}:${tcp_port}"
echo "Set key/value k2 v2"
docker exec junoclient sh -c "${junocli} set -s ${proxy}:${tcp_port}  -ns testns2 -c ${cfg} k2 v2"
sleep 1
echo "Get key k2"
docker exec junoclient sh -c "${junocli} get -s ${proxy}:${tcp_port}  -ns testns2 -c ${cfg} k2"
echo "Delete key k2"
docker exec junoclient sh -c "${junocli} destroy -s ${proxy}:${tcp_port}  -ns testns2 -c ${cfg} k2"
sleep 1
echo "Get key k2"
docker exec junoclient sh -c "${junocli} get -s ${proxy}:${tcp_port}  -ns testns2 -c ${cfg} k2"
echo "********************==========*****************************"