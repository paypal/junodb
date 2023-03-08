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
  echo "This script will run juno docker containers"
  echo "********************==========*****************************"
  echo "Note: All options are set thru env variables/config files"
  echo "Refer manifest/docker-compose.yaml"
  echo "Refer manifest/.env for environment variables"
  echo "1. VERSION - Default version is 'latest'"
  echo "2. TZ - Default time zone is 'America/Los_Angeles'"
  echo "********************==========*****************************"
  exit
fi

wd=`pwd`
echo "Starting Juno Services..."
cd ${wd}/manifest && docker compose -f ${wd}/manifest/docker-compose.yaml up --detach && cd ${wd}
