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
######################################################################################################
# This script will build juno services - junoclusterserv, junoclustercfg, junoserv , junostorageserv
# Then it will create docker compose with proper setup 
# Start and Stop scripts will help in starting all of them as one environment
######################################################################################################

: ${image_tag:=latest}
: ${source_branch:=main}
: ${docker_registry:=registry.hub.docker.com}
: ${docker_repo:=juno}
: ${source_repo:=git@github.com:paypal/juno.git}
: ${GoLangVersion:=1.18.2}

wd=`pwd`

# Fetch junosrc
make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} copysrc

if [ -z ${ACTIONS_CACHE_URL+x} ]; then
  # Build juno normally
  make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} GoLangVersion=${GoLangVersion} build
else
  # Build juno using buildx (uses gha caching)
  make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} GoLangVersion=${GoLangVersion} buildx
fi

# Build Docker images
make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} build_junoclusterserv
make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} build_junoclustercfg
make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} build_junoserv
make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} build_junostorageserv
make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} build_junoclient


# Set the app version using image_tag in manifest/.env
sed -i "s#.*VERSION=.*#VERSION=${image_tag}#g" ${wd}/manifest/.env

# Generate the test secrets to initialize proxy
manifest/config/secrets/gensecrets.sh

