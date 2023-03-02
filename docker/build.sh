#!/bin/bash
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

# Build juno
make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} GoLangVersion=${GoLangVersion} build

# Build Docker images
make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} build_junoclusterserv
make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} build_junoclustercfg
make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} build_junoserv
make image_tag=${image_tag} docker_repo=${docker_repo} source_repo=${source_repo} source_branch=${source_branch} build_junostorageserv


# Set the app version using image_tag in manifest/.env
sed -i "s#.*VERSION=.*#VERSION=${image_tag}#g" ${wd}/manifest/.env

# Generate the test secrets to initialize proxy
manifest/config/secrets/gensecrets.sh

