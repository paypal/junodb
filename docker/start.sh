#!/bin/bash
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
