#!/bin/bash
set -euo pipefail
IFS=$'\n\t'
cd "$(dirname "$0")"
wd=`pwd`
docker compose -f ${wd}/manifest/docker-compose.yaml down -v --remove-orphans