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

set -e
set -x

prefix=/opt/juno

etcdir=$prefix/etc
logdir=$prefix/logs
callogdir=${prefix}/callogs

ncfg_file=$etcdir/ncfg.toml

echo > $ncfg_file

config_args="-o $etcdir/config.toml $etcdir/default.toml"

if [ -f $prefix/config/config.toml ]; then
  config_args+=" $prefix/config/config.toml"
fi

config_args+=" $ncfg_file"

if [ ! -z "$CLUSTER_FLAVOR" ]; then
  echo "ClusterName = \"junoserv-$CLUSTER_FLAVOR\"" >> $ncfg_file
  echo "[Sherlock]" >> $ncfg_file
  echo "SherlockSvc = \"junoserv-$CLUSTER_FLAVOR\"" >> $ncfg_file
  echo "[CAL]" >> $ncfg_file
  echo "Poolname = \"junoserv-$CLUSTER_FLAVOR\"" >> $ncfg_file
  echo "[Sec]" >> $ncfg_file
  echo "AppName = \"junoserv-$CLUSTER_FLAVOR\"" >> $ncfg_file
fi

if [ ! -z "$ETCD_ENDPOINTS" ]; then
  echo "[Etcd]" >> $ncfg_file
  echo   "Endpoints = [$ETCD_ENDPOINTS]" >> $ncfg_file
fi

#Provide option for replication endpoint

for var in "${!REPLICATION_TARGET_@}"; do
    IN="$var"
    replication=(${IN//_/ })
    replication_target_name=${replication[2]}
    replication_target=${!var}
    echo "[[Replication.Targets]]" >> $ncfg_file
    echo " Addr = \"${replication_target}:5080\"" >> $ncfg_file
    echo " Name = \"${replication_target_name}\"" >> $ncfg_file
    echo " SSLEnabled = true" >> $ncfg_file
    echo " BypassLTMEnabled = true" >> $ncfg_file
done


cat $ncfg_file
echo "Generating Juno Proxy config..."
junocli config $config_args

if [ ! -p $logdir/state.log ]; then
  mkdir -p $logdir && mkfifo $logdir/state.log
fi

if [ ! -p ${callogdir}/callog.txt ]; then
  mkdir -p ${callogdir} && mkfifo ${callogdir}/callog.txt
fi

# Generate secrets if not exist
if [ ! -f /opt/juno/bin/secrets/server.pem ] || [ ! -f /opt/juno/bin/secrets/server.crt ] || [ ! -f /opt/juno/bin/secrets/ca.crt ] || [ ! -f /opt/juno/bin/secrets/keystore.toml ]; then
    /opt/juno/bin/secrets/gensecrets.sh
fi

if [ "${1:0:1}" = '-' ]; then
	set -- proxy "$@"
fi

exec "$@" 
