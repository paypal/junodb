#! /bin/bash

set -e

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
fi

if [ ! -z "$ETCD_ENDPOINTS" ]; then
  echo "[Etcd]" >> $ncfg_file
  echo   "Endpoints = [$ETCD_ENDPOINTS]" >> $ncfg_file
fi

cat $ncfg_file

junocli config $config_args

if [ ! -p $logdir/state.log ]; then
  mkdir -p $logdir && mkfifo $logdir/state.log
fi

if [ ! -p ${callogdir}/callog.txt ]; then
  mkdir -p ${callogdir} && mkfifo ${callogdir}/callog.txt
fi

if [ "${1:0:1}" = '-' ]; then
	set -- storageserv "$@"
fi

exec "$@"
