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
  echo "[Sec]" >> $ncfg_file
  echo "AppName = \"junoserv-$CLUSTER_FLAVOR\"" >> $ncfg_file
  # Provide option for Hashicorp Vault Security
  if [ ! -z "$HASHICORP_VAULT_ENDPOINT" ]; then
    echo "IsVaultServerSecurity = true" >> $ncfg_file
    echo "VaultServerAddress = \"$HASHICORP_VAULT_ENDPOINT\"" >> $ncfg_file
  fi
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

if [ "${1:0:1}" = '-' ]; then
	set -- proxy "$@"
fi

exec "$@" 
