#! /bin/bash

: ${PEER_NAMES:=p0}
: ${INITIAL_CLUSTER:=$(hostname -f)}

# generate etcdsvr.txt

echo "etcdsvr.peer_names=$PEER_NAMES
etcdsvr.data_dir=$DATA_DIR
etcdsvr.initial_cluster=$INITIAL_CLUSTER
etcdsvr.client_port=$CLIENT_PORT
etcdsvr.peer_port=$PEER_PORT" > /opt/juno/etcdsvr.txt

exec "$@"

