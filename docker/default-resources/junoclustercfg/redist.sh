#! /bin/bash

BASE=$(dirname "$0")

cd ${BASE}

echo "Start running clustermgr ..."
echo

group=juno

PIDLIST=`ps -wo pid,cmd -u $group | grep clustermgr | grep -v grep | awk '{print $1}'`
for PROCESS in $PIDLIST; do
    kill -9 $PROCESS
done

./clustermgr -cmd=redist "$@" > log 2>&1

