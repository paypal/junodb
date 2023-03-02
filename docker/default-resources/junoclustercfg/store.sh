#! /bin/bash

BASE=$(dirname "$0")

cd ${BASE}

echo "Start running clustermgr ..."
echo

./clustermgr -cmd=store "$@" > log 2>&1

