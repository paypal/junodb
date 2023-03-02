#! /bin/bash

BASE=$(dirname "$0")

cd ${BASE}

./clustermgr -cmd=status 2> /dev/null | grep "version="

