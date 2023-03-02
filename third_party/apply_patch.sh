#! /bin/bash
set -euo pipefail
IFS=$'\n\t'
cd "$(dirname "$0")"
wd=`pwd`
if [ ! -d snappy ]; then 
  # git clone google/snappy
  git clone --branch 1.1.5 https://github.com/google/snappy.git 
  # copy patched file
  cp -r patches/snappy/* snappy/
  # remove .git directory
  rm -rf snappy/.git
fi

if [ ! -d rocksdb ]; then
  # git clone rocksdb
  git clone --branch v5.5.1 https://github.com/facebook/rocksdb.git 
  # copy patched files
  cp -r patches/rocksdb/* rocksdb/
  # remove .git directory
  rm -rf rocksdb/.git
fi

if [ ! -d forked/tecbot/gorocksdb ]; then
  # git clone gorockdb
  git clone https://github.com/tecbot/gorocksdb.git forked/tecbot/gorocksdb
  cd forked/tecbot/gorocksdb && git checkout 57a309fefefb9c03d6dcc11a0e5705fc4711b46d && cd ${wd}
  # copy patched files
  cp -r patches/forked/tecbot/gorocksdb/* forked/tecbot/gorocksdb/
  # remove .git directory
  rm -rf forked/tecbot/.git
fi