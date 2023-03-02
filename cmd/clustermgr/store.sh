#!/bin/sh

BASE=$(dirname "$0")
FILE=junocfg.log

cd ${BASE}

echo "Start running clustermgr ..."
echo

find $FILE -mtime +2 -type f -delete

echo >> $FILE
echo "# ./store.sh" >> $FILE
echo >> $FILE

./clustermgr -cmd=store 2>&1 | tee -a $FILE
