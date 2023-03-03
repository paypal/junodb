#!/bin/bash

BASE=$(dirname "$0")
FILE=junocfg.log

cd ${BASE}

echo "Start running clustermgr ..."
echo 

find $FILE -mtime +2 -type f -delete

echo >> $FILE
echo "# ./clustermgr $@" >> $FILE
echo >> $FILE

./clustermgr -cmd=swaphost 2>&1 | tee -a $FILE
