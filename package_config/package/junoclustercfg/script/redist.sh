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

ARGS=""

for i in "$@"
do
case $i in
	-type=*)
	ARGS="${ARGS} $i"
	shift
	;;
	-zone=*)
	ARGS="${ARGS} $i"
	shift
	;;
	-max_failures=*)
	ARGS="${ARGS} $i"
	shift
	;;
	-min_wait=*)
	ARGS="${ARGS} $i"
	shift
	;;
	-ratelimit=*)
	ARGS="${ARGS} $i"
	shift
	;;
esac
done

./clustermgr -cmd=redist ${ARGS} 2>&1 | tee -a $FILE
