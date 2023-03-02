#! /bin/bash

group=juno

PIDLIST=`ps -wo pid,cmd -u $group | grep clustermgr | grep -v grep | awk '{print $1}'`
for PROCESS in $PIDLIST; do
    kill -9 $PROCESS
done

echo "Shutdown completed"
