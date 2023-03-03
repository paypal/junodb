#!/bin/sh

service="etcdsvr"

#
# Tools 
#
AWK="awk"
SUDO="/usr/bin/sudo"
USER=`/usr/bin/id -un`
CUT="/usr/bin/cut"
GREP="/bin/grep"
KILL="/bin/kill"
PS="/bin/ps -wo pid,cmd -u $USER"

PIDLIST=`$PS | $GREP "${service}.py$" | $GREP -v grep | $AWK '{print $1}'`
for k in $PIDLIST; do
    $KILL $k
done

sleep 3 

PIDLIST=`$PS | $GREP $service | $GREP -v "grep\|join\|shutdown" | $AWK '{print $1}'`
for k in $PIDLIST; do
        $KILL -9 $k
done
