#!/bin/sh

service="etcdsvr"
log_file="${PWD}/etcdsvr.log"

#
# Tools 
#
AWK="awk"
SUDO="/usr/bin/sudo"
USER=`/usr/bin/id -un`
CUT="/usr/bin/cut"
GREP="/bin/grep"
KILL="/bin/kill"
MV="/bin/mv"
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

if [ -e $log_file ]; then
   $MV $log_file $log_file.1
fi
echo > $log_file
"${PWD}/${service}.py" $1 2> $log_file &
