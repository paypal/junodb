#!/bin/bash

#
# Service variables
#
name=$NAME
group=$GROUP
prefix=$PREFIX
service=$SERVICE

#
# Create log directory
#
if [ ! -d $prefix/$name/state-logs ]; then
        mkdir -p $prefix/$name/state-logs
fi

log() {
  if [ -z "$1" ]; then
    echo ""
  else
    if [ ! -z "$logfile" ]; then
    	echo "$(date +"%m-%d-%Y-%T") $1" >> $logfile
    fi
    echo "$(date +"%m-%d-%Y-%T") $1"
  fi
}

#
# Start the logs
#
FIFO="/usr/local/bin/fifo"
MULTILOG="/usr/local/bin/multilog s5000000 n50"
sub_svc=$1
echo ""
echo "Starting $name $sub_svc log." "["`date`"]"
echo ""
logs=$prefix/$name/logs/
mkdir -p $logs
exec $FIFO $prefix/$name/$sub_svc.log | $MULTILOG $logs
