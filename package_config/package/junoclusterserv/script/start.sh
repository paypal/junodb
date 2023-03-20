#!/bin/bash
#  
#  Copyright 2023 PayPal Inc.
#  
#  Licensed to the Apache Software Foundation (ASF) under one or more
#  contributor license agreements.  See the NOTICE file distributed with
#  this work for additional information regarding copyright ownership.
#  The ASF licenses this file to You under the Apache License, Version 2.0
#  (the "License"); you may not use this file except in compliance with
#  the License.  You may obtain a copy of the License at
#  
#     http://www.apache.org/licenses/LICENSE-2.0
#  
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#  


# Start.sh for Juno Proxy
#
# Service variables
#
name=$NAME
group=$GROUP
prefix=$PREFIX
service=$SERVICE
stage=$STAGE


baseprefix=`echo $prefix | sed -e 's/\([^-]*\(-WACK\)\?\).*/\1/'`

#
# Tools and state
#
AWK="/bin/awk"
SUDO="/usr/bin/sudo"
CURRENT_USER=`/usr/bin/id -un`

#stage2 is a master stage, so it should not run under faketime and it's determined in the following
# /x/web/$stage/ppfaketime/ft-setup.sh
if [ -e "/x/web/$stage/ppfaketime/ft-setup.sh" ]; then . "/x/web/$stage/ppfaketime/ft-setup.sh"; fi


#LANG=en_US
LANG=UTF-8
export LANG


#
# Print out debug info
#
echo PATH is $PATH
echo LD_LIBRARY_PATH is $LD_LIBRARY_PATH


start_log() {
    svc=$1
    $prefix/$name/log.sh $svc &
    COUNT=1
    while [ ! -p $prefix/$name/$svc.log ]
    do
      #echo "sleeping .1 second waiting for pipe $prefix/$name/$svc.log"
      sleep 1
      if [ "$COUNT" = "200" ]; then
          echo "Error: pipe not created after 20 seconds: $prefix/$name/$svc.log"
          exit -1
      fi
      COUNT=`expr $COUNT + 1`
    done
}

check_port_in_use() {
    port=$1
    max=$2
    let j=0
    if [ $max -lt 1 ]; then
         if [ `netstat -lnt 2>/dev/null | grep ":$port .*:" | wc -l` = 0 ];  then
             return 0
         else
             return 1
         fi
    else
         while [ `netstat -lnt 2>/dev/null | grep ":$port .*:" | wc -l` = 0 ] \
               && [ $j -lt $max ]; do
             sleep 1
             let j=$j+1
         done
         if [ $j -ge $max ]; then
             return 0
         else
             return 1
         fi
    fi
}

#shutdown the service 
#
#
sleep 1
#

if [ ! -f /$prefix/$name/disable ]; then
    # Start service
    echo ""
    echo "Starting up $name $service." "["`date`"]"
    echo ""
    export CAL_CONFIG=$prefix/$name/
    cd $prefix/$name
    if [ -f $(which mkfifo) ] && [ -f $(which multilog) ]; then
    	start_log $service
    else
    	echo "Cannot start $service. Multilog or Fifo not available."
    	exit 1
    fi
    
    SVC_START_CMD="$prefix/$name/${service}.py"
    $SVC_START_CMD $1 2> $prefix/$name/$service.log &
    pid=$!
	sleep 4
    if [ ! -d /proc/$pid ]; then
        echo ERROR: $service failed to start
        exit 1
    fi
else
    # Do not Start Service
	    echo ""
	    echo " Service - $service should not run at this location"
	    echo "Exiting"
	    exit
fi


