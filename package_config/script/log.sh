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
