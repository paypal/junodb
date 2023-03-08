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
stage=$STAGE

# Check to see if state.log already exists
if [ -e $prefix/$name/state.log ]; then
    # Check to see if it is a fifo process
    state_log_pid=$(ps -wo pid,cmd -u $group | egrep "$prefix/$name/[s]tate\.log")
    if [ -z "$state_log_pid" ]; then
        # It is not a process
        rm $prefix/$name/state.log
    else
        # try to kill the fifo process
        kill $(echo $state_log_pid | cut -d'/' -f1)
    fi
fi

#
# Create log directory
#
if [ ! -d $prefix/$name/state-logs ]; then
        mkdir -p $prefix/$name/state-logs
fi

#
# Start the logs
#
FIFO="/usr/local/bin/fifo"
MULTILOG="/usr/local/bin/multilog s11867040 n60"

trap '' SIGHUP

echo ""
echo "Starting $name $service state log." "["`date`"]"
echo ""
statelogs=$prefix/$name/state-logs
mkdir -p $statelogs
exec $FIFO $prefix/$name/state.log | $MULTILOG $statelogs
