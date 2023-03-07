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
