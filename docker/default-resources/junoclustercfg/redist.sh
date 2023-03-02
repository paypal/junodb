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
#  Package utility provides the utility interfaces for mux package
#  
#! /bin/bash

BASE=$(dirname "$0")

cd ${BASE}

echo "Start running clustermgr ..."
echo

group=juno

PIDLIST=`ps -wo pid,cmd -u $group | grep clustermgr | grep -v grep | awk '{print $1}'`
for PROCESS in $PIDLIST; do
    kill -9 $PROCESS
done

./clustermgr -cmd=redist "$@" > log 2>&1

