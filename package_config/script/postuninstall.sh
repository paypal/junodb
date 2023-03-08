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
CAT="/bin/cat"
GREP="/bin/grep"
KILL="/bin/kill"
CUT="/usr/bin/cut"
PS="/bin/ps -wo pid,cmd -u $group"
SUDO="/usr/bin/sudo"
CURRENT_USER=`/usr/bin/id -un`
RM="/bin/rm"
#
# Run as special user
#
if [ $CURRENT_USER != $group ]; then
	$SUDO -u $group $0 $@
	exit 0
fi

#
# Post UnInstall script
#
base_dir=$prefix/$name

$RM -rf $base_dir/config*
$RM -rf $base_dir/*etcd*
