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

#!/bin/bash

#
# Post Install script
#

#
# Command line params
#

name=$1
package=$1
service=$2
prefix=$3
group=$4
export SERVICE=$2

#
# Tools
#
LN="/bin/ln"
MKDIR="/bin/mkdir"
MV="/bin/mv"
RM="/bin/rm"
CP="/bin/cp"
CAT="/bin/cat"
SED="/bin/sed"

INSTALL="/usr/bin/install"
if echo $prefix | grep "\-NON" > /dev/null; then
	short_prefix=`echo $prefix | $SED "s/-NON[^\/]\+//"`
else
	short_prefix=$prefix
fi

stage=`/bin/echo $short_prefix | /usr/bin/cut -d"/" -f4`

base_dir=$short_prefix/$name

#
# Cleanup the Archived Logs older than 30 days
# 
if [ -d "$prefix/$name/logs" ]; then
	echo "Cleanup the Archived Logs older than 30 days" 2>&1 | tee -a $log_file
	find "$prefix/$name/logs" -name "$name*" -type d -mtime +30 -exec rm -rf {} \; 2>/dev/null
fi

#User Check
CURRENT_USER=`/usr/bin/id -un`

$INSTALL -m 755 -o $CURRENT_USER -g $group $prefix/$name/config-$name.txt $prefix/$name/etcdsvr.txt
$RM -f $prefix/$name/config-$name.toml
$RM -f $prefix/$name/config-$name.txt

stageip=`ip -o route get to 8.8.8.8 | sed -n 's/.*src \([0-9.]\+\).*/\1/p'`

#
# Change files with name and prefix
#
cd $prefix/$name
for FILE in start.sh stop.sh shutdown.sh \
    log.sh logstate.sh config.toml etcdsvr.txt \
    preinstall.sh preuninstall.sh ; do
  #
  if [ ! -f $prefix/$name/$FILE ]; then
    continue
  fi
  $CAT $prefix/$name/$FILE       | \
    $SED s,\$NAME,$name,g          | \
    $SED s,\$SERVICE,$service,g    | \
    $SED s,\$GROUP,$group,g        | \
    $SED s,\$STAGEIP,$stageip,g        | \
    $SED s,\$STAGE,$stage,g        | \
    $SED s,\$PREFIX,$short_prefix,g      > \
      $prefix/$name/$FILE.temp
  $CP $prefix/$name/$FILE.temp $prefix/$name/$FILE
  $RM $prefix/$name/$FILE.temp
  #
done

echo " "
echo "====================================================================="
echo "postInstall.sh starting at $(date)"

#Print Base Paremeters
echo "package: $name"
echo "group: $group"
echo "base_dir: $base_dir"
echo "stage: $stage"

if [ -f $prefix/$name/shutdown.sh ]; then
        $prefix/$name/shutdown.sh
fi

# Start up the server
#
if [ "x$NO_AUTOSTART" == "x" -a -x $prefix/$name/start.sh ]; then
	# etcd specific
    # if [ ! -f $prefix/$name/$service ]; then
    	#echo "move etcdsvr to $service to start it" 
    	#$MV $prefix/$name/etcdsvr $prefix/$name/$service
    # fi
	$RM -f $prefix/$name/etcdsvr
#	exec $prefix/$name/start.sh
	$prefix/$name/start.sh
else
	echo "Service not started."
fi

echo "postInstall.sh completed at $(date)"
