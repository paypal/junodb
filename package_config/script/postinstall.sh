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

LN="/bin/ln"
MKDIR="/bin/mkdir"
MV="/bin/mv"
RM="/bin/rm"
INSTALL="/usr/bin/install"

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
buildId=$5
releaseId=$6
export SERVICE=$2

if echo $prefix | grep "\-NON" > /dev/null; then
	short_prefix=`echo $prefix | sed "s/-NON[^\/]\+//"`
else
	short_prefix=$prefix
fi

#stage=`/bin/echo $prefix | /usr/bin/cut -d"/" -f4`
stage=`/bin/echo $short_prefix | /usr/bin/cut -d"/" -f4`

base_dir=$short_prefix/$name

#
# Cleanup the Archived Logs older than 30 days
# 
if [ -d "/x/web/logs/$name" ]; then
	echo "Cleanup the Archived Logs older than 30 days" 2>&1 | tee -a $log_file
	find "/x/web/logs/$name" -name "$name*" -type d -mtime +30 -exec rm -rf {} \; 2>/dev/null
fi

#
# Tools
#
CP="/bin/cp"
RM="/bin/rm"
CAT="/bin/cat"
SED="/bin/sed"

#User Check
CURRENT_USER=`/usr/bin/id -un`

#
# Override the group for playground installs
#
case $stage in
	LIVE*|STAGE*|SANDBOX*|CSZ*|MS* )
		group=$4
		;;
	* )
		group=$CURRENT_USER
		;;
esac

case $stage in 
	STAGE* )
		stage=STAGE
		;;
	* )
	;;
esac

# Delete etcd cache file
$RM -rf $prefix/$name/etcd_cache

echo $INSTALL -m 755 -o $group -g $group $prefix/$name/config-$name.toml $prefix/$name/config.toml
$INSTALL -m 755 -o $group -g $group $prefix/$name/config-$name.toml $prefix/$name/config.toml
$RM -rf $prefix/$name/config-$name.toml
echo $RM -rf $prefix/$name/config-$name.toml
#
# Change files with name and prefix
#
for FILE in db.sh ics.sh run.sh run-console.sh start.sh stop.sh shutdown.sh \
            all.sql all_adm.sql all_conf.sql cycle.sh log.sh logstate.sh logssconn.sh \
	    preinstall.sh preuninstall.sh cycleall.sh shutdownall.sh startall.sh ; do
	#
	if [ ! -f $prefix/$name/$FILE ]; then
		continue
	fi
	$CAT $prefix/$name/$FILE       | \
	$SED s,\$NAME,$name,g          | \
	$SED s,\$SERVICE,$service,g    | \
	$SED s,\$GROUP,$group,g        | \
	$SED s,\$STAGE,$stage,g        | \
	$SED s,\$PREFIX,$short_prefix,g      > \
	$prefix/$name/$FILE.temp
	$CP $prefix/$name/$FILE.temp $prefix/$name/$FILE
	$RM $prefix/$name/$FILE.temp
	#
done


echo " "
echo "======================================================================================"
echo "postInstall.sh starting at $(date)"

#Print Base Paremeters
echo "package: $name"
echo "group: $group"
echo "base_dir: $base_dir"
echo "stage: $stage"
echo "build_id: $buildId"
echo "release_version: $releaseId"

if [ -f $prefix/$name/shutdown.sh ]; then
	$prefix/$name/shutdown.sh
fi

ClusterName=""
#
# Change files with name and prefix
#
for FILE in config.toml etcdsvr.txt; do
	#
	if [ ! -f $prefix/$name/$FILE ]; then
		continue
	fi
	$CAT $prefix/$name/$FILE       | \
	$SED s,\$NAME,$name,g          | \
	$SED s,\$SERVICE,$service,g    | \
	$SED s,\$GROUP,$group,g        | \
	$SED s,\$CLUSTERNAME,$ClusterName,g        | \
	$SED s,\$STAGE,$stage,g        | \
	$SED s,\$PREFIX,$short_prefix,g      > \
	$prefix/$name/$FILE.temp
	$CP $prefix/$name/$FILE.temp $prefix/$name/$FILE
	$RM $prefix/$name/$FILE.temp
	#
done
	
#
# Start up the server
#
if [ "x$NO_AUTOSTART" == "x" -a -x $prefix/$name/start.sh ]; then
#	exec $prefix/$name/start.sh
	$prefix/$name/start.sh
else
	echo "Service not started."
fi

echo "postInstall.sh completed at $(date)"
