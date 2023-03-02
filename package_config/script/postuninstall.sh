#!/bin/bash
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
