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
 

###############################################################
### BUILDTOP is github root folder, JUNO_BUILD_DIR is    ###### 
### binary code folder. Sample: BUILDTOP=~/github/juno   ######
###############################################################
cd "$(dirname "$0")"
if [ "$BUILDTOP" == "" ]; then
  echo "JUNO_BUILD_DIR and BUILDTOP required but not defined"
  exit
elif [ "$JUNO_BUILD_DIR" == "" ]; then
  export JUNO_BUILD_DIR=$BUILDTOP/release-binary/code-build
fi

group=`/usr/bin/id -gn`
TAR='/bin/tar xvzf'
RM='/bin/rm -rf'
GO_VERSION=1.18.2

if [ ! -d deploy ]; then
  mkdir -p deploy
fi
cd deploy

# copy onfig, shutdown/start, binary etc. sript or files to each package folder
for i in junoclustercfg junoclusterserv junoserv junostorageserv;
do
  if [ ! -d $i ]; then  #create package folder
    mkdir -p $i
  elif [ -f ${i}/shutdown.sh ]; then  #stop service if it is up
    $i/shutdown.sh
  fi 

  if [ $i != "junoclustercfg" ]; then
    cp $BUILDTOP/package_config/package/${i}/script/shutdown.sh $i
    cp $BUILDTOP/package_config/package/${i}/script/start.sh $i
  fi
  
  cp $BUILDTOP/package_config/package/${i}/script/postinstall.sh $i
  cp $BUILDTOP/package_config/script/postuninstall.sh $i
  cp $BUILDTOP/package_config/script/logstate.sh $i
  cp $BUILDTOP/package_config/script/log.sh $i

  $BUILDTOP/package_config/package/${i}/build.sh
  cp $BUILDTOP/package_config/package/${i}/config-${i}* $i
  $RM $BUILDTOP/package_config/package/${i}/config-${i}*
done

cp $JUNO_BUILD_DIR/etcdsvr.py	junoclusterserv
cp $JUNO_BUILD_DIR/etcdctl	junoclusterserv
cp $JUNO_BUILD_DIR/etcdsvr_exe	junoclusterserv
cp $JUNO_BUILD_DIR/join.sh	junoclusterserv
cp $JUNO_BUILD_DIR/status.sh	junoclusterserv
cp $JUNO_BUILD_DIR/tool.py	junoclusterserv

cp $JUNO_BUILD_DIR/clustermgr	junoclustercfg
cp $JUNO_BUILD_DIR/junocfg	junoclustercfg

cp $JUNO_BUILD_DIR/storageserv  junostorageserv
cp $JUNO_BUILD_DIR/dbcopy       junostorageserv
cp $JUNO_BUILD_DIR/dbscanserv   junostorageserv

cp -r $BUILDTOP/package_config/package/junoserv/secrets/	junoserv
cp $JUNO_BUILD_DIR/proxy	junoserv

####### install all four packages, start up junostorageserv/junoserv service ########
prefix=$BUILDTOP/script/deploy
junoclusterserv/postinstall.sh junoclusterserv etcdsvr $prefix $group
junoclustercfg/postinstall.sh junoclustercfg junoclustercfg $prefix $group
junostorageserv/postinstall.sh junostorageserv storageserv $prefix $group
junoserv/postinstall.sh junoserv proxy $prefix $group

cd $BUILDTOP/script  #get out of deploy folder, into script folder to create test link

### create soft link to test folder #####
if [ ! -d test ]; then
   ln -s $BUILDTOP/test test
fi
