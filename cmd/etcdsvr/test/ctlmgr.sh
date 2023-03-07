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

printHelp() {
   echo "Usage: ctlmgr.sh <required:operation> [optional:old config or key] [optional:new config]
                <etcd>  --start etcd server
                <g>     --get etcd config
                <d>     --delete all etcd config
                <init>  --load init config      <original config file>
                <dist>  --load redist config    <new config file>
                <nodeshard> --get connect ipport and shard info for a passin hexkey <key>
                <z0-z4> --redist start for zone0 to zone4  <new config file>
		<auto>	--enable redistribution and auto complete till end <new config file>
                <checkfin> --check if all zones are snapshot_finish
		<resume> --resume the aborted redistribution
                <apply> --apply final etcd config <new config>
		<markup> --markup nodes
		<abort> --abort all the redistribution
		<replaceConn> --replace ConnInfo part for old and new config file <newboxip> <one,two..etc> "
}

export ETCDCTL_API=3
RM=/bin/rm
CP=/bin/cp
SED=/bin/sed
ssdir="/x/web/LIVE21/storageserv"
github="/home/wcai/github/juno"
ETCDPrefix=$github/cmd/etcdsvr
testPrefix=$github/test/functest/etcd_test
hostip=`ifconfig | grep -A 5 eth0 | grep "inet" | head -1 | awk '/inet addr/{print substr($2,6)}'`
etcdport=2379

replaceConn() {
   newip=$1
   no=$2
#   $CP ${testPrefix}/connInfo $ssdir
   $SED -i "s/SecondHost/${newip}/g" $ssdir/connInfo  #replace to new ip in connInfo file
   $CP $ssdir/config.toml $ssdir/new_config.toml
  
   deleteNStart=`cat -n $ssdir/new_config.toml | grep -A 10 ClusterInfo | grep -A 1 ConnInfo | tail -1 | awk '{print $1}'`
   deleteNEnd=`expr $deleteNStart + 5`
   echo deleteNStart:$deleteNStart 
   sed -i -e "${deleteNStart},${deleteNEnd}d" ${ssdir}/new_config.toml #remove 5 lines after ConnInfo in config file

   for i in `seq 0 5`; do 
      j=`expr ${i} + 1`
      addNStart=`expr ${deleteNStart} + ${i}`    
      replaceNewConn=`grep -A ${j} ${no}NConnInfo ${ssdir}/connInfo | tail -1` #add connInfo ipport get from connInfo file
      echo addNStart:$addNStart 
      sed -i "${addNStart}i\\${replaceNewConn}" ${ssdir}/new_config.toml
   done
}

if [ -z $1 ] || [ $1 == "-h" ]; then
   printHelp
   exit 0
elif [ $1 != "etcd" ] && [ $1 != "g" ] && [ $1 != "d" ] && [ $1 != "checkfin" ]; then
      config=$2
fi

if [ $1 == "z0" ] || [ $1 == "z1" ] || [ $1 == "z2" ] || [ $1 == "z3" ] || [ $1 == "z4" ] || [ $1 == "auto" ]; then
      markdown=$3
fi

if [ $1 == "etcd" ]; then
   cd $ETCDPrefix; $ETCDPrefix/cmd/etcdsvr > $ETCDPrefix/etcdlog 2>&1 &
fi

if [ $1 == "g" ]; then
   $ETCDPrefix/etcdctl --endpoints=${hostip}:${etcdport} --prefix=true get ""
fi

if [ $1 == "d" ]; then
   $ETCDPrefix/etcdctl --endpoints=${hostip}:${etcdport} --prefix=true del ""
fi

if [ $1 == "nodeshard" ]; then
   key=$2
   keycount=0

   $RM -rf $ssdir/ipport
   echo hexkey $key >> $ssdir/ipport
   chmod 0755 $ssdir/ipport

   shardNo=`grep -i $key $ssdir/*.out | grep shid | tail -1 | awk '{print $9}' | cut -d "," -f6 | cut -d "=" -f2`  #get shard no for give key by grep ss log
   echo "key is $key, shardNo is $shardNo"

   shardNodes=`$ETCDPrefix/etcdctl --endpoints=${hostip}:${etcdport} --prefix=true get "" | grep -B 1 ",$shardNo.*|" | grep node_shards | cut -d "_" -f 4-5`    #get zoneNodes such as 2_0, 3_1, 4_0 for given shards by looking into etcd config
   
   zonenodes=($(echo $shardNodes | tr "\n", "\n"))  #split the zonenodes info so it gets zone_node array
   len=${#zonenodes[@]}
   index=`expr $len - 1`
   for i in `seq 0 $index`; do 
      ipports[i]=`$ETCDPrefix/etcdctl --endpoints=${hostip}:${etcdport} --prefix=true get "" | grep -A 1 node_ipport_${zonenodes[$i]} | tail -1`	#get ipport for given shardNodes
      echo ipports $i is ${ipports[$i]}, shard is $shardNo >> $ssdir/ipport 
   done
fi

if [ $1 == "init" ]; then
   $ETCDPrefix/etcdctl --endpoints=${hostip}:${etcdport} --prefix=true del ""
   $ssdir/clustermgr -config $ssdir/$config -cmd store -type cluster_info 

elif [ $1 == "dist" ]; then
   $ssdir/clustermgr -new_config $ssdir/$config -cmd redist -type prepare

elif [ $1 == "z0" ]; then
   if [ $markdown == "1" ]; then
   	$ssdir/clustermgr -new_config $ssdir/$config  -cmd redist -type start_tgt --zone 0
   	$ssdir/clustermgr -new_config $ssdir/$config  -cmd redist -type start_src --zone 0
   else
        $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist --automarkdown=false -type start_tgt --zone 0
        $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist --automarkdown=false -type start_src --zone 0	
   fi

elif [ $1 == "z1" ]; then
   if [ $markdown == "1" ]; then
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist -type start_tgt --zone 1
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist -type start_src --zone 1 
   else 
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist --automarkdown=false -type start_tgt --zone 1
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist --automarkdown=false -type start_src --zone 1
   fi

elif [ $1 == "z2" ]; then
   if [ $markdown == "1" ]; then
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist -type start_tgt --zone 2
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist -type start_src --zone 2
   else
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist --automarkdown=false -type start_tgt --zone 2
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist --automarkdown=false -type start_src --zone 2
   fi

elif [ $1 == "z3" ]; then
   if [ $markdown == "1" ]; then
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist -type start_tgt --zone 3
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist -type start_src --zone 3
   else
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist --automarkdown=false -type start_tgt --zone 3
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist --automarkdown=false -type start_src --zone 3
   fi

elif [ $1 == "z4" ]; then
   if [ $markdown == "1" ]; then
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist -type start_tgt --zone 4
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist -type start_src --zone 4
   else
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist --automarkdown=false -type start_tgt --zone 4
      $ssdir/clustermgr -new_config $ssdir/$config  -cmd redist --automarkdown=false -type start_src --zone 4
   fi

elif [ $1 == "apply" ]; then
   $ssdir/clustermgr -new_config $ssdir/$config -cmd redist -type commit

elif [ $1 == "markup" ]; then
  $ssdir/clustermgr -new_config $ssdir/$config -cmd zonemarkdown -type delete 

elif [ $1 == "abort" ]; then
  $ssdir/clustermgr -new_config $ssdir/$config -cmd redist -type abort

elif [ $1 == "resume" ]; then
   $ssdir/clustermgr --new_config $ssdir/$config --cmd redist --type resume -zone 0
   $ssdir/clustermgr --new_config $ssdir/$config --cmd redist --type resume -zone 1
   $ssdir/clustermgr --new_config $ssdir/$config --cmd redist --type resume -zone 2
   $ssdir/clustermgr --new_config $ssdir/$config --cmd redist --type resume -zone 3
   $ssdir/clustermgr --new_config $ssdir/$config --cmd redist --type resume -zone 4

elif [ $1 == "checkfin" ]; then
   while true; do 
      number=`$ETCDPrefix/etcdctl --endpoints=${hostip}:${etcdport} --prefix=true get "" | egrep -A 1 'redist_enable|redist_state' | grep -v redist | grep -v ready | grep -v yes_source | grep -v yes_target | grep -v source_resume | grep -v st=F | grep -v '\-\-' | wc -l`  #keep checking if the redistribution finish or not

#      $ETCDPrefix/etcdctl --endpoints=${hostip}:${etcdport} --prefix=true get ""
      echo "$ETCDPrefix/etcdctl --endpoints=${hostip}:${etcdport} --prefix=true get "" | egrep -A 1 'redist_enable|redist_state' | grep -v redist | grep -v ready | grep -v yes_source | grep -v yes_target | grep -v source_resume | grep -v st=F | grep -v '\-\-' | wc -l "
      if [ $number -eq 0 ]; then
         break;
      else
         echo "not all zone finish, continue wait"
         date;
      fi
      usleep 500000
   done

elif [ $1 == "auto" ]; then
   if [ $markdown == "1" ]; then 
	   $ssdir/clustermgr -new_config $ssdir/$config -cmd redist -type auto
   else
	   $ssdir/clustermgr -new_config $ssdir/$config -cmd redist --automarkdown=false -type auto
   fi

elif [ $# -lt 3 ] && [ $1 == "replaceConn" ]; then
   	echo "Usage for replaceConn: ./ctlmgr.sh replaceConn <newhostip> <one etc.>"
	exit 0;
elif [ $# -eq 3 ] && [ $1 == "replaceConn" ]; then
	replaceConn $2 $3;
fi

returnCode=$?
if [ $returnCode == 1 ]; then
   exit 1
fi
