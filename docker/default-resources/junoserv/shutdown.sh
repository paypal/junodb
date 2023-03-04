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
#!/bin/sh
#
# Service variables
#
group=juno
prefix=$(pwd)
service=proxy

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

stop_service() {
    svc=$1
    echo ""
    echo "Shutting down $svc." "["`date`"]"
    echo ""
    if [ -f $prefix/$svc.pid ]; then
        pid=`$CAT $prefix/$svc.pid`
        if [ -d /proc ]; then
            # we have the /proc filesystem. use it to figure out when the process dies.
            if [ -d /proc/$pid ]; then
                $KILL $pid
                tstart=$SECONDS
                tcount=0
                # NOTE: race condition exists here; what if a new process is created with the same pid?
                while [ -d /proc/$pid ]; do
                    sleep 1
                    # break out if it's been more than 10 seconds or 100 loop iterations
                    # the loop iterations count is to handle clock jumps (e.g. on a time machine stage)
                    tcount=$((tcount + 1))
                    if [ $(($SECONDS - $tstart)) -ge 10 -o $tcount -ge 100 ]; then
                        echo "WARNING: service $svc. did not shutdown on time!"
                        break
                    fi
                done
            else
                # whoops the process id from the pid file isn't there!
                echo "WARNING: process $pid from pid file does not exist"
            fi
        else
            # hmm the /proc directory doesn't exist. do it the old way.
            $KILL $pid
            sleep 5
        fi
    fi
}

stop_service $service

#
# Remove left over processes
#
echo ""
echo "Removing left over $service processes." "["`date`"]"
echo ""

PIDLIST=`$PS | $GREP "$service" | $GREP -v fifo | $GREP -v $GREP | $CUT -d"/" -f1`

for PROCESS in $PIDLIST; do
	#
	$KILL $PROCESS
	#
done
#
# Now hard-kill the processes if they are leftover
#
if [ ! -z "$PIDLIST" -a -d /proc ]; then
	tstart=$SECONDS
	tcount=0
	while [ ! -z "$PIDLIST" -a $(($SECONDS - $tstart)) -lt 20 -a $tcount -lt 200 ]; do
		# give it a chance
		sleep 1

		# check how many processes remain
		newlist=""
		for PROCESS in $PIDLIST; do
			[ -d /proc/$PROCESS ] && newlist="$newlist $PROCESS"
		done
		PIDLIST=$newlist

		# loop
		tcount=$((tcount + 1))
	done

	# if any remain, then hard kill them
	for PROCESS in $PIDLIST; do
		echo "WARNING: doing HARD KILL of $PROCESS"
		$KILL -KILL $PROCESS
	done
fi


#
# Remove left over fifo's
#
echo ""
echo "Removing left over $service fifo's." "["`date`"]"
echo ""
PIDLIST=`$PS | $GREP fifo | $GREP "$prefix" | $CUT -d"/" -f1`
for PROCESS in $PIDLIST; do
	#
	$KILL -9 $PROCESS
	#
done

$RM -f $prefix/*.log

echo "Shutdown completed"
