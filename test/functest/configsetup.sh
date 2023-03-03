#!/bin/bash

######################################################################
# To replace config ip with self host ip and generate secrets files ##
######################################################################
export PATH=$PATH:/bin:/sbin
SED='/bin/sed'
stageip=$1

if [ ! $stageip ]; then 
   stageip=`ip -o route get to 8.8.8.8 | sed -n 's/.*src \([0-9.]\+\).*/\1/p'`
fi

$SED "s/STAGEIP/$stageip/g" config.toml_sample > config.toml
cd secrets
./gensecrets.sh
