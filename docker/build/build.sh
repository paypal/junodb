#! /bin/bash
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
 

export CGO_CFLAGS="-I/usr/local/include"
export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lrt -lpthread -ldl"

juno_executables="\
        juno/cmd/proxy \
        juno/cmd/storageserv \
        juno/cmd/clustermgr/clusterctl \
        juno/cmd/dbscanserv \
        juno/cmd/dbscanserv/junoctl \
        juno/cmd/tools/junostats \
        juno/cmd/tools/junocfg \
        juno/cmd/tools/junocli \
        juno/test/drv/junoload \
        juno/test/drv/bulkload \
        juno/cmd/storageserv/storage/db/dbcopy \
        "

export PATH=/usr/local/go/bin:$PATH
#export GOPATH=/juno
export GOROOT=/usr/local/go

cd /juno
build_time=`date '+%Y-%m-%d_%I:%M:%S%p_%Z'`
code_revision=`cat git_revision.txt`
pwd

juno_version_info="-X juno/pkg/version.BuildTime=$build_time -X juno/pkg/version.Revision=$code_revision -X juno/pkg/version.BuildId=$JUNO_BUILD_NUMBER"

env GOBIN=/juno/bin go install $build_tag --ldflags "-linkmode external -extldflags -static $juno_version_info" $juno_executables 2>&1 |tee build.log

