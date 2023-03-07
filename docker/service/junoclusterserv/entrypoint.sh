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
 
#! /bin/bash

: ${PEER_NAMES:=p0}
: ${INITIAL_CLUSTER:=$(hostname -f)}

# generate etcdsvr.txt

echo "etcdsvr.peer_names=$PEER_NAMES
etcdsvr.data_dir=$DATA_DIR
etcdsvr.initial_cluster=$INITIAL_CLUSTER
etcdsvr.client_port=$CLIENT_PORT
etcdsvr.peer_port=$PEER_PORT" > /opt/juno/etcdsvr.txt

exec "$@"

