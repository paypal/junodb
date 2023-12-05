[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
# Juno Host Expansion Instruction 
Juno is a high scalable and available distributed key value store. In Juno architecture, the whole storage space is partitioned into a fixed number (e.g. 1024) of logical shards, and shards are mapped to storage nodes within the cluster. When a cluster scaling out or scaling in (storage nodes are either added or removed to/from the cluster), some shards must be redistributed to different nodes to reflect new cluster topology. Following are the instuctions on how to expand(or shrink) the juno storageserv nodes. 

## Expand or Shrink Storage Host
### Step0 (pre-requisite)
Deploy juno storageserv (and or junoserv) to all new boxes 

junostorageserv on all the original boxes need to be up

Pre-insert data to original cluster so it can be used for later validation(optional but suggested)

Overall, the expansion contains below steps:
```bash
1. Markdown one zone at a time to stop incoming real time traffic to this zone
2. Run command to update cluster topo to new cluster in etcd
3. Start storageserv on new box for relavant zone. 
4. Run command to start redistribution
   4a. if requests failed to forward after serveral auto retry, run resume command to do redistribution again 
   4b. if after 3a still some requests fail, restart source storageserv so the failed one can start forward again.
5. Run command to commit if previous steps are all successful.  
```

Loop through zone0 to zone5 to finish redistribution for all zones
Retrieve pre-inserted data from storageserv for validation (optional)


### Step1
under junoclustercfg, run 
```bash
 ./clustermgr --config config.toml --cmd zonemarkdown --type set -zone 0 (1,2,3,4)
```
verify markdown works by checking etcd cluster info (zonemarkdown flag added) and junostorageserv state log 
that no new request coming
```bash
--- etcd cluster info shows markdown flag is set ---
run command "export ETCDCTL_API=3; ~/junoclusterserv/etcdctl --endpoints=<ip>:<port> --prefix=true get "" | tail -8 " 
juno.junoserv_numzones
3
juno.junoserv_version
1
juno.junoserv_zonemarkdown	(markdown flag is set for zone 0)
0
juno.root_junoserv
2023-12-05 12:16:17|u20box|/home/deploy/junoclustercfg

--- junostorageserv/state-logs/current shows the number of incoming requests(req) didn't change, i.e. no new traffic coming ---
12-05 12:39:49       id     free     used      req      apt     Read        D        C        A       RR     keys       LN  compSec compCount  pCompKB    stall     pCPU     mCPU     pMem     mMem 
12-05 12:39:49      3-0   453110    42899    34267       98     3303        0    15482        0        0     4338        0        0         0        0        0      0.1      0.2      0.1     15.6 
12-05 12:39:50      3-0   453110    42899    34267       98     3303        0    15482        0        0     4338        0        0         0        0        0      0.1      0.2      0.1     15.6 
12-05 12:39:51      3-0   453110    42899    34267       98     3303        0    15482        0        0     4338        0        0         0        0        0      0.1      0.3      0.1     15.6 
```

### Step2
under junoclustercfg, run 
```bash
  ./clustermgr -new_config config.toml_new -cmd redist -type prepare -zone 0 (1,2,3,4)
```
NOTE: A UI monitoring http link will be generated in redistserv.pid. It can be used for monitoring

### Step3
start junostorageserv on new box for relavant zone -- zone 0(1,2,3,4)

### Step4
under junoclustercfg, run 
```bash
  ./clustermgr -new_config config.toml_new -ratelimit 5000 -cmd redist -type start_tgt --zone 0 (1,2,3,4)
  ./clustermgr -new_config config.toml_new -ratelimit 5000 -cmd redist -type start_src --zone 0 (1,2,3,4)
```
NOTE: the ratelimit needs to be tuned for each different system. Depending on old/new cluster, the rate setting
      will be different. For example,expansion from 5 to 10 boxes or expansion from 5 to 15 boxes, rate will be
      different

#### Step4a (only if requests forward final failed after several auto retry) 
under junoclustercfg, run
```bash
  ./clustermgr --new_config config.toml_new --cmd redist --type resume -ratelimit 5000 -zone 0(1,2,3,4)
```

#### Step4b (only if 4a still doesn't fix the failure)
restart source storageserv and wait for redistribution complete

### Step5
under junoclustercfg, run
```bash
  ./clustermgr -new_config config.toml_new --cmd redist --type commit -zone 0(1,2,3,4)  
```
Loop around zone0 to zone5 to complete all zones' redistribution
 
## Validation (Optional but suggest)

### Steps
run juno client tool to get shard map which contains ss ip:port
```bash
  ./junocli ssgrp -c config.toml_new -hex=false key 
```

run juno client tool to verify if key exists in the expected ss. ip:port is the one get from previoius command
```bash 
  ./junocli read -s ip:port key
```
 
