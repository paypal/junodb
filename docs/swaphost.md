[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
# Juno Host Swap Instruction 
Bad Juno nodes may have to be swapped out on a live cluster. The node that needs to be swapped can be running etcd (junoclusterserv), or juno storage or both. Following is a step-by-step guide to perform the swap.

## Swapping a Storage Host
### Step0 (optional)
Pre-insert some data via junocli tool before host swap, after host swap, retrieve data and see if all data are able to be retrieved.  

### Step1
Deploy junostorageserv (and or junoserv) on New_Host1, both services don't need to be start up, just deploy. 
update the junoclustercfg config.toml by changing old box into new box, make a package and deploy it to new box.

```bash
Old Config			New Config
SSHosts=[			SSHosts=[
# Zone 0			# Zone 0
[				[
	"Host1"			        "New_Host1"
],				],
# Zone 1			# Zone 1
[				[
	"Host2"			        "Host2"
],				],
# Zone 2			# Zone 2
[				[
	"Host3"				"Host3"
],
# Zone 3			# Zone 3
[				[
	"Host4"			        "Host4"
],				],
# Zone 4			# Zone 4
[				[
	"Host5"				"Host5"
]				]
]				]
```
Make sure storageserv are up on all the boxes other than the bad box.

### Step2
If to be replaced box is a bad box, this step can be skipped. If to be replaced box is a good box, shutdown 
junostorageserv on to be replaced box, copy rocksdb_junostorageserv from it to new box on the same location. 

### Step3
On the new box (the cluster config contains New_Host1), from junoclustercfg directory, run ./swaphost.sh.  
This step will bump up the junocluster config version in etcd and all the running junoserv and junostorageserv 
hosts will update their cluster map accordingly after script run. 

### Step4
Start up junostorageserv (and or junoserv) on New_Host1. It will fetch the latest junoclustercfg from etcd.

### Step5 (Optional)
Validation - use junocli to retrieve pre-inserted data, all data should be able to retrieve.

### Step6
Once junoserv on New_Host1 works fine, if there is LB in front of junoserv, fix LB to replace Host1 with New_Host1

Deploy the updated junoclustercfg package which contains New_Host1 to all the junoclustercfg boxes. All boxes have
same version of junoclustercfg package after that.

## Swapping host which has etcd server runs on
The etcd cluster has three or five hosts depending on 3 quorum or 5 quorum - Host1^Host2^Host3^Host4^Host5

Identify a new host (New_Host1) for the swap. Make sure etcd servers are up on all hosts except the bad one. 
Host1 is to be swapped with New_Host1

### Step1
Change the etcdsvr.txt under junoclusterserv 
```bash
Old etcdsvr.txt						New etcdsvr.txt
[etcdsvr]						[etcdsvr]
initial_cluster = "Host1^Host2^Host3^Host4^Host5"	initial_cluster = "New_Host1^Host2^Host3^Host4^Host5"
```
Build the junoclusterserv package and deploy to new box (New_Host1)

### Step2
On the old box (Host1), shutdown junoclusterserv by shutdown.sh under junoclusterserv

On the new box(New_Host1), under junoclusterserv, first run join.sh, then run start.sh to have the new box
join the members of quorum 

### Step3
Deploy and start the new junoclusterserv package one by one to all other junoclusterserv boxes

### Step4
Fix LB of etcd to replace old Host1 with New_Host1.
