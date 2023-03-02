[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

# Run etcd from command line.

<br>

### 1. Copy ~/src/juno/etcdsvr/* to stage. <br>

### 2. Modify etcdsvr.txt as needed. <br>

### 3. Start or restart etcd server on the local host. start.sh will stop current etcd server if any. <br> 

| $ test/start.sh |
| ---------- |

### 4. Shutdown etcd server.  <br>

| $ test/shutdown.sh |
| --------------|

### 5. Features
- Two processes will be started: etcdsvr and etcdsvr_exe.  etcdsvr is the watchdog and etcdsvr_exe is the server. <br>
- Watchdog will restart etcdsvr_exe automatically, if it stops.

