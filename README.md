# JunoDB - A secure, consistent and highly available key-value store

[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Build](https://github.com/paypal/junoDB/actions/workflows/juno_server_bin_build.yml/badge.svg?branch=main)](https://github.com/paypal/junoDB/actions/workflows/juno_server_bin_build.yml)
[![Docker](https://github.com/paypal/junoDB/actions/workflows/juno_server_docker_build.yml/badge.svg?branch=main)](https://github.com/paypal/junoDB/actions/workflows/juno_server_docker_build.yml)


## What is JunoDB
JunoDB is PayPal's home-grown Secure, consistent and highly available Key-value store providing low, single digit millisecond, latency at any scale. 

<details>
  <summary>JunoDB high level architecture</summary>
   
<img
  src="./JunoDBHighLevelArch.png"
  style="display: inline-block; margin: 0 auto; max-width: 600px">

</details>

When a client wants to store a (key, value) pair in JunoDB, Proxy writes the key in 3 out of 5 shards in the storage server. The shard-map is stored in the etcd.

JunoDB therefore works using three main components, the ETCD, storage server and proxy. 

</details>
<br>



## Getting Started with the JunoDB Server

### Clone the repository from [github](https://github.com/paypal/junodb)

```bash
git clone https://github.com/paypal/junodb.git
```

### Set BUILDTOP variable

```bash
export BUILDTOP=<path_to_junodb_folder>/junodb
cd $BUILDTOP
```

Continue building JunoDB server with 
1. [Docker build](#docker-build) or 
2. [Manual build](#manual-build) 
<br>

## Docker Build

* [Docker](#docker-build)    
    * [Install Dependencies](#docker_install_dependencies)
    * [Build JunoDB](#docker_build_junodb)
    * [Run JunoDB](#docker_run_junodb)
    * [Generate Secrets for Dev](#docker_secrets)
    * [Validate JunoDB](#docker_validate_junodb)


<!-- toc -->

### <h3 id="docker_install_dependencies">Install Dependencies</h3>
[Install Docker Engine version 20.10.0+](https://docs.docker.com/engine/install/ubuntu/)
Check for existing docker version
```bash
docker version
```

Install Docker if not installed or version is older than 20.10.0
```bash
./docker/setup.sh
```

### <h3 id="docker_build_junodb">Build JunoDB</h3>
```bash
# Build junodb docker images
#junoclusterserv
#junoclustercfg
#junoserv
#junostorageserv
docker login
./docker/build.sh 
```

### <h3 id="docker_run_junodb">Run JunoDB</h3>
```bash
# Setup junodb network and start junodb services
#junoclusterserv
#junoclustercfg
#junostorageserv
#junoserv

# JunoDB proxy service listens on port 
# :5080 TLS and :8080 TCP
./docker/start.sh 

```

### Shutdown JunoDB services
```bash
# This will shutdown junodb services
#junoclusterserv
#junoclustercfg
#junostorageserv
#junoserv
./docker/shutdown.sh 

```

### Manually Run JunoDB services
```bash
#This can be done instead of ./start.sh to start up the docker services

cd $BUILDTOP/docker/manifest

# To run junodb services in --detach mode (recommended)
docker-compose up -d

# Juno proxy service listens on port 
# :5080 TLS and :8080 TCP

#To view the running containes 
docker ps

# To stop junodb services
docker-compose down
```
### <h3 id="docker_secrets">Generate Secrets for Dev</h3>

<br>

> **_NOTE:_**  secrets for TLS and Encryption can be generated for dev/testing.
```bash 
sh $BUILDTOP/docker/manifest/config/secrets/gensecrets.sh

## generated secrets
# server.crt/server.pem - certificate/key for junodb proxy for TLS 
# ca.crt - CA cert
# keystore.toml - sample keystore file
```


### <h3 id="docker_validate_junodb">Validate JunoDB</h3>

Login to docker client
```bash 
docker exec -it junoclient bash
```

Check connection with proxy
```bash
nc -vz proxy 5080
```


You can also test the junodb server by running junoload from the docker client. 
See instructions [here](docs/junoload.md) 
<br>


<br>

## Manual Build

The following sections explain the process for manually building the JunoDB server without Docker. These instructions are based on an Ubuntu 20.04.5 system
* [Manual](#manual-build)    
    * [Install Dependencies](#manual_install_dependencies)
    * [Build JunoDB](#manual_build_junodb)
    * [Run JunoDB](#manual_run_junodb)
    * [Validate](#manual_validate_junodb)


### <h3 id="manual_install_dependencies">Install Dependencies</h3>

Install [OpenSSL 1.0.2g+](https://www.openssl.org/source/)
```bash
sudo apt install openssl
```



Install [multilog](https://manpages.ubuntu.com/manpages/bionic/man8/multilog.8.html)

```bash
sudo apt install daemontools
```

Install dependencies for rocksdb

```bash
sudo apt-get install build-essential libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev -y
```

<br>
Install Python 

```bash
#install python
sudo apt-get install python3.8
#set soft link
cd /usr/bin
sudo ln -s python3.8 python
```




### <h3 id="manual_build_junodb">Build JunoDB</h3>
```bash
./binary_build/build.sh
```
<br>


### <h3 id="manual_run_junodb">Run JunoDB</h3>
```bash
export JUNO_BUILD_DIR=$BUILDTOP/release-binary/code-build
./script/deploy.sh
```
<br>


### <h3 id="manual_validate_junodb">Validate JunoDB</h3>
```bash
#Validate if deploy was successful by checking if the proxy (junoserv), storage (junostorageserv), and etcd (junoclusterserv) processes are running
ps -eaf | grep juno
```
<br>


### Run functional tests
```bash
#Assuming user is in $BUILDTOP folder
./script/test/functest/configsetup.sh
cd script/test/functest
$BUILDTOP/release-binary/tool/go/bin/go test -v -config=config.toml
```
<br>


### Run unit tests
```bash
#Assuming user is in $BUILDTOP folder
cd script/test/unittest
$BUILDTOP/release-binary/tool/go/bin/go test -v
```

### Test out the server using junoload command

See instructions [here](docs/junoload.md) 

