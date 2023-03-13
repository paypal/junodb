# JunoDB - A secure, consistent and highly available key-value store


## What is JunoDB
JunoDB is PayPal's home-grown Secure, consistent and highly available Key-value store providing low, single digit millisecond, latency at any scale. 

<details>
  <summary>JunoDB high level architecture</summary>
   
<img
  src="./docs/JunoDBHighLevelArch.png"
  style="display: inline-block; margin: 0 auto; max-width: 600px">

</details>

When a client wants to store a (key, value) pair in JunoDB, the proxy maps the (key, value) pair to 3 out of 5 zones in the storage based on a mapping provided by the ETCD. 

JunoDB therefore works using three main components, the ETCD, storage server and proxy. 

</details>
<br>



## Getting Started

### Clone the repository from [github](https://github.com/paypal/junodb)

```bash
git clone https://github.com/paypal/junodb.git
```

### Set BUILDTOP variable

```bash
export BUILDTOP=<path_to_junodb_folder>/junodb
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

```bash
cd $BUILDTOP/docker
./setup.sh
```
### <h3 id="docker_build_junodb">Build JunoDB</h3>
```bash
# Build juno docker images
#junoclusterserv
#junoclustercfg
#junoserv
#junostorageserv
docker login
cd $BUILDTOP/docker
./build.sh 
```

### <h3 id="docker_run_junodb">Run JunoDB</h3>
```bash
# Setup juno network and start juno services
#junoclusterserv
#junoclustercfg
#junostorageserv
#junoserv

# Juno proxy service listens on port 
# :5080 TLS and :8080 TCP
cd $BUILDTOP/docker
./start.sh 

```

### Shutdown JunoDB services
```bash
# This will shutdown juno services
#junoclusterserv
#junoclustercfg
#junostorageserv
#junoserv
cd $BUILDTOP/docker
./shutdown.sh 

```

### Manually Run JunoDB services
```bash
#This can be done instead of ./start.sh to start up the docker services

cd $BUILDTOP/docker/manifest

# To run juno services in --detach mode (recommended)
docker-compose up -d

# Juno proxy service listens on port 
# :5080 TLS and :8080 TCP

#To view the running containes 
docker ps

# To stop juno services
docker-compose down
```
### <h3 id="docker_secrets">Generate Secrets for Dev</h3>

<br>

> **_NOTE:_**  secrets for TLS and Encryption can be generated for dev/testing.
```bash 
cd $BUILDTOP/docker/manifest/config/secrets 
sh gensecrets.sh

## generated secrets
# server.crt/server.pem - certificate/key for juno proxy for TLS 
# ca.crt - CA cert
# keystore.toml - sample keystore file
```
<br>

### <h3 id="docker_validate_junodb">Validate JunoDB</h3>

### Test out the server using junoload command

See instructions [here](./docs/junoload.md) 
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
Install Python 2.7

```bash
#install python
sudo apt-get install python2.7
#set soft link
cd /usr/bin
sudo ln -s python2.7 python2
```


### <h3 id="manual_build_junodb">Build JunoDB</h3>
```bash
cd $BUILDTOP
./binary_build/build.sh
```
<br>


### <h3 id="manual_run_junodb">Run JunoDB</h3>
```bash
cd $BUILDTOP/script
export JUNO_BUILD_DIR=$BUILDTOP/release-binary/code-build
./deploy.sh
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
cd script/test/functest
./configsetup.sh
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

See instructions [here](./docs/junoload.md) 

[def]: #install-dependencies

[![License](http://img.shields.io/:license-Apache%202-blue.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Build](https://github.com/paypal/junoDB/actions/workflows/juno_server_bin_build.yml/badge.svg?branch=main)](https://github.com/paypal/junoDB/actions/workflows/juno_server_bin_build.yml)
[![Docker](https://github.com/paypal/junoDB/actions/workflows/juno_server_docker_build.yml/badge.svg?branch=main)](https://github.com/paypal/junoDB/actions/workflows/juno_server_docker_build.yml)

