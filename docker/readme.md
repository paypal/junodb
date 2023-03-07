[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

* [Docker](#docker)    
    * [Install Dependencies](#install-dependencies)
    * [Build Juno](#build-juno)
    * [Run Juno](#run-juno)
    * [Generate Secrets for Dev](#generate-secrets-for-dev)

# Docker
> 

<!-- toc -->
## Install Dependencies

[Install Docker Engine v20.10.13+](https://docs.docker.com/engine/install/ubuntu/)
- Docker compose plugin is required on Linux to support docker compose v2.
- In Moby v20.10.13, compose-plugin is an optional Docker CLI plugin for easier installation.

```bash
## Setup docker 

./setup.sh

```
<br>



## Build Juno
### Build docker images for juno
```bash
# Build juno docker images
#junoclusterserv
#junoclustercfg
#junoserv
#junostorageserv

./build.sh 

```
## Run Juno 
### Start juno services
```bash
# Setup juno network and start juno services
#junoclusterserv
#junoclustercfg
#junostorageserv
#junoserv

# Juno proxy service listens on port 
# :5080 TLS and :8080 TCP

./start.sh 

```

### Shutdown juno services
```bash
# This will shutdown juno services
#junoclusterserv
#junoclustercfg
#junostorageserv
#junoserv

./shutdown.sh 

```

### Manually Run juno services
```bash
cd manifest

# To run juno services in --detach mode (recommended)
docker compose up -d

# Juno proxy service listens on port 
# :5080 TLS and :8080 TCP

#To view the running containes 
docker ps

# To stop juno services
docker compose down
```

### Generate Secrets for Dev
<br>

> **_NOTE:_**  secrets for TLS and Encryption can be generated for dev/testing.
```bash 
cd manifest/config/secrets 
sh gensecrets.sh

## generated secrets
# server.crt/server.pem - certificate/key for juno proxy for TLS 
# ca.crt - CA cert
# keystore.toml - sample keystore file
```

