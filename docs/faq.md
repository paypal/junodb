[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
# Frequently Asked Questions

1. I can't login to docker after ./setup.sh?
* Logout and login to the machine you are on and use ```docker login```. <br>
* The setup script adds the user to the `docker` group. If the user was not already added to the group, they will have to logout and re-login.
* Verify that your username is added to `docker` group
```bash
groups
#or
cat /etc/group | grep docker
```
* If the user is not added to `docker` group, you may add manually. Logout and login after this step.
```bash
sudo usermod -a -G docker $USER
```


<br>

2. Do I need to do the ./setup.sh step if I already have docker installed?
* If you have a docker verison that is greater than  20.10.0, then you do not need to run ./setup.sh

<br>

3. How do I get the proxy ip and port for the junocli and junoload command?
* Find the proxy ip by entering ``` hostname -i``` command on the proxy machine<br>
* For manual build, go to $BUILDTOP/package_config/package/junoserv/config/config.toml, for docker build go to $BUILDTOP/docker/manifest/config/proxy/config.toml <br>
* Find the proxy listener port under ListenerPort (Example: 5080)<br>
* Note: Use the TLS port with SSL enabled when using the -ssl flag

<br>

4. How do I open the proxy monitoring page?<br>
* Go to $BUILDTOP/package_config/package/junoserv/config/config.toml for manual build <br>
* Go to $BUILDTOP/docker/manifest/config/proxy/config.toml for docker build <br>
* Find the proxy monitoring port under parameter HttpMonAddr (Example: 8088)<br>
* Find the proxy ip by entering ``` hostname -i``` command on the proxy machine<br>
* In a web browser, in the URL box, type <proxy_ip>:<proxy_monitoring_port><br>
* This should open the proxy monitoring page<br>

<br>
5. How do I see the logs? <br>

* For manual build, 

```bash 
#etcd logs
cat $BUILDTOP/script/deploy/junoclusterserv/logs/current


#junostorageserv logs
cat $BUILDTOP/script/deploy/junostorageserv/logs/current


#junoserv logs
cat $BUILDTOP/script/deploy/junoserv/logs/current
```

* For docker
```bash
#docker logs <container_name> will print the logs for docker

#etcd logs 
docker logs etcd

#junostorageserv logs 
docker logs storageserv

#junoserv logs 
docker logs proxy
```

<br>
6. Docker containers fail to start when using docker/start.sh due to port conflicts

* This may happen if there are any running apps that use the same ports as forwarded on the machine in  `docker/manifest/docker-compose.yaml`
* Ports 5080,8080,8088 from proxy and 8089 from storageserv are forwarded
* These ports are set as env var under docker/manifest/.env
  - PROXY_TLS_PORT=5080
  - PROXY_TCP_PORT=8080
  - PROXY_ADMIN_PORT=8088
  - SS_ADMIN_PORT=8089
* To resolve this, pass env var when initializing the start script as shown in the example below. Choose non-conflicting ports on your machine.
```bash
PROXY_TLS_PORT=15080 \
PROXY_TCP_PORT=18080 \
PROXY_ADMIN_PORT=18088 \
SS_ADMIN_PORT=18089 \
docker/start.sh
```

<br>
7. Juno services fail to start due to conflicting ports when deployed using script/deploy.sh

* Modify the configured ports in juno config under package-config (as required)
> Etcd (junoclusterserv) \
client-port: 2379 \
peer-port: 2378
```toml
# in package-config/package/junoclusterserv/config/config.toml

# from

[etcdsvr]
# client port
client_port=2379
# peer port
peer_port=2378

# to

[etcdsvr]
# client port
client_port=12379
# peer port
peer_port=12378
```

> Clustercfg (junoclustercfg) \
SSPorts: 25761,26970,26974,26975,26976,26977,26978,26979,26980,26981,26971,26972 \
etcd-endpoint-port: 2379
```toml
# in package-config/package/junoclustercfg/config/config.toml
# Only change the conflicting ports. In the example below, all ports are updated assuming all were already in use

# from
[ClusterInfo]
  SSPorts = [25761,26970,26974,26975,26976,26977,26978,26979,26980,26981,26971,26972]

# to
[ClusterInfo]
  SSPorts = [35761,36970,36974,36975,36976,36977,36978,36979,36980,36981,36971,36972]

# If the client etcd port was modified in package-config/package/junoclusterserv/config/config.toml, then update Etcd Endpoint too
# from
[Etcd]
  Endpoints=["$STAGEIP:2379"]

# to
[Etcd]
  Endpoints=["$STAGEIP:12379"]
```


> Proxy (junoserv): package-config/package/junoserv/config/config.toml \
tls port: 5080 \
tcp port: 8080 \
monitoring port: 8088 \
etcd-endpoint-port: 2379
```toml
# in package-config/package/junoserv/config/config.toml

# from

HttpMonAddr=":8088"
[[Listener]]
 Addr = ":8080"

[[Listener]]
 Addr = ":5080"
 SSLEnabled = true

# to

HttpMonAddr=":18088"
[[Listener]]
 Addr = ":18080"

[[Listener]]
 Addr = ":15080"
 SSLEnabled = true

# If the client etcd port was modified in package-config/package/junoclusterserv/config/config.toml, then update Etcd Endpoint too
# from
[Etcd]
  Endpoints=["$STAGEIP:2379"]

# to
[Etcd]
  Endpoints=["$STAGEIP:12379"]

```

> Storageserv (junostorageserv): package-config/package/junostorageserv/config/config.toml \
monitoring port: 8089 \
etcd-endpoint-port: 2379
```toml
# in package-config/package/junostorageserv/config/config.toml

# from

HttpMonAddr = ":8089"

# to

HttpMonAddr = ":18089"

# If the client etcd port was modified in package-config/package/junoclusterserv/config/config.toml, then update Etcd Endpoint too
# from
[Etcd]
  Endpoints=["$STAGEIP:2379"]

# to
[Etcd]
  Endpoints=["$STAGEIP:12379"]
```


<br>
8. Proxy failed to initialize due to missing tls certs/keys when starting docker containers

* Check that the secrets configured under [Sec] in docker/manifest/config/proxy/config.toml exist at docker/manifest/config/secrets
* A script is availble to generate the secret key/certs for testing. [Please note that these secrets should only be used for testing purpose]
* To generate the secrets, run
```bash
docker/manifest/config/secrets/gensecrets.sh
```