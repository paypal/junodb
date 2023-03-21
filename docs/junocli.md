[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
# JunoDB Command Line Interface
The JunoDB command line interface is used to query a JunoDB data storage service.  It supports all JunoDB API operations that any JunoDB client library supports.

The command to run junocli is 

```./junocli [-version] [[options] <command> [<args>]] ```


## Getting started with junocli

### Docker Build
junocli command run from within docker client
```bash 
docker exec -it junoclient bash -c '/opt/juno/<junocli command>'
#Sample junocli command and full explanation of junocli given below
```


### Manual Build 
junocli command can be found under $BUILDTOP/release-binary/code-build
```bash 

#Create junocli directory
mkdir junocli_tests

#Copy the junocli script
cp release-binary/code-build/junocli junocli_tests/junocli

#Copy the secrets folder
cp -r script/deploy/junoserv/secrets junocli_tests/

cd junocli_tests

#Write the config file (Sample config file and explanation of config parameters given in the link below)
vi config.toml

<Run the junocli command>
#Sample junocli command and full explanation of junocli given below
```
Sample config file and explanation found [here](config_file_for_junoload_and_junocli.md)


## Sample JunoCLI commands

proxy_ip is the ip of the machine on which the server proxy is running. Can be found using ```hostname -i``` command on the proxy machine.
<br>
proxy_port for manual build can be found under $BUILDTOP/package_config/package/junoserv/config/config.toml under listener ports (SSL Listener port if using SSL)
<br>
proxy_port for docker build can be found under $BUILDTOP/docker/manifest/config/proxy/config.toml under listener ports (SSL Listener port if using SSL)
<br>
<br>

1. CREATE
```bash 
./junocli create -s <proxy_ip>:<proxy_port> -c config.toml -ns test_ns test_key test_value
```

2. GET
```bash 
./junocli get -s <proxy_ip>:<proxy_port> -c config.toml -ns test_ns test_key
```

3. UPDATE
```bash
./junocli update -s <proxy_ip>:<proxy_port> -c config.toml -ns test_ns test_key test_value_updated
#the value and version number will be updated. You can check this by using the GET command again.
```

4. DESTROY
```bash
./junocli destroy -s <proxy_ip>:<proxy_port> -c config.toml -ns test_ns test_key 
```


## JunoDB CLI <command>

  * proxy commands <br>
    * create<br>
      create a record<br>
    * get<br>
      get the value of a given key<br>
    * update<br>
      update a record<br>
    * set<br>
      create or update a record if exists<br>
    * destroy<br>
      destroy a record<br>
    * udfget<br>
      udf get<br>
    * udfset<br>
      udf set<br>
    * populate<br>
      populate a set of records with set commands<br>
  * storage commands<br>
    * pcreate<br>
      PrepareCreate to storage server<br>
    * read<br>
      Read to storage server<br>
    * pupdate<br>
      PrepareUpdate to storage server<br>
    * pset<br>
      PrepareSet to storage server<br>
    * delete<br>
      Delete to storage server<br>
    * pdelete<br>
      PrepareDelete to storage server<br>
    * commit<br>
      Commit the record having been prepared successfully<br>
    * abort<br>
      Abort the record having been prepared successfully<br>
    * repair<br>
      Repair record<br>
    * mdelete<br>
      mark a record as deleted<br>
    * populatess<br>
      populate storage with a set of repair commands<br>
  * others<br>
    * cfggen<br>
      generate default configuration file<br>
    * config<br>
      unify the given toml configuration file(s)<br>
    * inspect<br>
      check juno binary message, ...<br>
    * ridinsp<br>
      check RequestID, ...<br>
    * ssgrp<br>
      print the Shard ID and the SS group of a given key<br>

<br>
For each command, the user can specify arguments. An explanation for these arguments can be found using <br>

```bash
./junocli <command> --help
```


