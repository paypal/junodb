# Running junoload
Junoload is a tool used for benchmarking JunoDB. You can also use it to validate whether the server has been set up correctly. 

```bash
mkdir junoload_tests

#Copy the junoload script
cp release-binary/code-build/junoload junoload_tests/junoload

#Copy the secrets folder
cp -r script/deploy/junoserv/secrets junoload_tests/

#Copy the secrets folder below if using docker build
#cp -r /docker/manifest/config/secrets /junoload_tests/

cd junoload_tests

#Write the config file (Sample config file and explanation of config parameters given below)
vi config.toml

#Run the junoload command (Sample command - explanation of arguments given below)
#<proxy_port> can be found under $BUILDTOP/package_config/package/junoserv/config/config.toml under listener ports (SSL Listener port if using SSL)
#<proxy_ip> is the ip of the machine on which the server proxy is running. Can be found using hostname -i command on the proxy machine. 
.junoload -s <proxy_ip>:<proxy_port> -f 150 -ttl 1800 -t 3600 -ssl -c config.toml
```

## To run junoload from a docker client 
Login to the docker client and run the ./junoload command directly. The proxy ip is aliased as "proxy"
```bash 
docker exec -it junoclient bash -c ‘/opt/juno/junoload -s proxy:5080 -ssl -c config.toml -o 1’
```
<br>


## Sample Config file

You can copy the below sample config file when running junoload and change the AppName if required

```bash
#Name of the application 
AppName ="junos2"

[Sec]
  
  AppName = "junoserv"

  #Path to keystore
  KeyStoreFilePath = "./secrets/keystore.toml"
  
  #Path to server certificate
  CertPemFilePath  = "./secrets/server.crt"
  
  #Path to server pem file
  KeyPemFilePath   = "./secrets/server.pem"
  
  #Path to CA certificate
  CAFilePath = "./secrets/ca.crt"

```

## Explanation of config parameters
The complete list of parameters that can be customized in the config file is shown below with the default values if it is not explicitly set in the user defined config file

```bash
    #Name of the application
    Appname = "junoload"

    #Namespace for the application
    Namespace = "ns"

    #Number of times to retry if connection fails
    RetryCount = 1

    #TTL of entries
    DefaultTimeToLive = 1800

    #Total milliseconds after which connect operation fails
    ConnectTimeout = "100ms"

    #Total milliseconds after which read operation fails
    ReadTimeout = "500ms"

    #Total milliseconds after which write operation fails
    WriteTimeout = "500ms"

    #Total seconds after which request operation fails
    RequestTimeout = "1s"

    #Total milliseconds after which connection recycles
    ConnRecycleTimeout = "9s"

    #Payload length in B
    PayloadLen = 2048

    #TTL of entries 
    TimeToLive = 1800

    #Request pattern in terms of Create, Get, Update, Set, Destroy
    RequestPattern = "C:1,G:1,U:1,S:1,D:1"

    #Monitoring address for proxy monitor
    HttpMonAddr = ""

    #Number of executors to be running in parallel
    NumExecutor = 1

    #Number of requests per second
    NumReqPerSecond = 1000

    #Running time in seconds
    RunningTime = 100

    #Frequency with which to output statistic information in second
    StatOutputRate = 10
    
    [Server]
      #Proxy details
      #Addr = "<proxy_ip>:<proxy_port>
      Addr = "127.0.0.1:8080"
      Network = "" 
      SSLEnabled = false
    
    [Sec]
      #Security details
      AppName = "junoserv"
      CertPem = ""
      KeyPem = ""
      ClientAuth = true
      KeyStoreFilePath = ""
      CertPemFilePath = ""
      KeyPemFilePath = ""
      CAFilePath = ""
    
    
```


## Junoload command arguments
You can also define some of the config parameters using command line arguments

```bash
NAME
  junoload - test driver

SYNOPSIS
  junoload [<args>]

OPTION
  -s, -server string
      (default "127.0.0.1:8080")
      specify proxy address

  -c, -config string
      (default "")
      specify toml configuration file name

  -p, -request-pattern string
      (default "C:1,G:1,U:1,S:1,D:1")
      specify request pattern, a sequence of requests to be
      invoked in format
        <Req>:<num>[{,<Req>:<num>}]
      Supported type of Requests:
        C    CREATE
        G    GET
        S    SET
        U    UPDATE
        D    DESTROY
      
  -var-load, -variable-load
      (default false)
      specify if you wants to vary the payload length, throughput and ttl throught the test

  -ssl
      (default false)
      specify if enabling SSL 

  -n, -num-executor int
      (default 1)
      specify the number of executors to be running in parallel

  -l, -payload-length int
      (default 2048)
      specify payload length

  -f, -num-req-per-second int
      (default 1000)
      specify expected throughput (number of requests per second)

  -t, -running-time int
      (default 100)
      specify drivers running time in second

  -ttl, -record-time-to-live int
      (default 1800)
      specify record TTL in second

  -o, -stat-output-rate int
      (default 10)
      specify how often to output statistic information in second
      for the period of time.

  -mon-addr, -monitoring-address string
      (default "")
      specify the http monitoring address. 
      override HttpMonAddr in config file

  -version
      (default false)
      display version information.

  -dbpath string
      (default "")
      to display rocksdb stats

  -log-level string
      (default "info")
      specify log level
      Options: "verbose" | "info" | "warning" | "error"

  -disableGetTTL
      (default false)
      not use random ttl for get operation



    

EXAMPLE
  run the driver against server listening on 127.0.0.1:8080 with default 
  options
    junoload -s 127.0.0.1:8080

  run the driver with SSL
    junoload -s 127.0.0.1:5080 -ssl

  run the driver with options specified in config.toml
    junoload -c config.toml

```


